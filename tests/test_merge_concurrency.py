"""Two-process concurrency tests for merges, aliases and interleaved ingests.

Like test_concurrency.py these boot TWO real API processes sharing one SQLite
file.  They prove that, across processes:

* a paused merge (impact analysis outside the write lock) does not block
  another process's ingest, including ingest that uses the retiring number;
* concurrent merges / ingests never lose a rewrite, never resurrect a retired
  point and never produce an alias cycle or a duplicated road reference;
* after everything settles, every alias resolves to a live point and every
  persisted geometry matches the persisted point coordinates.
"""
from __future__ import annotations

import os
import threading
import time

import httpx
import pytest

from tests.test_concurrency import two_apis  # reuse the real two-process harness


@pytest.fixture(scope="module")
def apis(tmp_path_factory):
    db = tmp_path_factory.mktemp("merge_shared") / "gis.db"
    with two_apis(db) as bases:
        yield bases


def _seed(apis, prefix, n=8):
    a = apis[0]
    points = [
        {"id": f"{prefix}{i}", "lon": 100.0 + i, "lat": 20.0 + i} for i in range(n)
    ]
    r = httpx.post(a + "/api/v1/batches", json={"points": points}, timeout=20)
    assert r.status_code == 201, r.text


def _merge(base, keep, merged, **params):
    return httpx.post(
        base + "/api/v1/points/merge",
        params=params,
        json={"keep_point_id": keep, "merged_point_id": merged},
        timeout=40,
    )


def _batch(base, body, **params):
    return httpx.post(base + "/api/v1/batches", json=body, params=params, timeout=40)


def _state(base):
    return httpx.get(base + "/api/v1/state", timeout=20).json()


def _assert_invariants(state):
    live = {p["id"] for p in state["points"]}
    canon = {a["alias_id"]: a["canonical_id"] for a in state["aliases"]}
    # 1. alias ids are never also live points (no resurrection)
    assert not (set(canon) & live), "alias id resurrected as a live point"
    # 2. every alias resolves to a live point and never to another alias (no cycle)
    for alias_id, canonical_id in canon.items():
        assert canonical_id in live, f"alias {alias_id} dangles at {canonical_id}"
        assert canonical_id not in canon, f"alias chain/cycle via {canonical_id}"
    # 3. stored road references are canonical, unique, and match the geometry
    coords = {p["id"]: [p["lon"], p["lat"]] for p in state["points"]}
    for road in state["roads"]:
        refs = road["point_ids"]
        assert len(set(refs)) == len(refs), f"{road['id']} has duplicate references"
        assert all(r not in canon for r in refs), f"{road['id']} stores an alias id"
        if road["geometry"] is not None:
            for pid, pos in zip(refs, road["geometry"]["coordinates"]):
                assert pos == coords[pid], f"{road['id']} stale coordinate for {pid}"


def test_paused_merge_does_not_block_alias_ingest_on_other_process(apis):
    _seed(apis, "PA")
    a, b = apis
    # road on B referencing the point A is about to retire
    r = _batch(b, {"roads": [{"id": "MR1", "point_ids": ["PA0", "PA2"]}]})
    assert r.status_code == 201

    result = {}

    def slow_merge():
        result["r"] = _merge(a, "PA0", "PA1", pause_ms=2500)

    t = threading.Thread(target=slow_merge)
    t.start()
    time.sleep(0.2)  # merge is inside its lock-free analysis pause

    # another process keeps ingesting while the merge is paused.  The merge
    # holds its analysis pause for 1.5s with no write lock, so a writer that
    # had to wait on that lock would take >= ~1.5s; allow generous slack for
    # CI/process-startup load while still proving the lock was not held.
    t0 = time.time()
    r = _batch(
        b,
        {"roads": [{"id": "MR2", "point_ids": ["PA3", "PA4"]}]},
    )
    elapsed = time.time() - t0
    assert r.status_code == 201, r.text
    assert elapsed < 1.4, f"merge analysis blocked the other writer ({elapsed:.2f}s)"

    # and an ingest using the retiring number lands while the merge is paused;
    # whichever commits first, the final state must be conflict-free
    r2 = _batch(b, {"roads": [{"id": "MR3", "point_ids": ["PA1", "PA2"]}]})
    t.join(timeout=30)
    assert result["r"].status_code == 200, result["r"].text
    # MR3 is either accepted (PA1 resolved to PA0) or, if it would duplicate,
    # rejected with 409 and zero writes - never half-applied
    assert r2.status_code in (201, 409)
    if r2.status_code == 409:
        assert r2.json()["detail"]["error"] == "duplicate_road_reference"

    state = _state(a)
    _assert_invariants(state)
    assert "PA1" not in {p["id"] for p in state["points"]}
    alias = next(x for x in state["aliases"] if x["alias_id"] == "PA1")
    assert alias["canonical_id"] == "PA0"


def test_concurrent_merges_same_target_never_resurrect_or_cycle(apis):
    _seed(apis, "CM", n=8)
    a, b = apis
    _batch(a, {"roads": [{"id": "CM0", "point_ids": ["CM0", "CM2"]}, {"id": "CM1", "point_ids": ["CM1", "CM3"]}]})

    errors: list[str] = []

    def worker(base, keep, merged):
        try:
            r = _merge(base, keep, merged)
            # 200 = this process won the merge; 409 = the other process already
            # retired one input, so this request replanned into a clean reject
            assert r.status_code in (200, 409), r.text
            if r.status_code == 409:
                assert r.json()["detail"]["error"] in (
                    "alias_id_conflict",
                    "merge_conflict",
                )
        except Exception as exc:  # pragma: no cover - failure path
            errors.append(f"{base} {keep}<-{merged}: {exc}")

    # both processes try to merge CM1 into CM0 at the same time
    threads = [
        threading.Thread(target=worker, args=(a, "CM0", "CM1")),
        threading.Thread(target=worker, args=(b, "CM0", "CM1")),
        # plus competing merges touching neighbouring points
        threading.Thread(target=worker, args=(a, "CM2", "CM3")),
        threading.Thread(target=worker, args=(b, "CM3", "CM2")),
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not errors, errors

    state = _state(a)
    _assert_invariants(state)
    # exactly one of each symmetric pair survives, the other is an alias
    alias_ids = {a2["alias_id"] for a2 in state["aliases"]}
    for x, y in (("CM0", "CM1"), ("CM2", "CM3")):
        live = {p["id"] for p in state["points"]}
        present = [i for i in (x, y) if i in live]
        aliased = [i for i in (x, y) if i in alias_ids]
        assert len(present) == 1 and len(aliased) == 1, (x, y)


def test_merge_interleaved_with_ingests_and_repoints(apis):
    _seed(apis, "IL", n=8)
    a, b = apis
    # IL4 and IL5 never co-occur in a road, so their merge can always commit
    _batch(a, {"roads": [
        {"id": "IR1", "point_ids": ["IL5", "IL6"]},
        {"id": "IR2", "point_ids": ["IL5", "IL7"]},
    ]})

    errors: list[str] = []
    stop = threading.Event()

    def merger():
        # repeatedly merge on A while B mutates roads and points
        for i in range(8):
            try:
                r = _merge(a, "IL4", "IL5")
                if r.status_code == 200:
                    # IL5 is now a permanent alias; using it as a merge input
                    # must reject cleanly rather than resurrect the old point
                    again = _merge(a, "IL6", "IL5")
                    assert again.status_code == 409, again.text
                    assert again.json()["detail"]["error"] == "alias_id_conflict"
                    stop.set()
                    return
                assert r.status_code == 409
            except Exception as exc:  # pragma: no cover
                errors.append(f"merge: {exc}")
                stop.set()
                return
            time.sleep(0.02)
        stop.set()

    def mutator():
        k = 0
        while not stop.is_set():
            try:
                # correct a coordinate, repoint a road, and ingest with the
                # number being retired - all through the OTHER process
                r = _batch(
                    b,
                    {
                        "points": [{"id": "IL6", "lon": 110.0 + (k % 7) / 10.0, "lat": 27.0}],
                        "roads": [
                            {"id": "IR2", "point_ids": ["IL5", "IL7"]},
                            {"id": f"IRX{k % 4}", "point_ids": ["IL7", "IL6"]},
                        ],
                    },
                )
                assert r.status_code in (201, 409), r.text
            except Exception as exc:  # pragma: no cover
                errors.append(f"ingest: {exc}")
                stop.set()
                return
            k += 1
            time.sleep(0.01)

    threads = [threading.Thread(target=merger), threading.Thread(target=mutator)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=60)
    assert not errors, errors

    state = _state(a)
    _assert_invariants(state)
    # the merge landed: IL5 was retired and never came back to life, and its
    # alias points at a surviving canonical point
    live = {p["id"] for p in state["points"]}
    assert "IL5" not in live
    il5 = next(al for al in state["aliases"] if al["alias_id"] == "IL5")
    assert il5["canonical_id"] in live
    # the roads that referenced IL5 were rewritten to the survivor IL4
    ir1 = next(r for r in state["roads"] if r["id"] == "IR1")
    assert ir1["point_ids"][0] == "IL4"


def test_alias_ingest_under_concurrent_merge_resolves_or_cleanly_409s(apis):
    _seed(apis, "AC", n=6)
    a, b = apis
    _batch(a, {"roads": [{"id": "AR1", "point_ids": ["AC0", "AC2"]}]})

    outcomes = []
    lock = threading.Lock()

    def do_merge():
        r = _merge(a, "AC0", "AC1", pause_ms=800)
        with lock:
            outcomes.append(("merge", r.status_code))

    def do_alias_ingest():
        time.sleep(0.15)  # ensure the merge has taken its snapshot and paused
        # road submitted with the retiring number AC1
        r = _batch(b, {"roads": [{"id": "AR2", "point_ids": ["AC1", "AC2"]}]})
        with lock:
            outcomes.append(("ingest", r.status_code, r.text))

    threads = [threading.Thread(target=do_merge), threading.Thread(target=do_alias_ingest)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert ("merge", 200) in outcomes
    ingest = next(o for o in outcomes if o[0] == "ingest")
    # resolved and stored canonically as [AC0, AC2], or rejected 409 with no write
    assert ingest[1] in (201, 409)
    state = _state(a)
    _assert_invariants(state)
    ar2 = next((r for r in state["roads"] if r["id"] == "AR2"), None)
    if ingest[1] == 201:
        assert ar2 is not None and ar2["point_ids"] == ["AC0", "AC2"]
    else:
        assert ar2 is None

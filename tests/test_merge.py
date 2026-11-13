"""Functional tests: point merges, permanent aliases, 409 conflicts, atomicity."""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from app.main import app


@pytest.fixture()
def client(tmp_path, monkeypatch):
    from app import config

    monkeypatch.setattr(config.settings, "db_path", str(tmp_path / "gis.db"))
    with TestClient(app) as c:
        yield c


def batch(client, body, **params):
    return client.post("/api/v1/batches", json=body, params=params)


def merge(client, body, **params):
    return client.post("/api/v1/points/merge", json=body, params=params)


P = {
    "A": (120.1, 30.1),
    "B": (120.2, 30.2),
    "C": (120.3, 30.3),
    "D": (120.4, 30.4),
}


def seed(client, ids=("A", "B", "C", "D"), roads=None):
    batch(
        client,
        {
            "points": [{"id": i, "lon": P[i][0], "lat": P[i][1]} for i in ids],
            "roads": roads or [],
        },
    )


# ---------------------------------------------------------------------------
# Basic merge semantics
# ---------------------------------------------------------------------------


def test_merge_keeps_coordinates_rewrites_refs_and_aliases_the_old_id(client):
    seed(
        client,
        roads=[
            {"id": "R1", "point_ids": ["A", "C"]},
            {"id": "R2", "point_ids": ["B", "C"]},
            {"id": "R3", "point_ids": ["C", "D"]},
        ],
    )
    r = merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["keep_point_id"] == "A"
    assert body["merged_point_id"] == "B"
    assert body["new_alias_ids"] == ["B"]
    assert body["repointed_alias_ids"] == []
    assert sorted(body["affected_road_ids"]) == ["R2"]

    # survivor keeps its original coordinates
    kept = client.get("/api/v1/points/A").json()
    assert [kept["lon"], kept["lat"]] == list(P["A"])
    # merged point is gone
    assert client.get("/api/v1/points/B").status_code == 404

    # references were rewritten to the survivor, geometry rebuilt from A's coords
    r2 = client.get("/api/v1/roads/R2").json()
    assert r2["point_ids"] == ["A", "C"]
    assert r2["geometry"]["coordinates"] == [[120.1, 30.1], [120.3, 30.3]]
    # roads that never referenced B are untouched
    r1 = client.get("/api/v1/roads/R1").json()
    assert r1["point_ids"] == ["A", "C"]
    assert r1["revision"] == 1
    r3 = client.get("/api/v1/roads/R3").json()
    assert r3["revision"] == 1

    # old id is now a permanent alias
    alias = client.get("/api/v1/aliases/B").json()
    assert alias == {"alias_id": "B", "canonical_id": "A", "created_at": alias["created_at"]}
    aliases = client.get("/api/v1/aliases").json()["aliases"]
    assert [a["alias_id"] for a in aliases] == ["B"]


def test_merge_without_referencing_roads_still_aliases_and_deletes(client):
    seed(client)
    r = merge(client, {"keep_point_id": "C", "merged_point_id": "D"})
    assert r.status_code == 200
    assert r.json()["affected_road_ids"] == []
    assert client.get("/api/v1/points/D").status_code == 404
    assert client.get("/api/v1/aliases/D").json()["canonical_id"] == "C"


def test_merge_requires_existing_non_alias_points(client):
    seed(client, ids=("A", "B"))
    r = merge(client, {"keep_point_id": "A", "merged_point_id": "NOPE"})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "merge_conflict"
    assert r.json()["detail"]["point_ids"] == ["NOPE"]

    r = merge(client, {"keep_point_id": "NOPE1", "merged_point_id": "NOPE2"})
    assert r.status_code == 409
    assert sorted(r.json()["detail"]["point_ids"]) == ["NOPE1", "NOPE2"]


def test_merge_rejects_same_point(client):
    seed(client, ids=("A", "B"))
    r = merge(client, {"keep_point_id": "A", "merged_point_id": "A"})
    assert r.status_code == 422


def test_merge_duplicate_reference_is_409_and_writes_nothing(client):
    seed(client, roads=[{"id": "R1", "point_ids": ["A", "B"]}])
    before = client.get("/api/v1/state").json()

    r = merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "duplicate_road_reference"
    assert detail["road_ids"] == ["R1"]
    # whole attempt rejected: no vertex removed, nothing changed
    assert client.get("/api/v1/state").json() == before
    assert client.get("/api/v1/points/B").status_code == 200
    assert client.get("/api/v1/aliases/B").status_code == 404
    assert client.get("/api/v1/roads/R1").json()["point_ids"] == ["A", "B"]


# ---------------------------------------------------------------------------
# Alias resolution during road ingest
# ---------------------------------------------------------------------------


def test_road_ingest_resolves_alias_to_canonical_id(client):
    seed(client, roads=[{"id": "R0", "point_ids": ["C", "D"]}])
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})

    # a new road submitted with the retired number gets resolved on ingest
    r = batch(client, {"roads": [{"id": "R9", "point_ids": ["B", "C"]}]})
    assert r.status_code == 201, r.text
    road = r.json()["roads"][0]
    assert road["point_ids"] == ["A", "C"]
    assert road["geometry"]["coordinates"][0] == list(P["A"])
    # persisted references hold canonical ids, never aliases
    assert client.get("/api/v1/roads/R9").json()["point_ids"] == ["A", "C"]


def test_alias_resolution_duplicate_is_409_with_listing_and_no_auto_remove(client):
    seed(client)
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    before = client.get("/api/v1/state").json()

    # raw ids are distinct, but B resolves to A -> duplicate after resolution
    r = batch(client, {"roads": [{"id": "R1", "point_ids": ["A", "B", "C"]}]})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "duplicate_road_reference"
    assert detail["road_ids"] == ["R1"]
    assert client.get("/api/v1/state").json() == before
    assert client.get("/api/v1/roads/R1").status_code == 404

    # multiple offending roads are all listed
    r = batch(
        client,
        {
            "roads": [
                {"id": "R1", "point_ids": ["A", "B"]},
                {"id": "R2", "point_ids": ["B", "A", "D"]},
            ]
        },
    )
    assert r.status_code == 409
    assert sorted(r.json()["detail"]["road_ids"]) == ["R1", "R2"]


def test_repointing_existing_road_through_alias_resolves(client):
    seed(client, roads=[{"id": "R1", "point_ids": ["C", "D"]}])
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    r = batch(client, {"roads": [{"id": "R1", "point_ids": ["B", "D"]}]})
    assert r.status_code == 201
    assert r.json()["roads"][0]["point_ids"] == ["A", "D"]


# ---------------------------------------------------------------------------
# Alias ids are permanent: 409 on write / delete
# ---------------------------------------------------------------------------


def test_writing_alias_id_is_409(client):
    seed(client)
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    r = batch(client, {"points": [{"id": "B", "lon": 1.0, "lat": 1.0}]})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "alias_id_conflict"
    assert detail["point_ids"] == ["B"]
    # the alias must not have resurrected a point row
    assert client.get("/api/v1/points/B").status_code == 404


def test_deleting_alias_id_is_409(client):
    seed(client)
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    r = batch(client, {"delete_points": ["B"]})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "alias_id_conflict"
    assert client.get("/api/v1/aliases/B").status_code == 200


def test_point_owning_aliases_cannot_be_deleted_directly(client):
    seed(client)
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    before = client.get("/api/v1/state").json()
    r = batch(client, {"delete_points": ["A"]})
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["error"] == "aliased_point_delete"
    assert detail["point_ids"] == ["A"]
    assert client.get("/api/v1/state").json() == before

    # a point without aliases can still be deleted normally
    r = batch(client, {"delete_points": ["D"]})
    assert r.status_code == 201
    assert client.get("/api/v1/points/D").status_code == 404


def test_correcting_survivor_coordinates_is_allowed_and_fans_out(client):
    seed(client, roads=[{"id": "R1", "point_ids": ["A", "C"]}, {"id": "R2", "point_ids": ["B", "C"]}])
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    r = batch(client, {"points": [{"id": "A", "lon": 121.9, "lat": 31.9}]})
    assert r.status_code == 201
    affected = sorted(r.json()["affected_road_ids"])
    assert affected == ["R1", "R2"]
    for road in r.json()["roads"]:
        assert [121.9, 31.9] in road["geometry"]["coordinates"]


# ---------------------------------------------------------------------------
# Chained merges: aliases follow, no cycles
# ---------------------------------------------------------------------------


def test_chained_merge_carries_old_aliases(client):
    seed(client)
    # B -> A (B becomes alias of A)
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    # then A -> C: A is absorbed into C; B (alias of A) must follow to C
    r = merge(client, {"keep_point_id": "C", "merged_point_id": "A"})
    assert r.status_code == 200, r.text
    body = r.json()
    assert body["merged_point_id"] == "A"
    assert sorted(body["new_alias_ids"]) == ["A", "B"]
    assert body["repointed_alias_ids"] == ["B"]

    state = client.get("/api/v1/state").json()
    live = {p["id"] for p in state["points"]}
    assert live == {"C", "D"}
    canon = {a["alias_id"]: a["canonical_id"] for a in state["aliases"]}
    assert canon == {"A": "C", "B": "C"}
    # every alias resolves to a live point -> no alias cycle
    assert all(c in live for c in canon.values())

    # a third chain hop D absorbs C: A and B follow again
    r = merge(client, {"keep_point_id": "D", "merged_point_id": "C"})
    assert r.status_code == 200
    canon = {a["alias_id"]: a["canonical_id"] for a in client.get("/api/v1/aliases").json()["aliases"]}
    assert canon == {"A": "D", "B": "D", "C": "D"}
    assert client.get("/api/v1/points/D").status_code == 200


def test_chained_merge_rewrites_roads_and_resolves_all_old_numbers(client):
    seed(
        client,
        roads=[
            {"id": "R1", "point_ids": ["A", "C"]},
            {"id": "R2", "point_ids": ["B", "C"]},
        ],
    )
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    # R2 now [A, C]; B is an alias of A
    merge(client, {"keep_point_id": "D", "merged_point_id": "A"})
    # every old number A and B must resolve to D after the chained merge
    r1 = client.get("/api/v1/roads/R1").json()
    r2 = client.get("/api/v1/roads/R2").json()
    assert r1["point_ids"] == ["D", "C"]
    assert r2["point_ids"] == ["D", "C"]

    # ingest using either retired number resolves to D
    r = batch(client, {"roads": [{"id": "R9", "point_ids": ["B", "A"]}]})
    assert r.status_code == 409  # both resolve to D -> duplicate
    r = batch(client, {"roads": [{"id": "R9", "point_ids": ["B", "C"]}]})
    assert r.status_code == 201
    assert r.json()["roads"][0]["point_ids"] == ["D", "C"]


# ---------------------------------------------------------------------------
# State snapshot, atomicity, restart
# ---------------------------------------------------------------------------


def test_merge_rejects_alias_as_input(client):
    seed(client)
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    r = merge(client, {"keep_point_id": "C", "merged_point_id": "B"})
    assert r.status_code == 409
    assert r.json()["detail"]["error"] == "alias_id_conflict"
    assert r.json()["detail"]["point_ids"] == ["B"]
    r = merge(client, {"keep_point_id": "B", "merged_point_id": "C"})
    assert r.status_code == 409
    assert r.json()["detail"]["point_ids"] == ["B"]


def test_merge_rewrites_dangling_alias_reference(client):
    # a road can persist a dangling reference to an id that is later merged;
    # when its canonical point is itself merged, the dangling alias follows too
    seed(client, ids=("A", "C", "D"))
    batch(client, {"roads": [{"id": "R1", "point_ids": ["B", "C"]}]})  # B missing
    assert client.get("/api/v1/roads/R1").json()["status"] == "incomplete"
    # supply B, then merge B -> A (R1 becomes [A, C])
    batch(client, {"points": [{"id": "B", "lon": P["B"][0], "lat": P["B"][1]}]})
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    assert client.get("/api/v1/roads/R1").json()["point_ids"] == ["A", "C"]
    # now delete A's road-independent row? no - merge A -> D instead
    r = merge(client, {"keep_point_id": "D", "merged_point_id": "A"})
    assert r.status_code == 200
    assert client.get("/api/v1/roads/R1").json()["point_ids"] == ["D", "C"]


def test_state_includes_aliases_in_one_snapshot(client):
    seed(client, roads=[{"id": "R1", "point_ids": ["A", "C"]}])
    merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    state = client.get("/api/v1/state").json()
    assert {a["alias_id"] for a in state["aliases"]} == {"B"}
    assert state["aliases"][0]["canonical_id"] == "A"
    point_ids = {p["id"] for p in state["points"]}
    assert "B" not in point_ids and "A" in point_ids


def test_failed_merge_rolls_back_everything(client):
    seed(client, roads=[{"id": "R1", "point_ids": ["A", "C"]}, {"id": "R2", "point_ids": ["B", "D"]}])
    before = client.get("/api/v1/state").json()
    r = merge(
        client,
        {"keep_point_id": "A", "merged_point_id": "B"},
        fail=True,
    )
    assert r.status_code == 500
    after = client.get("/api/v1/state").json()
    assert after == before
    assert client.get("/api/v1/points/B").status_code == 200
    assert client.get("/api/v1/aliases/B").status_code == 404
    assert client.get("/api/v1/roads/R2").json()["point_ids"] == ["B", "D"]


def test_merge_restart_preserves_aliases_and_rewritten_roads(tmp_path, monkeypatch):
    from app import config

    db = str(tmp_path / "gis.db")
    monkeypatch.setattr(config.settings, "db_path", db)
    with TestClient(app) as c1:
        seed(c1, roads=[{"id": "R1", "point_ids": ["A", "C"]}, {"id": "R2", "point_ids": ["B", "C"]}])
        merge(c1, {"keep_point_id": "A", "merged_point_id": "B"})
        expected = c1.get("/api/v1/state").json()
    with TestClient(app) as c2:
        got = c2.get("/api/v1/state").json()
        assert got == expected
        # alias still resolves after restart, including for new ingest
        r = batch(c2, {"roads": [{"id": "R9", "point_ids": ["B", "A"]}]})
        assert r.status_code == 409  # both resolve to A
        r = batch(c2, {"roads": [{"id": "R9", "point_ids": ["B", "D"]}]})
        assert r.json()["roads"][0]["point_ids"] == ["A", "D"]


def test_merge_bumps_generation_once_and_road_revisions(client):
    seed(client, roads=[{"id": "R1", "point_ids": ["A", "C"]}, {"id": "R2", "point_ids": ["B", "C"]}])
    gen_before = client.get("/api/v1/state").json()["generation"]
    r = merge(client, {"keep_point_id": "A", "merged_point_id": "B"})
    assert r.status_code == 200
    gen_after = client.get("/api/v1/state").json()["generation"]
    assert gen_after == gen_before + 1
    assert client.get("/api/v1/roads/R2").json()["revision"] == 2
    assert client.get("/api/v1/roads/R1").json()["revision"] == 1


def test_merge_paused_does_not_block_other_writer(client, monkeypatch):
    """Impact analysis outside the write lock: a paused merge holds no lock."""
    import threading
    import time

    seed(
        client,
        roads=[{"id": "R1", "point_ids": ["A", "C"]}, {"id": "R2", "point_ids": ["B", "D"]}],
    )
    result = {}

    def slow_merge():
        result["r"] = merge(
            client,
            {"keep_point_id": "A", "merged_point_id": "B"},
            pause_ms=1500,
        )

    t = threading.Thread(target=slow_merge)
    t.start()
    time.sleep(0.2)
    t0 = time.time()
    r = batch(client, {"points": [{"id": "Z", "lon": 1.0, "lat": 1.0}]})
    elapsed = time.time() - t0
    assert r.status_code == 201
    assert elapsed < 1.0, f"merge analysis held the write lock ({elapsed:.2f}s)"
    t.join(timeout=10)
    assert result["r"].status_code == 200

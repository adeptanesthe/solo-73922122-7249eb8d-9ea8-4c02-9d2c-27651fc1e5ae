"""Scenario sample used by the docker compose demo.

Runs entirely against the public HTTP API of both processes, so it doubles as
a black-box acceptance check of the delivered environment:

1. shared points + roads are ingested through API A;
2. a referenced point goes missing -> no partial line, missing ids reported;
3. the point is re-supplied -> the line restores automatically;
4. a correction of one point fans out to every road referencing it;
5. a deliberately paused batch on A (assembly outside any write lock) runs
   while B keeps ingesting and repointing roads;
6. a failed batch rolls back; the final state is then re-read from B, and
   snapshots are validated against the persisted values;
7. a point merge retires one number into a survivor (kept coordinates, road
   references rewritten, permanent alias), with the 409 duplicate-reference,
   alias write/delete and alias-owner-delete rules demonstrated;
8. a chained merge carries old aliases along (no alias cycle), retired
   numbers resolve on later road ingest, and a paused merge races a concurrent
   old-number ingest on the other process without resurrecting the old point.

Run locally::

    python -m examples.run_demo --base-a http://localhost:8001 \\
                                --base-b http://localhost:8002
"""
from __future__ import annotations

import argparse
import json
import sys
import threading
import time

import httpx

POINTS = {
    "PT-001": (116.397428, 39.909230),
    "PT-002": (116.407428, 39.913000),
    "PT-003": (116.417428, 39.918000),
    "PT-004": (116.427428, 39.921000),
    "PT-005": (116.437428, 39.926000),
    "PT-006": (116.447428, 39.930000),
}


def pretty(title: str, payload) -> None:
    print(f"\n=== {title} " + "=" * max(0, 66 - len(title)))
    print(json.dumps(payload, ensure_ascii=False, indent=2))


class Api:
    def __init__(self, base: str) -> None:
        self.base = base.rstrip("/")
        self.http = httpx.Client(base_url=self.base, timeout=60.0)

    def batch(self, body: dict, **params):
        r = self.http.post("/api/v1/batches", json=body, params=params)
        r.raise_for_status()
        return r.json()

    def batch_raw(self, body: dict, **params):
        return self.http.post("/api/v1/batches", json=body, params=params)

    def merge(self, body: dict, **params):
        return self.http.post("/api/v1/points/merge", json=body, params=params)

    def aliases(self) -> dict:
        r = self.http.get("/api/v1/aliases")
        r.raise_for_status()
        return r.json()

    def alias(self, alias_id: str):
        return self.http.get(f"/api/v1/aliases/{alias_id}")

    def road(self, road_id: str) -> dict:
        r = self.http.get(f"/api/v1/roads/{road_id}")
        r.raise_for_status()
        return r.json()

    def state(self) -> dict:
        r = self.http.get("/api/v1/state")
        r.raise_for_status()
        return r.json()


def _run(args) -> None:
    a = Api(args.base_a)
    b = Api(args.base_b)
    print(f"API A = {a.base}\nAPI B = {b.base}  (both share /data/gis.db)")

    # -- 1 --------------------------------------------------------------
    points = [{"id": pid, "lon": lon, "lat": lat} for pid, (lon, lat) in POINTS.items()]
    roads = [
        {"id": "RD-101", "point_ids": ["PT-001", "PT-002", "PT-003"]},
        {"id": "RD-102", "point_ids": ["PT-003", "PT-004", "PT-005"]},
        {"id": "RD-103", "point_ids": ["PT-005", "PT-006"]},
    ]
    batch = a.batch({"points": points, "roads": roads})
    pretty("1. 批量入库（API A）: 测点 + 路段，线形已持久化", batch)

    # -- 2 --------------------------------------------------------------
    missing = a.batch({"delete_points": ["PT-004"]})
    pretty("2. 删除被引用测点 PT-004：不输出残线，列明缺失 ID", missing)
    assert missing["roads"][0]["geometry"] is None
    assert missing["roads"][0]["missing_point_ids"] == ["PT-004"]

    # -- 3 --------------------------------------------------------------
    restored = a.batch({"points": [{"id": "PT-004", "lon": POINTS["PT-004"][0], "lat": POINTS["PT-004"][1]}]})
    pretty("3. 补点后自动恢复线形（无需额外调用）", restored)
    assert restored["roads"][0]["status"] == "ok"

    # -- 4 --------------------------------------------------------------
    corrected = b.batch(
        {"points": [{"id": "PT-003", "lon": 116.417800, "lat": 39.919200}]}
    )
    pretty("4. 更正 PT-003（改由 API B 提交）：两条引用它的路段同时生效", corrected)
    assert sorted(corrected["affected_road_ids"]) == ["RD-101", "RD-102"]

    # -- 5 --------------------------------------------------------------
    # Paused batch on A: it reports affected roads based on the snapshot it
    # plans from, and commits only after B has already written.  Its commit
    # re-validates the snapshot, so it either waits or recomputes - but it can
    # never overwrite B's newer coordinates with its old ones.
    print("\n=== 5. 可控暂停下并发改线 " + "=" * 44)
    results: dict[str, object] = {}

    def paused_batch():
        t0 = time.time()
        results["a"] = a.batch(
            {
                "points": [{"id": "PT-006", "lon": 116.447900, "lat": 39.930400}],
                "roads": [{"id": "RD-104", "point_ids": ["PT-006", "PT-001"]}],
            },
            pause_ms=2000,
        )
        results["a_elapsed"] = round(time.time() - t0, 2)

    t = threading.Thread(target=paused_batch)
    t.start()
    time.sleep(0.3)  # the paused batch is now in its 2s assembly pause

    t0 = time.time()
    b_result = b.batch(
        {
            "points": [{"id": "PT-006", "lon": 116.447950, "lat": 39.930900}],
            "roads": [{"id": "RD-105", "point_ids": ["PT-006", "PT-002"]}],
        }
    )
    b_elapsed = round(time.time() - t0, 2)
    t.join()
    pretty(
        f"B 在 A 暂停期间完成入库，用时 {b_elapsed}s（未等待 A 的写锁）",
        b_result,
    )
    pretty(f"A 的暂停批次完成后返回，总用时 {results['a_elapsed']}s", results["a"])

    final = b.state()
    pretty("并发结束后（API B 读取）: 两条引用 PT-006 的路段都采用最后一次提交的值", {
        "PT-006": next(p for p in final["points"] if p["id"] == "PT-006"),
        "RD-104": b.road("RD-104"),
        "RD-105": b.road("RD-105"),
    })

    # -- 6 --------------------------------------------------------------
    before = b.state()
    try:
        a.batch(
            {
                "points": [
                    {"id": "PT-001", "lon": 1.0, "lat": 2.0},
                    {"id": "PT-999", "lon": 1.0, "lat": 3.0},
                ],
                "roads": [{"id": "RD-101", "point_ids": ["PT-999", "PT-002"]}],
            },
            fail=True,
        )
        raise SystemExit("failure injection did not fail")
    except httpx.HTTPStatusError as exc:
        pretty("6. 批内失败（写后注入异常）：整体回滚", exc.response.json())

    after = b.state()
    assert after == before, "failed batch leaked changes"
    pretty("回滚后状态与失败前完全一致", {"generation": after["generation"]})

    # -- 7: point merge -------------------------------------------------
    # Fresh survey-team numbers that describe the same physical spot as an
    # existing point.  First lay down roads that reference both numbers.
    print("\n=== 7. 测点归并：保留点沿用原坐标，旧编号永久成为别名 " + "=" * 14)
    merge_points = [
        {"id": "PT-101", "lon": 116.501, "lat": 39.951},
        {"id": "PT-102", "lon": 116.502, "lat": 39.952},
        {"id": "PT-103", "lon": 116.503, "lat": 39.953},
        {"id": "PT-104", "lon": 116.504, "lat": 39.954},
        {"id": "PT-105", "lon": 116.505, "lat": 39.955},
    ]
    a.batch({"points": merge_points})
    a.batch(
        {
            "roads": [
                # merge pair PT-102/PT-103 and second-hop pair PT-103/PT-105
                # never co-occur; RD-200 holds PT-101+PT-102 to show the 409
                {"id": "RD-200", "point_ids": ["PT-101", "PT-102"]},
                {"id": "RD-201", "point_ids": ["PT-102", "PT-104"]},
                {"id": "RD-202", "point_ids": ["PT-103", "PT-104"]},
            ]
        }
    )

    # 7a. a merge that would duplicate a reference is rejected 409, nothing changes
    dup = a.merge({"keep_point_id": "PT-101", "merged_point_id": "PT-102"})
    assert dup.status_code == 409
    pretty("7a. 归并将导致 RD-200 重复引用 -> 409，整次不改、不自动删顶点", dup.json())
    assert dup.json()["detail"]["road_ids"] == ["RD-200"]

    # 7b. a clean merge: PT-102 is absorbed into PT-103 (they never co-occur)
    merged = a.merge({"keep_point_id": "PT-103", "merged_point_id": "PT-102"})
    merged.raise_for_status()
    pretty("7b. 归并 PT-102 -> PT-103：引用替换、测点删除、旧编号成为别名", merged.json())
    body = merged.json()
    assert body["new_alias_ids"] == ["PT-102"]
    assert sorted(body["affected_road_ids"]) == ["RD-200", "RD-201"]
    assert a.road("RD-200")["point_ids"] == ["PT-101", "PT-103"]
    assert a.road("RD-201")["point_ids"] == ["PT-103", "PT-104"]
    assert a.http.get("/api/v1/points/PT-102").status_code == 404

    # 7c. alias lookup and alias resolution on later road ingest
    pretty("7c. 查别名 /api/v1/aliases/PT-102", a.alias("PT-102").json())
    assert a.alias("PT-102").json()["canonical_id"] == "PT-103"
    resolved = a.batch({"roads": [{"id": "RD-203", "point_ids": ["PT-102", "PT-101"]}]})
    pretty("用旧编号 PT-102 入库路段：别名解析为 PT-103 后持久化", resolved["roads"][0])
    assert resolved["roads"][0]["point_ids"] == ["PT-103", "PT-101"]

    # 7d. retired ids can neither be written nor deleted; alias owners can't
    #     be deleted directly
    write_alias = a.batch_raw({"points": [{"id": "PT-102", "lon": 1.0, "lat": 1.0}]})
    assert write_alias.status_code == 409
    pretty("7d. 写入别名编号 -> 409（旧点不会复活）", write_alias.json()["detail"])
    del_owner = a.batch_raw({"delete_points": ["PT-103"]})
    assert del_owner.status_code == 409
    pretty("直接删除仍拥有别名的保留点 PT-103 -> 409", del_owner.json()["detail"])

    # -- 8: chained merge and concurrent old-number ingest -------------
    print("\n=== 8. 链式归并（旧别名跟随）与并发旧编号入库 " + "=" * 22)
    # second hop into PT-105, which never shares a road with PT-103; the alias
    # PT-102 owned by PT-103 must follow to PT-105
    chained = a.merge({"keep_point_id": "PT-105", "merged_point_id": "PT-103"})
    chained.raise_for_status()
    pretty("8a. 再归并 PT-103 -> PT-105：PT-102 旧别名跟随，无别名环", chained.json())
    canon = {x["alias_id"]: x["canonical_id"] for x in a.aliases()["aliases"]}
    assert canon["PT-102"] == "PT-105" and canon["PT-103"] == "PT-105"
    # every previously retired number still resolves through the alias table
    again = a.batch_raw({"roads": [{"id": "RD-204", "point_ids": ["PT-102", "PT-101"]}]})
    pretty("8b. 用两代以前的旧编号 PT-102 入库 -> 解析到 PT-105", again.json())
    assert again.status_code == 201
    assert again.json()["roads"][0]["point_ids"] == ["PT-105", "PT-101"]

    # 8c. a paused merge on A while B ingests a road using the retiring number
    print("\n=== 8c. 可控暂停下并发旧编号入库（归并分析不持写锁） " + "=" * 16)
    a.batch(
        {
            "points": [
                {"id": "PT-201", "lon": 116.601, "lat": 40.001},
                {"id": "PT-202", "lon": 116.602, "lat": 40.002},
                {"id": "PT-203", "lon": 116.603, "lat": 40.003},
            ]
        }
    )
    a.batch({"roads": [{"id": "RD-301", "point_ids": ["PT-201", "PT-203"]}]})
    race: dict[str, object] = {}

    def paused_merge():
        t0 = time.time()
        race["merge"] = a.merge(
            {"keep_point_id": "PT-201", "merged_point_id": "PT-202"},
            pause_ms=2000,
        )
        race["merge_elapsed"] = round(time.time() - t0, 2)

    t = threading.Thread(target=paused_merge)
    t.start()
    time.sleep(0.3)  # A is inside its lock-free 2s analysis pause

    t0 = time.time()
    # B keeps writing; this road uses the very number A is about to retire
    r_ingest = b.batch_raw(
        {"roads": [{"id": "RD-302", "point_ids": ["PT-202", "PT-203"]}]}
    )
    ingest_elapsed = round(time.time() - t0, 2)
    t.join()
    assert race["merge"].status_code == 200, race["merge"].text
    pretty(
        f"B 在 A 归并暂停期间用旧编号入库，用时 {ingest_elapsed}s（未等写锁）",
        {"http_status": r_ingest.status_code, "body": r_ingest.json()},
    )
    pretty(f"A 的暂停归并完成，总用时 {race['merge_elapsed']}s", race["merge"].json())

    final = b.state()
    live = {p["id"] for p in final["points"]}
    final_aliases = {x["alias_id"]: x["canonical_id"] for x in final["aliases"]}
    assert "PT-202" not in live and final_aliases["PT-202"] == "PT-201"
    if r_ingest.status_code == 201:
        # committed first: stored canonically, then the merge rewrote it
        assert b.road("RD-302")["point_ids"] == ["PT-201", "PT-203"]
    else:
        # the merge committed first and resolving PT-202 would duplicate a
        # reference somewhere: clean 409, no half-written road
        assert r_ingest.status_code == 409
        assert r_ingest.json()["detail"]["error"] == "duplicate_road_reference"
    pretty(
        "并发结束后：旧点未复活、无别名环、引用全部为现存编号",
        {
            "PT-202_resolves_to": final_aliases.get("PT-202"),
            "alias_total": len(final_aliases),
            "RD-302": (
                b.road("RD-302")
                if r_ingest.status_code == 201
                else "409 rejected, nothing written"
            ),
        },
    )

    # consistency check on a single freshly read snapshot (steps 7-8 happened
    # after the `after` snapshot taken back in step 6)
    final_state = b.state()
    coords = {p["id"]: [p["lon"], p["lat"]] for p in final_state["points"]}
    live_ids = set(coords)
    final_aliases = {
        x["alias_id"]: x["canonical_id"] for x in final_state["aliases"]
    }
    for road in final_state["roads"]:
        # references are always canonical, unique live point ids
        assert len(set(road["point_ids"])) == len(road["point_ids"])
        assert all(pid not in final_aliases for pid in road["point_ids"])
        if road["geometry"] is None:
            continue
        for pid, pos in zip(road["point_ids"], road["geometry"]["coordinates"]):
            assert pos == coords[pid], f"{road['id']} geometry does not match point {pid}"
    # every merge-created alias resolves to a live point and never points at
    # another alias (the early batch-demo steps legitimately delete points
    # that are not part of any merge, so the invariant is scoped to the
    # PT-1xx/PT-2xx aliases created in steps 7-8)
    merge_aliases = {
        alias_id: canonical_id
        for alias_id, canonical_id in final_aliases.items()
        if alias_id.startswith(("PT-1", "PT-2"))
    }
    for alias_id, canonical_id in merge_aliases.items():
        assert alias_id not in live_ids, f"alias {alias_id} was resurrected"
        assert canonical_id in live_ids, f"alias {alias_id} dangles"
        assert canonical_id not in final_aliases, f"alias cycle at {alias_id}"

    pretty(
        "最终一致性校验通过：坐标=同快照测点原值；别名均指向现存点、无环、未复活",
        {
            "generation": final_state["generation"],
            "points": len(final_state["points"]),
            "aliases": len(final_state["aliases"]),
            "roads": {
                r["id"]: ("ok" if r["status"] == "ok" else f"缺 {r['missing_point_ids']}")
                for r in final_state["roads"]
            },
        },
    )
    print("\n演示完成。重启 api-a / api-b 后上述数据仍在（SQLite WAL 持久化）。")


def main(argv=None) -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--base-a", default="http://localhost:8001")
    ap.add_argument("--base-b", default="http://localhost:8002")
    _run(ap.parse_args(argv))
    return 0


if __name__ == "__main__":
    sys.exit(main())

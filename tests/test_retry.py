"""Deterministic test of the optimistic commit guard.

Simulates "another API process committed while this batch was assembling" by
bumping the store generation from a second connection between the snapshot and
the commit.  The stale plan must be discarded, not written.
"""
from __future__ import annotations

import sqlite3

import pytest

from app import config, store
from app.db import init_db
from app.schemas import BatchIn


@pytest.fixture()
def db(tmp_path, monkeypatch):
    path = str(tmp_path / "gis.db")
    monkeypatch.setattr(config.settings, "db_path", path)
    init_db()
    return path


def _batch():
    return BatchIn.model_validate(
        {
            "points": [
                {"id": "A", "lon": 1.0, "lat": 1.0},
                {"id": "B", "lon": 2.0, "lat": 2.0},
            ],
            "roads": [{"id": "R", "point_ids": ["A", "B"]}],
        }
    )


def test_stale_plan_is_recomputed_not_written(db, monkeypatch):
    # seed a road that references A (which will move concurrently)
    store.execute_batch(_batch())

    real_build = store._build_plan
    state = {"calls": 0}

    def build_with_concurrent_write(batch, snap):
        state["calls"] += 1
        if state["calls"] == 1:
            # another process commits A = (9,9) while we are still planning
            conn = sqlite3.connect(db, isolation_level=None)
            conn.execute("BEGIN IMMEDIATE")
            conn.execute("UPDATE points SET lon = 9, lat = 9 WHERE id = 'A'")
            conn.execute("UPDATE store_meta SET generation = generation + 1")
            conn.execute("COMMIT")
            conn.close()
        return real_build(batch, snap)

    monkeypatch.setattr(store, "_build_plan", build_with_concurrent_write)

    result, attempts = store.execute_batch(
        BatchIn.model_validate({"roads": [{"id": "R", "point_ids": ["A", "B"]}]})
    )
    assert attempts >= 2, "stale plan was not retried"
    # the committed geometry must use the OTHER process's coordinate, not the
    # coordinate this batch saw in its first snapshot
    assert result.roads[0].geometry["coordinates"][0] == [9.0, 9.0]
    assert result.roads[0].point_ids == ["A", "B"]


def test_failed_commit_does_not_bump_generation(db):
    conn = sqlite3.connect(db, isolation_level=None)
    before = conn.execute("SELECT generation FROM store_meta").fetchone()[0]
    conn.close()

    with pytest.raises(store.DemoFailure):
        store.execute_batch(
            BatchIn.model_validate({"points": [{"id": "C", "lon": 3, "lat": 3}]}),
            inject_failure=True,
        )

    conn = sqlite3.connect(db, isolation_level=None)
    after = conn.execute("SELECT generation FROM store_meta").fetchone()[0]
    has_c = conn.execute("SELECT count(*) FROM points WHERE id='C'").fetchone()[0]
    conn.close()
    assert after == before
    assert has_c == 0


def test_merge_replans_when_another_process_commits(db, monkeypatch):
    """A merge whose snapshot goes stale must recompute from the new state.

    While the merge is planning, another connection rewrites a road that
    references the point being absorbed.  The stale merge plan must be
    discarded and rebuilt so the concurrent road change is neither lost nor
    left referencing a deleted point.
    """
    from app.schemas import MergeIn

    store.execute_batch(
        BatchIn.model_validate(
            {
                "points": [
                    {"id": "A", "lon": 1.0, "lat": 1.0},
                    {"id": "B", "lon": 2.0, "lat": 2.0},
                    {"id": "C", "lon": 3.0, "lat": 3.0},
                ],
                "roads": [{"id": "R", "point_ids": ["B", "C"]}],
            }
        )
    )

    real_snapshot = store._take_merge_snapshot
    state = {"calls": 0}

    def snapshot_with_concurrent_commit(conn, merge):
        snap = real_snapshot(conn, merge)
        state["calls"] += 1
        if state["calls"] == 1:
            # another process commits while we are planning the merge
            other = sqlite3.connect(db, isolation_level=None)
            other.execute("BEGIN IMMEDIATE")
            # repoint the road away from B, then bump generation
            other.execute("DELETE FROM road_points WHERE road_id = 'R'")
            other.execute(
                "INSERT INTO road_points(road_id, seq, point_id) VALUES (?, ?, ?), (?, ?, ?)",
                ("R", 0, "A", "R", 1, "C"),
            )
            other.execute(
                "UPDATE roads SET geometry = ?, revision = revision + 1, updated_at = 't' "
                "WHERE id = 'R'",
                ('{"type":"LineString","coordinates":[[1,1],[3,3]]}',),
            )
            other.execute("UPDATE store_meta SET generation = generation + 1")
            other.execute("COMMIT")
            other.close()
        return snap

    monkeypatch.setattr(store, "_take_merge_snapshot", snapshot_with_concurrent_commit)

    result, attempts = store.execute_merge(
        MergeIn.model_validate({"keep_point_id": "A", "merged_point_id": "B"})
    )
    assert attempts >= 2, "stale merge plan was not retried"
    # after replanning the road no longer references B, so the merge finds no
    # affected road; B is still retired as an alias of A and nothing dangles
    assert result.affected_road_ids == []
    refs = [
        row[0]
        for row in sqlite3.connect(db).execute(
            "SELECT point_id FROM road_points WHERE road_id = 'R' ORDER BY seq"
        )
    ]
    assert refs == ["A", "C"]
    alias = sqlite3.connect(db).execute(
        "SELECT canonical_id FROM point_aliases WHERE alias_id = 'B'"
    ).fetchone()
    assert alias is not None and alias[0] == "A"
    assert sqlite3.connect(db).execute(
        "SELECT count(*) FROM points WHERE id = 'B'"
    ).fetchone()[0] == 0


def test_failed_merge_does_not_bump_generation_or_create_alias(db):
    from app.schemas import MergeIn

    store.execute_batch(
        BatchIn.model_validate(
            {
                "points": [
                    {"id": "A", "lon": 1.0, "lat": 1.0},
                    {"id": "B", "lon": 2.0, "lat": 2.0},
                    {"id": "C", "lon": 3.0, "lat": 3.0},
                ],
                "roads": [{"id": "R", "point_ids": ["B", "C"]}],
            }
        )
    )
    conn = sqlite3.connect(db, isolation_level=None)
    before = conn.execute("SELECT generation FROM store_meta").fetchone()[0]
    conn.close()

    with pytest.raises(store.DemoFailure):
        store.execute_merge(
            MergeIn.model_validate({"keep_point_id": "A", "merged_point_id": "B"}),
            inject_failure=True,
        )

    conn = sqlite3.connect(db, isolation_level=None)
    after = conn.execute("SELECT generation FROM store_meta").fetchone()[0]
    alias_count = conn.execute("SELECT count(*) FROM point_aliases").fetchone()[0]
    b_count = conn.execute("SELECT count(*) FROM points WHERE id='B'").fetchone()[0]
    conn.close()
    assert after == before
    assert alias_count == 0
    assert b_count == 1

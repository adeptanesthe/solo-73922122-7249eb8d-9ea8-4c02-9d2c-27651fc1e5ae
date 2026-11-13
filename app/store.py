"""Read/write orchestration for points, roads and point merges.

Concurrency model
-----------------
A mutating request (batch ingest or point merge) is *planned* from a WAL read
snapshot and the (potentially slow) assembly runs with **no database lock at
all**.  The plan is then committed in one short ``BEGIN IMMEDIATE``
transaction that first re-checks the store generation.  If any other writer
committed in the meantime the plan is discarded, nothing is written, and the
request is recomputed from a fresh snapshot.

Because ``BEGIN IMMEDIATE`` excludes every other writer until COMMIT, whoever
commits second is guaranteed to have observed the first writer's rows.  So a
road can never be written back from stale coordinates, a road created
concurrently can never be missed, and a merge can never resurrect a point that
another merge just retired: every plan is rebuilt from the newest committed
state before it is allowed to write.

Merge / alias rules
-------------------
* A merge takes two **existing, non-alias** point ids.  The survivor keeps its
  own coordinates; the absorbed point's road references are rewritten to the
  survivor, the absorbed row is deleted and its id becomes a *permanent*
  alias.  Aliases already owned by the absorbed point are re-pointed at the
  survivor in the same transaction, so chained merges never create a cycle
  and every alias always resolves to a live point.
* Batch ingestion resolves aliases before planning.  Writing or deleting an
  alias id, deleting a point that still owns aliases, or producing a
  duplicated road reference after resolution are all rejected with **409 and
  zero writes** - vertices are never auto-removed.
"""
from __future__ import annotations

import json
import sqlite3
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Iterable, Mapping, Sequence

from .config import settings
from .db import read_tx, write_tx
from .geometry import assemble as _assemble
from .schemas import BatchIn, BatchResult, MergeIn, MergeResult, PointIn, RoadOut

CHUNK = 400


class RetryPlanned(Exception):
    """Snapshot generation changed between planning and committing."""


class ConcurrentConflict(Exception):
    """Gave up after too many optimistic retries."""


class DemoFailure(Exception):
    """Injected failure used to demonstrate transactional rollback."""


class ConflictError(Exception):
    """A 409 business conflict.  Nothing was or will be written."""

    def __init__(self, error: str, message: str, point_ids=None, road_ids=None):
        super().__init__(message)
        self.error = error
        self.message = message
        self.point_ids = sorted(set(point_ids or []))
        self.road_ids = sorted(set(road_ids or []))

    def detail(self) -> dict:
        return {
            "error": self.error,
            "message": self.message,
            "point_ids": self.point_ids,
            "road_ids": self.road_ids,
        }


def _chunks(ids: Iterable[str], size: int = CHUNK) -> Iterable[Sequence[str]]:
    buf: list[str] = []
    for i in ids:
        buf.append(i)
        if len(buf) >= size:
            yield buf
            buf = []
    if buf:
        yield buf


def _now() -> str:
    return (
        datetime.now(timezone.utc)
        .isoformat(timespec="milliseconds")
        .replace("+00:00", "Z")
    )


# ---------------------------------------------------------------------------
# Alias helpers
# ---------------------------------------------------------------------------


def load_aliases(conn) -> dict[str, str]:
    """Return the full ``alias_id -> canonical_id`` mapping."""
    return {
        row["alias_id"]: row["canonical_id"]
        for row in conn.execute("SELECT alias_id, canonical_id FROM point_aliases")
    }


def resolve_id(pid: str, aliases: Mapping[str, str] | None = None) -> str:
    """Resolve one id through the alias map.

    Alias rows always point straight at a live point (never at another alias),
    so a single lookup is the full resolution; chains cannot exist.
    """
    if aliases is None:
        return pid
    return aliases.get(pid, pid)


# ---------------------------------------------------------------------------
# Snapshot
# ---------------------------------------------------------------------------


@dataclass
class Snapshot:
    generation: int
    old_points: dict[str, tuple[float, float]]
    old_refs: dict[str, list[str]]
    roads: dict[str, dict]
    coords: dict[str, tuple[float, float]]
    aliases: dict[str, str]
    alias_owners: dict[str, list[str]]


@dataclass
class PlannedRoad:
    road_id: str
    point_ids: list[str]
    geometry: dict | None
    missing: list[str]
    existed: bool
    changed: bool


@dataclass
class Plan:
    generation: int
    point_writes: list[tuple[str, float, float]]
    point_deletes: list[str]
    road_deletes: list[str]
    roads: list[PlannedRoad] = field(default_factory=list)


def _take_snapshot(conn, batch: BatchIn) -> Snapshot:
    generation = int(
        conn.execute("SELECT generation FROM store_meta WHERE id = 1").fetchone()[0]
    )
    aliases = load_aliases(conn)
    alias_owners: dict[str, list[str]] = {}
    for alias_id, canonical_id in aliases.items():
        alias_owners.setdefault(canonical_id, []).append(alias_id)

    touched_raw = {p.id for p in batch.points} | set(batch.delete_points)
    touched = {resolve_id(pid, aliases) for pid in touched_raw}
    candidates = {r.id for r in batch.roads} | set(batch.delete_roads)

    old_points: dict[str, tuple[float, float]] = {}
    if touched:
        for chunk in _chunks(sorted(touched)):
            marks = ",".join("?" * len(chunk))
            for row in conn.execute(
                f"SELECT id, lon, lat FROM points WHERE id IN ({marks})", chunk
            ):
                old_points[row["id"]] = (row["lon"], row["lat"])

    if touched:
        for chunk in _chunks(sorted(touched)):
            marks = ",".join("?" * len(chunk))
            for row in conn.execute(
                f"SELECT DISTINCT road_id FROM road_points WHERE point_id IN ({marks})",
                chunk,
            ):
                candidates.add(row["road_id"])

    # Persisted references only ever hold canonical ids.
    old_refs: dict[str, list[str]] = {rid: [] for rid in candidates}
    for chunk in _chunks(sorted(candidates)):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
            f"SELECT road_id, point_id FROM road_points WHERE road_id IN ({marks}) "
            "ORDER BY road_id, seq",
            chunk,
        ):
            old_refs[row["road_id"]].append(row["point_id"])

    roads: dict[str, dict] = {}
    for chunk in _chunks(sorted(candidates)):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
            f"SELECT id, geometry, missing_point_ids, revision, updated_at FROM roads "
            f"WHERE id IN ({marks})",
            chunk,
        ):
            roads[row["id"]] = {
                "geometry": json.loads(row["geometry"]) if row["geometry"] else None,
                "missing": json.loads(row["missing_point_ids"]),
            }

    # Coordinates for every point an affected road may reference.  This maps
    # directly onto the post-batch point state.
    needed = set(touched)
    for refs in old_refs.values():
        needed.update(refs)
    for road in batch.roads:
        needed.update(resolve_id(pid, aliases) for pid in road.point_ids)
    coords: dict[str, tuple[float, float]] = {}
    for chunk in _chunks(sorted(needed)):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
            f"SELECT id, lon, lat FROM points WHERE id IN ({marks})", chunk
        ):
            coords[row["id"]] = (row["lon"], row["lat"])

    return Snapshot(
        generation=generation,
        old_points=old_points,
        old_refs=old_refs,
        roads=roads,
        coords=coords,
        aliases=aliases,
        alias_owners=alias_owners,
    )


def _raise_batch_conflicts(batch: BatchIn, snap: Snapshot) -> None:
    """Validate every 409 rule against the planned snapshot.

    Runs on every planning attempt, so the checks always reflect the newest
    committed state; the request still writes nothing until the generation
    guard inside the write transaction accepts this very plan.
    """
    aliases = snap.aliases
    alias_point_ids: list[str] = []
    blocked_deletes: list[str] = []

    for p in batch.points:
        if p.id in aliases:
            alias_point_ids.append(p.id)
    for pid in batch.delete_points:
        if pid in aliases:
            alias_point_ids.append(pid)
        # a point that still owns permanent aliases must not be deleted
        # directly - merging it is the only allowed path
        elif pid in snap.alias_owners:
            blocked_deletes.append(pid)

    dup_roads: list[str] = []
    for road in batch.roads:
        resolved = [resolve_id(pid, aliases) for pid in road.point_ids]
        if len(set(resolved)) != len(resolved):
            dup_roads.append(road.id)

    if alias_point_ids:
        raise ConflictError(
            "alias_id_conflict",
            "retired alias ids can neither be written nor deleted; resolve them "
            "to their canonical point first",
            point_ids=alias_point_ids,
        )
    if blocked_deletes:
        raise ConflictError(
            "aliased_point_delete",
            "a point that owns permanent aliases cannot be deleted directly; "
            "merge it into another point instead",
            point_ids=blocked_deletes,
        )
    if dup_roads:
        raise ConflictError(
            "duplicate_road_reference",
            "resolving aliases would make a road reference the same point more "
            "than once; no vertex is removed automatically",
            road_ids=dup_roads,
        )


def _build_plan(batch: BatchIn, snap: Snapshot) -> Plan:
    _raise_batch_conflicts(batch, snap)
    aliases = snap.aliases

    points = dict(snap.coords)
    for p in batch.points:
        points[p.id] = (p.lon, p.lat)  # non-alias ids guaranteed by the check
    for pid in batch.delete_points:
        points.pop(pid, None)

    refs: dict[str, list[str]] = {rid: list(v) for rid, v in snap.old_refs.items()}
    for road in batch.roads:
        refs[road.id] = [resolve_id(pid, aliases) for pid in road.point_ids]
    for rid in batch.delete_roads:
        refs.pop(rid, None)

    point_writes: list[tuple[str, float, float]] = []
    for p in batch.points:
        if snap.old_points.get(p.id) != (p.lon, p.lat):
            point_writes.append((p.id, p.lon, p.lat))
    point_deletes = [pid for pid in batch.delete_points if pid in snap.old_points]
    road_deletes = [rid for rid in batch.delete_roads if rid in snap.roads]

    planned: list[PlannedRoad] = []
    for rid in sorted(refs):
        road_refs = refs[rid]
        geometry, missing = _assemble(road_refs, points)
        meta = snap.roads.get(rid)
        existed = meta is not None
        if not existed:
            changed = True
        else:
            changed = (
                road_refs != snap.old_refs.get(rid, [])
                or geometry != meta["geometry"]
                or missing != meta["missing"]
            )
        planned.append(
            PlannedRoad(
                road_id=rid,
                point_ids=road_refs,
                geometry=geometry,
                missing=missing,
                existed=existed,
                changed=changed,
            )
        )

    return Plan(
        generation=snap.generation,
        point_writes=point_writes,
        point_deletes=point_deletes,
        road_deletes=road_deletes,
        roads=planned,
    )


# ---------------------------------------------------------------------------
# Point merge
# ---------------------------------------------------------------------------


@dataclass
class MergeSnapshot:
    generation: int
    keep_id: str
    merged_id: str
    keep_coord: tuple[float, float]
    aliases: dict[str, str]
    # road_id -> (current refs, planned refs, geometry-or-None, missing)
    affected: dict[str, tuple[list[str], list[str], dict | None, list[str]]]
    repointed_aliases: list[str]


def _take_merge_snapshot(conn, merge: MergeIn) -> MergeSnapshot:
    generation = int(
        conn.execute("SELECT generation FROM store_meta WHERE id = 1").fetchone()[0]
    )
    aliases = load_aliases(conn)
    keep_id, merged_id = merge.keep_point_id, merge.merged_point_id

    keep_row = conn.execute(
        "SELECT id, lon, lat FROM points WHERE id = ?", (keep_id,)
    ).fetchone()
    merged_row = conn.execute(
        "SELECT id, lon, lat FROM points WHERE id = ?", (merged_id,)
    ).fetchone()
    # alias inputs are a distinct, permanent error class and take precedence
    # over "point does not exist" (an alias row never has a points row)
    alias_inputs = [pid for pid in (keep_id, merged_id) if pid in aliases]
    if alias_inputs:
        raise ConflictError(
            "alias_id_conflict",
            "merge inputs must be live points, not retired alias ids",
            point_ids=alias_inputs,
        )
    missing = [
        pid
        for pid, row in ((keep_id, keep_row), (merged_id, merged_row))
        if row is None
    ]
    if missing:
        raise ConflictError(
            "merge_conflict",
            "both merge inputs must be existing non-alias point ids",
            point_ids=missing,
        )

    # every road referencing the absorbed point - or one of its aliases - has
    # to be rewritten.  Persisted references normally hold canonical ids, but
    # a dangling reference created while the point was missing may name an
    # alias number; resolving all of them keeps the invariant "stored
    # references are always canonical" intact across chained merges.
    absorbed_ids = sorted(
        {merged_id}
        | {alias_id for alias_id, canonical in aliases.items() if canonical == merged_id}
    )
    affected_ids: list[str] = []
    for chunk in _chunks(absorbed_ids):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
            f"SELECT DISTINCT road_id FROM road_points WHERE point_id IN ({marks}) "
            "ORDER BY road_id",
            chunk,
        ):
            if row["road_id"] not in affected_ids:
                affected_ids.append(row["road_id"])

    refs_by_road: dict[str, list[str]] = {rid: [] for rid in affected_ids}
    if affected_ids:
        for chunk in _chunks(affected_ids):
            marks = ",".join("?" * len(chunk))
            for row in conn.execute(
                f"SELECT road_id, point_id FROM road_points WHERE road_id IN ({marks}) "
                "ORDER BY road_id, seq",
                chunk,
            ):
                refs_by_road[row["road_id"]].append(row["point_id"])

    # coordinates of every referenced point, for in-transaction assembly
    needed = {keep_id, merged_id}
    for refs in refs_by_road.values():
        needed.update(refs)
    coords: dict[str, tuple[float, float]] = {}
    for chunk in _chunks(sorted(needed)):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
            f"SELECT id, lon, lat FROM points WHERE id IN ({marks})", chunk
        ):
            coords[row["id"]] = (row["lon"], row["lat"])
    # post-merge state: the merged row is gone, every reference now resolves
    # to the survivor, whose coordinates are unchanged.
    coords.pop(merged_id, None)
    coords[keep_id] = (keep_row["lon"], keep_row["lat"])

    affected: dict[str, tuple[list[str], list[str], dict | None, list[str]]] = {}
    dup_roads: list[str] = []
    absorbed = set(absorbed_ids)
    for rid in affected_ids:
        current = refs_by_road[rid]
        planned = [keep_id if pid in absorbed else pid for pid in current]
        if len(set(planned)) != len(planned):
            dup_roads.append(rid)
            continue
        geometry, missing_ids = _assemble(planned, coords)
        affected[rid] = (current, planned, geometry, missing_ids)
    if dup_roads:
        raise ConflictError(
            "duplicate_road_reference",
            "the merge would make these roads reference the surviving point "
            "more than once; the merge is rejected and no vertex is removed",
            road_ids=dup_roads,
        )

    repointed_aliases = sorted(
        alias_id for alias_id, canonical in aliases.items() if canonical == merged_id
    )
    return MergeSnapshot(
        generation=generation,
        keep_id=keep_id,
        merged_id=merged_id,
        keep_coord=(keep_row["lon"], keep_row["lat"]),
        aliases=aliases,
        affected=affected,
        repointed_aliases=repointed_aliases,
    )


def _apply_merge(conn, snap: MergeSnapshot, stamp: str, inject_failure: bool) -> list[RoadOut]:
    keep_id, merged_id = snap.keep_id, snap.merged_id

    # Re-validate against the rows this transaction actually locked.  The
    # generation guard already proves nothing changed since planning; these
    # checks are the in-transaction invariant that makes merges crash-safe.
    live = {
        r["id"]
        for r in conn.execute("SELECT id FROM points WHERE id IN (?, ?)", (keep_id, merged_id))
    }
    gone = [pid for pid in (keep_id, merged_id) if pid not in live]
    if gone:
        # another merge retired one of the inputs after our snapshot: replan
        raise RetryPlanned()
    alias_now = load_aliases(conn)
    if keep_id in alias_now or merged_id in alias_now:
        raise RetryPlanned()

    # 1. rewrite every road reference merged_id -> keep_id, then recompute and
    #    persist the affected geometry in the same transaction
    out: list[RoadOut] = []
    for rid in sorted(snap.affected):
        current, planned, geometry, missing = snap.affected[rid]
        try:
            write_refs(conn, rid, current, planned)
        except sqlite3.IntegrityError as exc:
            # UNIQUE(road_id, point_id): duplicated reference at the SQL level
            raise ConflictError(
                "duplicate_road_reference",
                "the merge would duplicate a road reference; rejected",
                road_ids=[rid],
            ) from exc
        geom_text = json.dumps(geometry, separators=(",", ":")) if geometry else None
        missing_text = json.dumps(missing, separators=(",", ":"))
        revision = int(
            conn.execute("SELECT revision FROM roads WHERE id = ?", (rid,)).fetchone()[0]
        ) + 1
        conn.execute(
            "UPDATE roads SET geometry = ?, missing_point_ids = ?, revision = ?, "
            "updated_at = ? WHERE id = ?",
            (geom_text, missing_text, revision, stamp, rid),
        )
        out.append(
            RoadOut(
                id=rid,
                point_ids=planned,
                status="ok" if geometry else "incomplete",
                geometry=geometry,
                missing_point_ids=missing,
                revision=revision,
                updated_at=stamp,
            )
        )

    # 2. aliases already owned by the absorbed point follow the merge (still
    #    pointing at a live point, never at an alias -> no cycle possible)
    for alias_id in snap.repointed_aliases:
        conn.execute(
            "UPDATE point_aliases SET canonical_id = ? WHERE alias_id = ?",
            (keep_id, alias_id),
        )

    # 3. the absorbed number is retired permanently and its row removed.
    #    No road_points row can still name it (step 1 rewrote them all), so
    #    deleting it can never leave a dangling reference behind.
    conn.execute(
        "INSERT INTO point_aliases(alias_id, canonical_id, created_at) VALUES (?, ?, ?)",
        (merged_id, keep_id, stamp),
    )
    conn.execute("DELETE FROM points WHERE id = ?", (merged_id,))

    if inject_failure:
        # Demo hook after every write, immediately before COMMIT.
        raise DemoFailure("injected failure before commit; merge rolled back")

    conn.execute("UPDATE store_meta SET generation = generation + 1 WHERE id = 1")
    return out


def execute_merge(
    merge: MergeIn, *, pause_ms: int = 0, inject_failure: bool = False
) -> tuple[MergeResult, int]:
    """Plan, verify and commit a point merge.  Returns (result, attempts)."""
    max_attempts = max(1, settings.max_commit_attempts)
    for attempt in range(1, max_attempts + 1):
        with read_tx() as conn:
            snap = _take_merge_snapshot(conn, merge)

        if attempt == 1 and pause_ms > 0:
            # impact analysis / assembly happens outside any write lock
            time.sleep(pause_ms / 1000.0)

        stamp = _now()
        try:
            with write_tx() as conn:
                current = int(
                    conn.execute(
                        "SELECT generation FROM store_meta WHERE id = 1"
                    ).fetchone()[0]
                )
                if current != snap.generation:
                    raise RetryPlanned()
                roads_out = _apply_merge(conn, snap, stamp, inject_failure)
        except RetryPlanned:
            time.sleep(min(0.02 * attempt, 0.2))
            continue

        all_aliases = sorted(set(snap.repointed_aliases) | {merge.merged_point_id})
        result = MergeResult(
            keep_point_id=merge.keep_point_id,
            merged_point_id=merge.merged_point_id,
            new_alias_ids=all_aliases,
            repointed_alias_ids=snap.repointed_aliases,
            affected_road_ids=sorted(snap.affected),
            roads=sorted(roads_out, key=lambda r: r.id),
            commit_attempts=attempt,
        )
        return result, attempt
    raise ConcurrentConflict(
        f"could not commit merge after {max_attempts} attempts due to concurrent writes"
    )


# ---------------------------------------------------------------------------
# Commit (batch)
# ---------------------------------------------------------------------------


def write_refs(conn, road_id: str, current: list[str], wanted: list[str]) -> None:
    """Replace a road's reference list only where it actually differs."""
    if current == wanted:
        return
    conn.execute("DELETE FROM road_points WHERE road_id = ?", (road_id,))
    rows = [(road_id, seq, pid) for seq, pid in enumerate(wanted)]
    conn.executemany(
        "INSERT INTO road_points(road_id, seq, point_id) VALUES (?, ?, ?)", rows
    )


def _apply(
    conn, plan: Plan, stamp: str, inject_failure: bool
) -> tuple[list[RoadOut], int]:
    written = [r for r in plan.roads if r.changed]

    for pid, lon, lat in plan.point_writes:
        conn.execute(
            "INSERT INTO points(id, lon, lat) VALUES (?, ?, ?) "
            "ON CONFLICT(id) DO UPDATE SET lon = excluded.lon, lat = excluded.lat",
            (pid, lon, lat),
        )
    if plan.point_deletes:
        conn.executemany(
            "DELETE FROM points WHERE id = ?", [(pid,) for pid in plan.point_deletes]
        )
    if plan.road_deletes:
        # road_points rows go with the road via ON DELETE CASCADE
        conn.executemany(
            "DELETE FROM roads WHERE id = ?", [(rid,) for rid in plan.road_deletes]
        )

    new_roads = 0
    out: list[RoadOut] = []
    for road in written:
        geom_text = (
            json.dumps(road.geometry, separators=(",", ":")) if road.geometry else None
        )
        missing_text = json.dumps(road.missing, separators=(",", ":"))
        if road.existed:
            revision = int(
                conn.execute(
                    "SELECT revision FROM roads WHERE id = ?", (road.road_id,)
                ).fetchone()[0]
            ) + 1
            conn.execute(
                "UPDATE roads SET geometry = ?, missing_point_ids = ?, revision = ?, "
                "updated_at = ? WHERE id = ?",
                (geom_text, missing_text, revision, stamp, road.road_id),
            )
            stored_refs = [
                r["point_id"]
                for r in conn.execute(
                    "SELECT point_id FROM road_points WHERE road_id = ? ORDER BY seq",
                    (road.road_id,),
                )
            ]
            write_refs(conn, road.road_id, stored_refs, road.point_ids)
        else:
            new_roads += 1
            revision = 1
            conn.execute(
                "INSERT INTO roads(id, geometry, missing_point_ids, revision, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (road.road_id, geom_text, missing_text, revision, stamp),
            )
            write_refs(conn, road.road_id, [], road.point_ids)
        out.append(
            RoadOut(
                id=road.road_id,
                point_ids=road.point_ids,
                status="ok" if road.geometry else "incomplete",
                geometry=road.geometry,
                missing_point_ids=road.missing,
                revision=revision,
                updated_at=stamp,
            )
        )

    if inject_failure:
        # Issued after every write, immediately before COMMIT: proves the
        # whole batch (points, references and geometry) is rolled back.
        raise DemoFailure("injected failure before commit; transaction rolled back")

    if written or plan.point_writes or plan.point_deletes or plan.road_deletes:
        # Bumping the generation invalidates any concurrent plan that was
        # computed from the pre-commit state, forcing it to recompute instead
        # of writing stale geometry back.
        conn.execute("UPDATE store_meta SET generation = generation + 1 WHERE id = 1")
    return out, new_roads


def execute_batch(
    batch: BatchIn, *, pause_ms: int = 0, inject_failure: bool = False
) -> tuple[BatchResult, int]:
    """Plan, verify and commit.  Returns (result, commit_attempts)."""
    max_attempts = max(1, settings.max_commit_attempts)
    for attempt in range(1, max_attempts + 1):
        with read_tx() as conn:
            snap = _take_snapshot(conn, batch)

        if attempt == 1 and pause_ms > 0:
            # Demo hook: the pause happens after the snapshot is released and
            # before any write lock is taken.  Other batches commit freely.
            time.sleep(pause_ms / 1000.0)

        # ConflictError propagates: it was evaluated against this snapshot and
        # is only surfaced when the generation guard accepts this very plan.
        plan = _build_plan(batch, snap)
        stamp = _now()
        try:
            with write_tx() as conn:
                current = int(
                    conn.execute(
                        "SELECT generation FROM store_meta WHERE id = 1"
                    ).fetchone()[0]
                )
                if current != plan.generation:
                    raise RetryPlanned()
                roads_out, new_roads = _apply(conn, plan, stamp, inject_failure)
        except RetryPlanned:
            # small jittered backoff before replanning from a fresh snapshot
            time.sleep(min(0.02 * attempt, 0.2))
            continue

        result = BatchResult(
            points_upserted=len(plan.point_writes),
            roads_upserted=new_roads,
            roads_updated=len([r for r in plan.roads if r.changed and r.existed]),
            points_deleted=len(plan.point_deletes),
            roads_deleted=len(plan.road_deletes),
            affected_road_ids=sorted(r.road_id for r in plan.roads if r.changed),
            roads=sorted(roads_out, key=lambda r: r.id),
            commit_attempts=attempt,
        )
        return result, attempt
    raise ConcurrentConflict(
        f"could not commit after {max_attempts} attempts due to concurrent writes"
    )


# ---------------------------------------------------------------------------
# Reads - persisted rows only, never assembled on the fly
# ---------------------------------------------------------------------------


def get_generation(conn) -> int:
    return int(conn.execute("SELECT generation FROM store_meta WHERE id = 1").fetchone()[0])


def list_aliases(conn) -> list:
    from .schemas import AliasOut

    return [
        AliasOut(
            alias_id=row["alias_id"],
            canonical_id=row["canonical_id"],
            created_at=row["created_at"],
        )
        for row in conn.execute(
            "SELECT alias_id, canonical_id, created_at FROM point_aliases "
            "ORDER BY alias_id"
        )
    ]


def get_alias(conn, alias_id: str):
    from .schemas import AliasOut

    row = conn.execute(
        "SELECT alias_id, canonical_id, created_at FROM point_aliases WHERE alias_id = ?",
        (alias_id,),
    ).fetchone()
    if row is None:
        return None
    return AliasOut(
        alias_id=row["alias_id"],
        canonical_id=row["canonical_id"],
        created_at=row["created_at"],
    )


def list_points(conn) -> list[PointIn]:
    return [
        PointIn(id=r["id"], lon=r["lon"], lat=r["lat"])
        for r in conn.execute("SELECT id, lon, lat FROM points ORDER BY id")
    ]


def get_point(conn, point_id: str) -> PointIn | None:
    row = conn.execute(
        "SELECT id, lon, lat FROM points WHERE id = ?", (point_id,)
    ).fetchone()
    return None if row is None else PointIn(id=row["id"], lon=row["lon"], lat=row["lat"])


def list_roads(conn, road_id: str | None = None) -> list[RoadOut]:
    if road_id is None:
        rows = conn.execute(
            "SELECT id, geometry, missing_point_ids, revision, updated_at FROM roads "
            "ORDER BY id"
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT id, geometry, missing_point_ids, revision, updated_at FROM roads "
            "WHERE id = ?",
            (road_id,),
        ).fetchall()
    if not rows:
        return []
    refs_by_road: dict[str, list[str]] = {}
    for chunk in _chunks([r["id"] for r in rows]):
        marks = ",".join("?" * len(chunk))
        for row in conn.execute(
            f"SELECT road_id, point_id FROM road_points WHERE road_id IN ({marks}) "
            "ORDER BY road_id, seq",
            chunk,
        ):
            refs_by_road.setdefault(row["road_id"], []).append(row["point_id"])
    out: list[RoadOut] = []
    for row in rows:
        geometry = json.loads(row["geometry"]) if row["geometry"] else None
        out.append(
            RoadOut(
                id=row["id"],
                point_ids=refs_by_road.get(row["id"], []),
                status="ok" if geometry else "incomplete",
                geometry=geometry,
                missing_point_ids=json.loads(row["missing_point_ids"]),
                revision=row["revision"],
                updated_at=row["updated_at"],
            )
        )
    return out

"""FastAPI application: batch ingest, point merges and persisted queries."""
from __future__ import annotations

import sqlite3
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Query

from . import store
from .config import settings
from .db import init_db, read_tx
from .schemas import (
    AliasOut,
    AliasesResponse,
    BatchIn,
    BatchResult,
    ConflictDetail,
    HealthResponse,
    MergeIn,
    MergeResult,
    PointIn,
    PointsResponse,
    RoadOut,
    RoadsResponse,
    StateResponse,
)

DESCRIPTION = """
测绘队共享测点 -> 路段线形加工服务，支持**测点归并与永久别名**。

* `POST /api/v1/batches` 一次事务内完成测点/路段的**批量新增、更正、删除**；
  路段入库时先解析别名（旧编号自动替换为保留点编号）。
* `POST /api/v1/points/merge` 归并两个现存非别名测点：保留点沿用原坐标，
  被归并点的全部路段引用被替换、测点删除，旧编号永久成为别名；旧别名跟随，
  不形成别名环。
* 写入/删除别名测点、直接删除仍拥有别名的测点、归并或别名解析后路段出现
  重复引用，一律 **409 且整次不改**（不自动删顶点），响应列出冲突对象。
* 线形在提交时按路段引用的测点顺序组装并**持久保存**为 GeoJSON
  `LineString`；查询接口只读已持久化的表，不现场组线。
* `GET /api/v1/state` 在**一个读快照**里返回同一已提交状态的测点、别名与
  已持久化线形。

组线与归并影响面分析都发生在事务之外；提交只是很短的
`BEGIN IMMEDIATE`。两个 API 进程共用同一个 SQLite 文件（WAL 模式），
写者互斥、读者不阻塞，提交前还会复核快照代号，因此并发入库、改线或归并
不会漏改、不会复活旧点、也不会写回旧坐标。
"""

CONFLICT_RESPONSES = {
    409: {
        "model": ConflictDetail,
        "description": "别名/重复引用冲突：整次操作未写入任何数据",
    }
}


@asynccontextmanager
async def lifespan(app: FastAPI):
    init_db()
    yield


app = FastAPI(
    title="Municipal Road Geometry Service",
    description=DESCRIPTION,
    version="1.1.0",
    lifespan=lifespan,
)


@app.get("/health", response_model=HealthResponse)
def health() -> HealthResponse:
    with read_tx() as conn:
        mode = conn.execute("PRAGMA journal_mode").fetchone()[0]
        conn.execute("SELECT count(*) FROM points").fetchone()
    return HealthResponse(status="ok", db=settings.db_path, journal_mode=mode)


@app.post(
    "/api/v1/batches",
    response_model=BatchResult,
    status_code=201,
    responses=CONFLICT_RESPONSES,
)
def post_batch(
    batch: BatchIn,
    pause_ms: int = Query(
        default=0,
        ge=0,
        le=30_000,
        description="演示用：规划完成后、进入写事务前暂停指定毫秒；证明组线期间"
        "不持写锁，另一进程仍可入库。",
    ),
    fail: bool = Query(
        default=False, description="演示用：在全部写入之后、提交之前注入异常以验证整批回滚。"
    ),
):
    try:
        result, _attempts = store.execute_batch(
            batch, pause_ms=pause_ms, inject_failure=fail
        )
    except store.ConflictError as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc
    except store.ConcurrentConflict as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except store.DemoFailure as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except sqlite3.OperationalError as exc:
        raise HTTPException(status_code=503, detail=f"database busy: {exc}") from exc
    return result


@app.post(
    "/api/v1/points/merge",
    response_model=MergeResult,
    status_code=200,
    responses=CONFLICT_RESPONSES,
)
def post_merge(
    merge: MergeIn,
    pause_ms: int = Query(
        default=0,
        ge=0,
        le=30_000,
        description="演示用：影响面分析完成后、进入写事务前暂停指定毫秒；证明归并"
        "分析期间不持写锁，另一进程仍可入库（含旧编号别名入库）。",
    ),
    fail: bool = Query(
        default=False, description="演示用：在全部写入之后、提交之前注入异常以验证归并整体回滚。"
    ),
):
    """归并两个现存非别名测点。

    保留点（`keep_point_id`）沿用其原坐标；被归并点（`merged_point_id`）的
    全部路段引用替换为保留点、测点删除，其编号永久成为保留点的别名，
    被归并点原有的别名在同一事务内跟随到保留点。
    """
    try:
        result, _attempts = store.execute_merge(
            merge, pause_ms=pause_ms, inject_failure=fail
        )
    except store.ConflictError as exc:
        raise HTTPException(status_code=409, detail=exc.detail()) from exc
    except store.ConcurrentConflict as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    except store.DemoFailure as exc:
        raise HTTPException(status_code=500, detail=str(exc)) from exc
    except sqlite3.OperationalError as exc:
        raise HTTPException(status_code=503, detail=f"database busy: {exc}") from exc
    return result


@app.get("/api/v1/state", response_model=StateResponse)
def get_state() -> StateResponse:
    """测点、别名与全部已持久化线形，来自**同一个读快照**。

    返回的每个坐标、每条别名与每条线形都属于同一已提交状态，不会出现
    “新坐标 + 旧线形”或别名与测点跨代混合的视图。
    """
    with read_tx() as conn:
        generation = store.get_generation(conn)
        points = store.list_points(conn)
        aliases = store.list_aliases(conn)
        roads = store.list_roads(conn)
    return StateResponse(
        generation=generation, points=points, aliases=aliases, roads=roads
    )


@app.get("/api/v1/aliases", response_model=AliasesResponse)
def get_aliases() -> AliasesResponse:
    with read_tx() as conn:
        aliases = store.list_aliases(conn)
    return AliasesResponse(aliases=aliases, total=len(aliases))


@app.get("/api/v1/aliases/{alias_id}", response_model=AliasOut)
def get_alias(alias_id: str) -> AliasOut:
    with read_tx() as conn:
        alias = store.get_alias(conn, alias_id)
    if alias is None:
        raise HTTPException(
            status_code=404, detail=f"alias {alias_id!r} not found"
        )
    return alias


@app.get("/api/v1/points", response_model=PointsResponse)
def get_points() -> PointsResponse:
    with read_tx() as conn:
        points = store.list_points(conn)
    return PointsResponse(points=points, total=len(points))


@app.get("/api/v1/points/{point_id}", response_model=PointIn)
def get_point(point_id: str) -> PointIn:
    with read_tx() as conn:
        point = store.get_point(conn, point_id)
    if point is None:
        raise HTTPException(status_code=404, detail=f"point {point_id!r} not found")
    return point


@app.get("/api/v1/roads", response_model=RoadsResponse)
def get_roads() -> RoadsResponse:
    with read_tx() as conn:
        roads = store.list_roads(conn)
    return RoadsResponse(roads=roads, total=len(roads))


@app.get("/api/v1/roads/{road_id}", response_model=RoadOut)
def get_road(road_id: str) -> RoadOut:
    with read_tx() as conn:
        roads = store.list_roads(conn, road_id=road_id)
    if not roads:
        raise HTTPException(status_code=404, detail=f"road {road_id!r} not found")
    return roads[0]

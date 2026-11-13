# 市政道路线形加工服务

测绘队共享测点 → 路段线形（GeoJSON LineString）加工与入库服务，支持**测点归并与永久别名**。
FastAPI + SQLite（WAL），两个 API 进程共用一个数据库文件，Docker Compose 一键启动。

## 启动

```bash
docker compose up --build -d
```

* API A：<http://localhost:8001>  Swagger UI：<http://localhost:8001/docs>
* API B：<http://localhost:8002>  Swagger UI：<http://localhost:8002/docs>
* 两个进程共用卷 `gis-data:/data/gis.db`（命名卷，`docker compose down` 不会丢数据；要清空请 `docker compose down -v`）。

跑一遍样例（可选，会自动做在线演示，见文末）：

```bash
docker compose --profile demo run --rm demo
```

重启验证（数据仍在，别名与改写后的线形一并保留）：

```bash
docker compose restart api-a api-b
curl -s http://localhost:8001/api/v1/state | head -c 400
```

## 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/api/v1/batches` | 一个批次=一个事务：批量新增/更正测点与路段、批量删除；**路段入库先解析别名** |
| POST | `/api/v1/points/merge` | **测点归并**：保留点沿用原坐标，替换被归并点的全部路段引用、删除该点，旧编号永久成为别名 |
| GET | `/api/v1/aliases` / `/api/v1/aliases/{id}` | 查询全部永久别名 / 单个别名解析目标 |
| GET | `/api/v1/state` | **一个读快照**返回全部测点原值、全部别名与全部已持久化线形（含 `generation`） |
| GET | `/api/v1/points` / `/api/v1/points/{id}` | 查询测点（别名编号不在测点表内，返回 404） |
| GET | `/api/v1/roads` / `/api/v1/roads/{id}` | 查询路段（只读已持久化的 `geometry`，不现场组线） |
| GET | `/health` | 健康检查（含 WAL 模式） |

### 测点归并 `POST /api/v1/points/merge`

请求体：

```json
{ "keep_point_id": "PT-001", "merged_point_id": "PT-007" }
```

* 两个入参必须是**现存、非别名**测点；相同返回 422，不存在或为别名返回 409。
* 保留点 `keep_point_id` **沿用其原坐标**（坐标不被被归并点覆盖）。
* 被归并点 `merged_point_id` 的**全部路段引用**替换为保留点，受影响路段在同一事务内重算线形并
  `revision + 1`；随后删除被归并点，其编号在 `point_aliases` 中**永久**成为保留点的别名。
* 被归并点原有的别名在同一事务内**整体跟随**到保留点（别名始终直指现存点、永不指向另一个别名，
  因此链式归并不可能形成别名环）。
* 若替换后任一路段对保留点出现**重复引用**，整次归并 **409 且零写入**，响应 `road_ids` 列出冲突路段，
  **不自动删顶点**。
* 演示参数 `pause_ms`（影响面分析暂停，**事务外**）、`fail=true`（写后注入异常演示回滚），
  均可在 `/docs` 的 “Try it out” 直接使用。

响应：

```json
{
  "keep_point_id": "PT-001",
  "merged_point_id": "PT-007",
  "new_alias_ids": ["PT-007"],
  "repointed_alias_ids": [],
  "affected_road_ids": ["RD-101"],
  "roads": [ { "id": "RD-101", "point_ids": ["PT-001", "PT-002"], "...": "..." } ],
  "commit_attempts": 1
}
```

### 409 冲突（整次操作零写入，不自动删顶点）

所有 409 响应体结构统一：

```json
{ "detail": { "error": "duplicate_road_reference", "message": "...", "point_ids": [], "road_ids": ["RD-101"] } }
```

| `error` | 触发场景 | 清单字段 |
| --- | --- | --- |
| `alias_id_conflict` | 用别名编号 upsert/删除测点；或把别名作为归并入参 | `point_ids` |
| `aliased_point_delete` | 直接删除仍拥有别名的测点（只能通过归并消化） | `point_ids` |
| `duplicate_road_reference` | 归并替换引用、或批次内别名解析后，任一路段出现重复测点 | `road_ids` |
| `merge_conflict` | 归并入参的测点编号不存在 | `point_ids` |

> 批次 `roads` 里**原始字面量**重复仍是 422（请求体校验）；字面量不同、但别名解析后重复才是 409。

### 批次请求体（原有）

所有字段可选，按 `points → roads → delete_roads → delete_points` 顺序生效：

```json
{
  "points":       [{"id": "PT-001", "lon": 116.397428, "lat": 39.909230}],
  "roads":        [{"id": "RD-101", "point_ids": ["PT-001", "PT-002", "PT-003"]}],
  "delete_points": ["PT-004"],
  "delete_roads":  ["RD-102"]
}
```

* 路段 `point_ids`：≥ 2 个、有序、批内字面量不可重复；允许引用**尚不存在**的测点（该路段进入 `incomplete`）。
* **别名解析**：路段入库/改线时，每个测点编号先经别名表解析到现存的规范编号，持久化的
  `road_points` 永远只存规范编号；解析后重复即 409。
* 已存在 id 视为**更正**（upsert）：测点改坐标、路段改引用顺序/成员。
* 同一 id 不能在同一批次里既 upsert 又 delete（返回 422）。
* 演示用查询参数：`pause_ms`（组线暂停，**事务外**）、`fail=true`（写完后注入异常以演示回滚）。

响应里的路段：

```json
{
  "id": "RD-101",
  "point_ids": ["PT-001", "PT-002", "PT-003"],
  "status": "ok",
  "geometry": {"type": "LineString", "coordinates": [[116.397428, 39.909230], ...]},
  "missing_point_ids": [],
  "revision": 3,
  "updated_at": "2026-11-13T07:42:30.316Z"
}
```

## 关键行为

1. **组线即持久化**：线形在提交事务内按 `point_ids` 顺序组装，写入 `roads.geometry`；
   查询只 SELECT 该列，绝不临时组线。
2. **缺点不输出残线**：任一引用测点不存在 → `geometry = null`、`status = incomplete`、
   `missing_point_ids` 列明缺失 ID（按路段顺序去重）。补点后**下一次批次**自动恢复线形，无需额外调用。
3. **影响面完整**：更正/删除测点、改变路段引用或归并测点时，所有受影响路段（含引用该测点及其
   别名的全部路段）在**同一事务**里重算；未受影响的路段不重写（`revision` 不变）。
4. **整批/整次原子**：批次或归并中途任何异常（含写完后注入）整体回滚，`generation` 不前进，
   别名、测点、引用与持久线形要么全部提交、要么全部不改。
5. **别名永久、无环、不复活**：别名行只增不改向（链式归并只把别名改指到新的现存保留点），
   别名编号不能再 upsert/删除/作为归并入参（409），拥有别名的测点不能直接删除（409）。
6. **重启保留**：SQLite 文件在命名卷里，`docker compose restart` / 重建容器后测点、别名与线形都在。

## 并发模型（两个进程共库）

* 每个请求一条短连接；写事务一律 `BEGIN IMMEDIATE` + `busy_timeout=30s`，两个进程的写入互斥但排队等待，不报 busy。
* **组线与归并影响面分析都在事务之外**：先在一个 WAL 读快照上把规划/引用替换/组线做完（含 `pause_ms`
  暂停），再进入极短的写事务。因此暂停的慢批次或慢归并不会挡住另一进程的入库——包括用正在退役的
  旧编号入库。
* **提交前复核快照代号**（`store_meta.generation`）：规划期间若有别的进程提交过，本次规划整体作废、
  从新快照重算（响应里 `commit_attempts` 可见）。归并在写事务内还会再次确认两个入参仍是现存非别名
  测点，因此并发归并不会漏改、不会把已退役的旧点复活，也不会写回旧坐标或形成别名环。
* 归并/别名解析与新增引用、改线、另一归并交错时，后来者要么基于最新已提交状态重算成功，要么得到
  **零写入的 409**，绝不会出现半应用状态。
* **读一致性**：`/api/v1/state` 在一个延迟事务里完成全部读取，返回的测点、别名与线形必定来自同一
  已提交状态。
* WAL 下读者不阻塞写者、写者不阻塞读者；`synchronous=FULL` 保证已提交数据断电安全。

## 本地运行与测试

```bash
python -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
GIS_DB_PATH=/tmp/gis.db uvicorn app.main:app --port 8001

pytest            # 功能 + 双进程并发（测试会真的起两个 API 进程共库运行）
```

（无 docker 环境时也可直接验证整套并发行为：`tests/test_concurrency.py` 与
`tests/test_merge_concurrency.py` 用 `tests/server_runner.py` 起两个独立进程，连接同一个 SQLite 文件。）

* `tests/test_batch.py`：组线、缺项、原子批次、回滚等基础功能；
* `tests/test_merge.py`：归并语义、永久别名、别名解析、四类 409、链式归并、重启保留、失败回滚；
* `tests/test_merge_concurrency.py`：双进程下暂停归并不持写锁、并发归并不复活/不成环、
  与入库改线交错的不变量、并发旧编号入库；
* `tests/test_concurrency.py`：双进程跨进程一致性、暂停组线不持写锁、并发改线无漏更新、
  `/state` 在持续写入下的快照一致性；
* `tests/test_retry.py`：可控地制造“规划后另一进程提交”，断言批次与归并的旧规划都被丢弃并重算。

## 演示样例（要点）

`examples/run_demo.py`（compose profile `demo`）对两个进程逐步演示。前 6 步为批次入库/缺项/恢复/
并发改线/失败回滚；第 7、8 步为归并与别名：

7. **测点归并**：先演示会产生重复引用的归并被 409 拒绝且零写入；再做一次干净归并——保留点坐标不变、
   被归并点的路段引用被替换、旧编号成为永久别名；随后演示查别名、用旧编号入库（自动解析到规范编号）、
   写/删别名编号与删除别名属主均返回 409；
8. **链式归并与并发旧编号入库**：第二次归并时旧别名整体跟随（无别名环），两代以前的旧编号仍能解析；
   最后 A 提交一个 `pause_ms=2000` 的归并，B 在暂停期间用**正在退役的旧编号**入库（用时 < 1s，未被
   写锁阻塞）；无论谁先提交，最终旧点不复活、无别名环、引用全部为现存编号，冲突时为零写入的 409。

本地直接跑（不用 compose）：

```bash
uvicorn app.main:app --port 8001 &  uvicorn app.main:app --port 8002 &
python -m examples.run_demo --base-a http://localhost:8001 --base-b http://localhost:8002
```

## 说明

* 缺测点用 `point_id` 不加外键约束是刻意设计：悬空引用是“可表示状态”，才能稳定报告缺失 ID
  并在补点后自动恢复；路段删除时其引用行由 `ON DELETE CASCADE` 清理。
* `point_aliases.canonical_id` 不加外键到 `points` 同样是刻意的：归并在同一事务内“先改写全部引用、
  再删旧点、再写别名”，外键会在中途瞬时违反；应用层保证别名永远指向提交后的现存点，并有测试守护
  “无悬空别名、无别名环”不变量。
* 单机 SQLite 的写入吞吐上限是设计边界：本方案保证**正确性**，写入串行化 + 组线/归并分析在事务外
  使得暂停/慢分析不会拖住其他入库。若未来需要多机横向扩展，应将 `store_meta.generation`
  复核替换为外部分布式锁/版本号。

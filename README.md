# 管道工坊 · 实时数仓与流式管道平台

面向实时数据接入、流式转换、变更捕获、窗口聚合、状态管理、断流恢复与多维数据集下发的实时数仓与流式管道开发平台。

这是一个前后端分离的管理平台：前端 Vue 3 + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 Vue 3 + Vite + TypeScript 前端
│   ├── src/views/            每个业务模块一个页面
│   ├── src/api/              统一请求封装
│   ├── src/stores/           会话与筛选状态
│   └── vite.config.ts        dev server 配置（open: false）
├── backend/                  FastAPI（Python） 后端
│   ├── app/routers/          每个业务模块一组接口
│   ├── app/services/         业务规则与状态流转
│   └── app/store.py          内存数据仓库与示例数据
├── .gitignore
└── docker-compose.yml
```

## 启动

### 后端

```bash
cd backend
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt
./run.sh
```

健康检查：`curl http://127.0.0.1:8000/api/health`

### 前端

```bash
cd frontend
npm ci
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 源端绑定 | `source_binding` | 数据源端 | 源端编号、源端类型、连接地址 |
| 变更捕获 | `cdc_task` | CDC任务 | 任务编号、源数据库、监控表 |
| 流式转换 | `stream_transform` | 转换算子 | 算子编号、输入流、输出流 |
| 窗口聚合 | `window_agg` | 时间窗口 | 窗口编号、聚合指标、窗口类型 |
| 状态管理 | `state_store` | 算子状态 | 状态编号、关联算子、存储后端 |
| 规则引擎 | `rule_engine` | 业务规则 | 规则编号、规则条件、触发动作 |
| 水位管理 | `watermark_mgr` | 事件水位 | 水位编号、关联流、水位类型 |
| 数据落盘 | `sink_writer` | 输出槽位 | 槽位编号、输出类型、目标地址 |
| 背压控制 | `backpressure` | 背压策略 | 策略编号、关联算子、队列上限 |
| 断流恢复 | `checkpoint` | 检查点 | 检查点编号、关联流水线、位点时间 |
| 流式质检 | `data_quality` | 质检规则 | 规则编号、关联流、检查项 |
| Schema演进 | `schema_evolve` | 结构变更 | 变更编号、关联流、变更类型 |
| 管道编排 | `pipeline_orch` | 管道DAG | 管道编号、管道名称、节点列表 |
| 去重管理 | `dedup_mgr` | 去重策略 | 策略编号、关联流、去重键 |
| 指标计算 | `metric_calc` | 实时指标 | 指标编号、指标名称、计算公式 |
| 事件路由 | `event_router` | 路由规则 | 规则编号、事件类型、匹配条件 |
| 历史回放 | `replay_batch` | 回放任务 | 任务编号、数据源、起始时间 |
| 分组分区 | `grp_partition` | 分组策略 | 策略编号、关联流、分区键 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.vue`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 分窗运行时与指标口径

窗口聚合不再按整批订单出数，而是按事件时间归桶后再聚合：

- `app/services/window_runtime.py`：按窗口配置的 `窗口长度秒` / `滑动步长秒` 计算桶边界。
  滚动窗口只落一个桶，滑动窗口按步长枚举，重叠部分允许同一订单进入多个桶；
  同一桶内重复补投（含同一批次内）只计一次。
- 水位按 `水位延迟秒` 推进，桶 `[开始, 结束)` 结束后封账。封账后在迟到阈值内到达的订单
  生成新的计算批次，批次保留 `原桶引用`、`上一批次` 和 `本次纳入` 的记录范围；指标按桶
  只选当前批次，不能同一指标混用两个桶版本。超出阈值的订单进入窗外记录，带落窗原因。
- `app/services/metric_runtime.py`：只消费分窗聚合结果，产出指标与指标发射快照；
  已发指标可按当时的桶 / 批次复算（`/api/metric_runtime/emissions/{id}/recompute`）。
- 下游确认 `POST /api/window_runtime/confirm` 的订单范围与指标来源一致；可指定
  `emission_id` 按当时引用的批次确认。

主要接口：

| 接口 | 说明 |
| --- | --- |
| `POST /api/window_runtime/run` | 分窗执行入口，返回实际引用的聚合批次 |
| `GET /api/window_runtime/buckets` | 按窗口、时段、封账状态或订单编号筛选分桶 |
| `GET /api/window_runtime/bucket-metrics` | 分桶指标清单：按桶查金额与独立用户 |
| `GET /api/window_runtime/batches` | 批次版本、记录范围与更正关系 |
| `GET /api/window_runtime/outside` | 窗外记录，按事件时段检索并带原因 |
| `POST /api/window_runtime/confirm` | 下游确认（范围与指标来源一致） |
| `POST /api/metric_runtime/run` | 消费分窗聚合结果计算指标 |
| `GET /api/metric_runtime/{指标编号}` | 指标详情，交代实际引用的聚合批次 |

迟到处理策略默认取环境变量 `WINDOW_LATE_POLICY`（`立即更正` 出数快但更正多，
`延迟合并` 等下游确认后合并，更正少但出数慢），也可在分窗执行时按次指定。

领域：数据工程
子方向：数仓与数据湖

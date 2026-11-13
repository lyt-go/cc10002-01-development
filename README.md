# 数仓分层建模与治理平台

面向原始数据落地、清洗转换、分层建模、维度事实设计、分区治理、质量校验、血缘追踪与历史补数的数据仓库工程管理平台。

这是一个前后端分离的管理平台：前端 React + Vite + TypeScript，后端 FastAPI（Python）。
两边各自独立启动，前端 dev server 已关掉自动打开页面，启动后按终端打印的地址手工打开。

## 目录结构

```text
.
├── frontend/                 React + Vite + TypeScript 前端
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
npm install
npm run dev
```

前端默认监听 `http://127.0.0.1:5173/`，dev server 不会自动打开浏览器，
需要自己访问。`/api` 由 vite 代理到后端 `http://127.0.0.1:8000`。

## 业务模块

| 模块 | 目录 | 业务对象 | 主要字段 |
| --- | --- | --- | --- |
| 原始落地 | `landing` | 落地批次 | 批次编号、来源系统、落地路径 |
| 入湖编排 | `ingest` | 入湖提交 | 批次编号、业务日期、分片清单 |
| 贴源层 | `ods` | 贴源表 | 表编号、表名称、来源系统 |
| 明细层 | `dwd` | 明细表 | 表编号、表名称、业务域 |
| 维度层 | `dim` | 维度表 | 维度编号、维度名称、自然键列 |
| 汇总层 | `dws` | 汇总表 | 表编号、表名称、统计粒度 |
| 应用层 | `ads` | 应用表 | 表编号、表名称、业务场景 |
| 分区治理 | `partition` | 分区方案 | 方案编号、数据表名、分区字段 |
| 质量校验 | `quality` | 质量规则 | 规则编号、校验表名、目标字段 |
| 数据血缘 | `lineage` | 血缘关系 | 关系编号、上游表名、下游表名 |
| 历史补数 | `backfill` | 补数任务 | 任务编号、目标表名、开始日期 |
| 清洗规则 | `cleanrule` | 清洗规则 | 规则编号、输入表名、空值策略 |
| 转换编排 | `transform` | 转换节点 | 节点编号、输入表名、输出表名 |
| 数据字典 | `dictionary` | 字典条目 | 条目编号、表名、字段名称 |
| 元数据管理 | `metamodel` | 元数据项 | 元数据编号、对象类型、对象名称 |
| 数据生命周期 | `lifecycle` | 生命周期策略 | 策略编号、数据表名、保留天数 |
| 存储成本 | `cost` | 成本账单 | 账单编号、数据表名、存储大小GB |
| 同步通道 | `sync` | 同步通道 | 通道编号、来源表名、目标表名 |
| 变更审计 | `auditlog` | 变更记录 | 记录编号、操作表名、变更类型 |

## 约定

- 每个模块的前端页面在 `frontend/src/views/<模块>/index.tsx`，后端接口在
  `backend/app/routers/<模块>.py`，业务规则在 `backend/app/services/<模块>.py`。
- 列表接口统一返回 `{ items, total, page, size }`，动作接口统一返回 `{ ok, message }`。
- 状态流转只允许在 `app/services` 里改，路由层不做业务判断。

## 入湖提交（landing + ods 联动）

原始文件登记和贴源表加载共用一次入湖提交，接口在 `/api/ingest`：

| 接口 | 说明 |
| --- | --- |
| `POST /api/ingest/batches` | 登记源端清单（来源系统、文件格式、原始路径、字节大小）与分片清单 |
| `POST /api/ingest/batches/{批次}/units` | 补登记迟到分片；重复路径、重复分片序号会被拒绝 |
| `POST /api/ingest/batches/{批次}/units/{分片}/verify` | 复核行数与摘要；已完成单元直接复用 |
| `POST /api/ingest/batches/{批次}/recompute` | 按本次读取内容重算分片摘要，用来发现摘要冲突 |
| `POST /api/ingest/batches/{批次}/publish` | 全部分片齐备后原子发布贴源表版本 |
| `GET /api/ingest/batches/{批次}` | 批次进度、分片明细、缺口说明、审计轨迹 |
| `GET /api/ingest/batches/{批次}/resume` | 恢复计划：只列未完成单元，已完成分片可复用 |
| `GET /api/ingest/availability` | 贴源表可用范围 |
| `GET /api/ingest/versions` | 已发布版本，已替代版本保留可追溯 |

规则：

- 每个分片是一个校验单元，保存行数、字节大小与 sha256 摘要；全部完成才发布版本。
- 任一分片失败，批次停在「落地中」，成功分片保留但贴源表保持不可用并说明缺口。
- 已发布版本不重写；同一摘要的新批次复用既有版本，摘要不同则以「先完整落库的版本」为准。
- 发布持有互斥锁，同一业务日并发发布只允许一个版本生效。
- 批次进度、贴源表可用范围、变更审计在同一次提交写入；审计按「关联批次」联动查询
  （`GET /api/auditlog?batch_no=...`），保留每次尝试的失败原因与最终通过摘要。

回归测试：`cd backend && .venv/bin/python -m unittest tests.test_ingest -v`。

领域：数据工程
子方向：数仓与数据湖

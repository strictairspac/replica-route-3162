# 站点读流量路由演练

栖原站点运维组（11人）在交接班前，用节点位点与会话时序复核读写路由，明确何时读副本、何时回主库、何时必须拒绝读取。

## 场景与模块

原先值班员在配置表和聊天记录中核对副本延迟，以“延迟很低”推断写后读安全。痛点：低延迟不等于已应用会话写位点、故障时回退策略不清、决策无逐请求证据。上游输入为人工确认的节点状态和请求轨迹，本站承接路由规则演练与证据留档，下游为站点值班与变更复核人员。

三个核心实体：复制节点 Node（主副角色/在线状态/日志谱系/位点/延迟），会话请求集 RequestSet（带时序的读写、复制应用、故障恢复事件），路由试验 Trial（冻结输入、策略参数与结果）。4业务页+登录页，另有管理员审计。

**这是受控复制路由模型，不连接真实主从集群。** MySQL真实保存节点、请求、任务及证据；Python真实逐事件计算策略。日志位点采用命名谱系中的单调整数，未实现完整GTID集合/真实复制协议。

## 快速启动

```sh
cp .env.example .env
docker compose up -d --build
docker compose exec -T backend python -m scripts.seed_demo
```

前端 http://localhost:18247 ，后端 http://localhost:19247/docs ，MySQL localhost:17747。

|角色|账号|密码|权限|
|---|---|---|---|
|admin|admin@qiyuan.test|Demo2026!|全部业务写入、运行与审计|
|operator|operator@qiyuan.test|Demo2026!|节点/请求编辑、运行、导出|
|viewer|viewer@qiyuan.test|Demo2026!|只读和导出，写API403|

## 一天的工作剧本

09:00 operator登录；09:05查看harbor-primary位点105、north-replica位点104/150ms、south-replica位点105/20ms；09:10打开 `QY-1117-A` 请求集；09:15确认八事件时序；09:20以100ms延迟门槛和500ms粘滞执行 `shift-20261117`；09:25检查：写入主库位点106、100ms读仍走主库、600ms南站追平106、700ms读走南站、800ms南站故障、900ms回主库、1000ms主库故障、1100ms拒绝读取；09:30导出route-1.json；09:40 viewer核验拒绝理由；10:00 admin看审计。

另有 `QY-1117-B` 高延迟副本回退主库、`QY-1117-C` 主库故障且副本未追平的拒绝案例。所有试验使用冻结快照，不会把模型故障写回节点台账。

范围分页读另含两个请求集：`QY-1117-D` 键序分页（翻页期间并发插删、重试不推进游标、末页读完）与 `QY-1117-E` portable 换节点续页。演练表单可选快照保留版本数（1–50，默认8）与续页策略（pinned/portable，默认pinned）。

## 范围分页读规则

在原有路由事件之外新增三类时序事件，仍按 at_ms 非递减进入同一条确定性时间线：

```json
[
  {"at_ms":0,"kind":"kv_put","session":"lab-KV","key":"key-01","value":"航道A"},
  {"at_ms":10,"kind":"kv_delete","session":"lab-KV","key":"key-03"},
  {"at_ms":40,"kind":"range_page","session":"lab-KV","scan_id":"scan-1","page_no":1,"start_key":"key-01","page_size":2},
  {"at_ms":90,"kind":"range_page","session":"lab-KV","scan_id":"scan-1","page_no":2},
  {"at_ms":95,"kind":"range_page","session":"lab-KV","scan_id":"scan-1","page_no":3,"retry":true}
]
```

- kv_put/kv_delete：主库在线才成功，主库位点+1并写入变更日志；delete 保留墓碑，旧快照仍可重放，所以翻页期间的增删不会改变已选快照的内容。每个会话同步记录 required_position。
- 有限快照窗口：每个节点只保留最近 N 个**数据变更版本**（纯日志复制不产生新版本、不触发淘汰）；窗口满时淘汰最旧的未锚定快照，模拟快照空间有限。窗口下界会写进逐事件证据。
- 首屏（page_no=1）：按会话 required_position 选择**当前已经持有该快照**的在线同谱系副本（在线/同谱系/位点足够/快照仍在窗口/延迟达标，五条件并列，排序沿用延迟升序、位点降序、名称字典序），**不等待任何副本追平**；无达标副本时与普通 read 一样回退主库，主库也无法提供该快照才中断并说明原因。
- 续页：携带不可变游标（scan_id + next_key + 快照位点），始终按键字典序读首屏那一份快照，因此不会丢失记录、不会读到翻页期间的新版。
- 两种续页策略的取舍：
  - **pinned（默认）**：首屏节点被锚定，其快照受窗口淘汰保护；首屏延迟等于节点延迟，速度最快。代价是锚点下线/谱系变化/快照丢失时**直接中断、不可换节点**。
  - **portable**：不锚定节点，窗口内任一合格节点都可续页、支持换节点；代价是副本首屏多一次快照登记开销（模型记为节点延迟+20ms，主库兜底无此开销），所有节点都回收该快照后仍会中断。
- 重试（retry=true）：只针对“待执行的下一页”（如响应丢失后重发同一请求），用当前游标重新取同一页，**游标与已服务页计数均不推进**；已读完的页不能用 retry 重发（入队前422），扫描 finished/interrupted 后新请求按拒绝处理。
- 中断不是任务失败：trace 中以 decision=interrupted 留档，summary 单列 interrupted 计数，并给出具体原因（锚点下线/快照窗口回收/无合格节点）。证据页按 scan_id 展示“待执行→读完/中断”的每页时序、服务节点、快照位点、游标前后值与本页键值。

## 真实计算规则

输入上限12节点、100事件；时间0–86400000ms，事件按at_ms非递减，时间相同按输入顺序执行。必须有且仅一个主节点，同谱系副本初始位点不能超过主库。事件字段：

```json
[
  {"at_ms":0,"kind":"write","session":"lab-A"},
  {"at_ms":100,"kind":"read","session":"lab-A"},
  {"at_ms":600,"kind":"replicate","node":"south-replica","position":106,"delay_ms":20},
  {"at_ms":700,"kind":"read","session":"lab-A"}
]
```

事件 kind 共八种：write/read、replicate/node_down/node_up，以及范围分页读的 kv_put/kv_delete/range_page（分页规则见上一节）。

- write：主库在线才成功；主库位点+1，记录该会话required_position和last_write_ms。失败写入不推进会话位点。
- read：若距最近成功写入 `< sticky_ms`，必须主库在线、同谱系且位点满足；否则拒绝。到达边界即结束粘滞。
- 粘滞期外：副本必须在线、同谱系、position≥会话required_position、delay_ms≤max_delay_ms，四条件同时成立。多个候选按延迟升序、位点降序、名称字典序选择。无副本达标则回主库；主库也失效则拒绝。
- replicate：只允许在线同谱系副本；位点不能回退、不能超过主库已提交位点。node_down/node_up修改模型状态。未知节点或非法复制在入队前422。
- 每个会话独立维护位点；从未成功写入的会话required=0，允许最终一致副本读取，不承诺全局强一致。低延迟绝不覆盖落后位点。

输出每事件目标/拒绝、会话所需位点、实际位点、各副本四项资格、最终节点、会话状态、汇总和SHA256。相同输入产生相同摘要；不依赖随机数或外部网络。拒绝不安全读是正常业务结果，不算任务失败。快照与结果保存在MySQL，Redis只传递任务ID。

## 工程说明

|服务|技术|职责|
|---|---|---|
|frontend|原生HTML/CSS/JS + Nginx|4业务页、编辑表单、共享组件、/api代理|
|backend|Python3.11/FastAPI/SQLAlchemy/PyJWT|校验、权限、审计、快照与导出|
|worker|Python3.11|会话时序与路由算法|
|db|MySQL8.0|真实持久化业务和JSON证据|
|redis|Redis7|持久队列|

各实体贯穿 `backend/src/{models,schemas,services,routes}` 和 `frontend/src/{api,stores,pages}`；其余强制目录含controllers/middlewares/config/constants/utils、components/common/hooks/types/router。三组件table/badge/frame共用；hooks/session/feedback和utils/forms/html共用；request_id/errors双中间件。

共享枚举角色admin/operator/viewer与任务queued/running/completed/failed位于 `backend/src/constants/enums.py` / `frontend/src/constants/enums.js`，security/trials、frame/badge消费。节点角色primary/replica和状态healthy/down由node schema及前端表单定义，事件类型由request_set schema及路由算法定义。

环境变量见.env.example：COMPOSE_PROJECT_NAME、三端口、DB_PASSWORD、JWT_SECRET、SEED_DEMO。默认密码仅本地演示、JWT8小时；seed自然键仅补缺失，不覆盖修改。schema由metadata.create_all初始建表，无在线升级引擎；范围分页特性新增的 trials 两列由启动时 `ensure_columns` 幂等补列（离线DDL见 migrations/001_initial.sql）。5服务均健康检查，backend/worker依赖健康db/redis，frontend依赖backend；数据库、Redis、worker产物使用命名卷。

本地开发：Python3.11安装backend/requirements.txt、配置DATABASE_URL/REDIS_URL，在backend运行 `uvicorn src.main:app --reload` 和 `python -m src.worker`，前端经Nginx同源代理。停止清空：`docker compose down -v --remove-orphans`。纯引擎测试不依赖外部服务：`python3 -I tests/test_scan_engine.py`（在仓库根目录执行）。

限制：单worker串行、无生产任务重试/崩溃续跑；不模拟网络随机抖动、自动选主或完整GTID集合。分页扫描的快照是模型内物化视图，不连接真实存储引擎；portable 的首屏登记开销为固定模型值20ms，用于展示策略取舍而非真实基准。Google Fonts离线回退，业务不依赖字体网络。

## License

MIT。源码外 `../../test-artifacts/gb-4108/` 含真实测试、截图与结果。

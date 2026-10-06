# 辐射实验舱 · 校准通道事务锁协调服务

在维护事务争用校准通道时，调用方以**稳定追踪标识**创建会话，按序提交
**至多 4 个事务、48 个事件**（`begin` / `share` / `exclusive` / `upgrade`
/ `release` / `commit`），并可在每步裁决中查看：

- 各通道**持锁集合**（共享 `S` / 独占 `X`）；
- 各通道 **FIFO 等待队列**；
- 累计**被撤销事务**（含开始序号与裁决序号）；
- 全局单调**裁决序号** `verdict_seq`。

纯 Python 3.11 标准库实现，**无任何第三方依赖**。

## 裁决规则

1. 共享锁互相兼容；独占锁与一切锁不兼容。
2. 每个通道严格 FIFO：**队首存在不兼容等待时，后到的兼容请求也不得越过队首**，
   一律排队；持锁释放/提交后按队首顺序推进。
3. `upgrade` 从事务已持有的共享锁升级为独占；双方分别持有不同通道共享锁并相互
   升级形成等待环时，系统撤销 **开始序号（begin_seq）最大**的事务，
   **原子清除其全部等待与锁**，随后队列推进，另一事务取得独占锁。
4. 稳定事件标识（`event_id`）：
   - 相同标识 + 相同内容重复提交 → 返回**首次裁决**（响应带 `"replay": true`）；
   - 相同标识 + 不同内容 → `409` 拒绝；
   - 非法释放、重复 begin、未 begin、撤销后继续操作、提交后继续操作 →
     `409` 拒绝，**不改变状态、不消耗裁决序号**。
5. 持久化原子性：每步裁决先在内存完成，再以
   `写临时文件 → fsync → os.replace → 目录 fsync` 原子替换整状态快照。
   在撤销持久化阶段注入中断（`before-persist` / `after-persist`）并重启后，
   状态只能位于**该事件之前**或**完整裁决之后**，且可继续处理后续合法事件。

## HTTP API

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| `GET`  | `/healthz` | 健康响应（含当前裁决序号） |
| `POST` | `/sessions` | 以稳定追踪标识创建/找回会话 `{"trace_id": "..."}` |
| `POST` | `/sessions/<id>/events` | 提交一个事件 |
| `GET`  | `/snapshot` | 持锁集合、等待队列、事务状态、被撤销事务、裁决序号 |
| `GET`  | `/events` | 已裁决的稳定事件标识列表 |
| `POST` | `/admin/crash?at=before-persist\|after-persist` | 验收用故障注入 |

事件体示例：

```json
{ "event_id": "evt-0007", "type": "upgrade", "transaction": "T1", "channel": "CH-B" }
```

事件体内也可带 `"crash": "before-persist" | "after-persist"` 模拟该事件
落盘前/后的进程崩溃（容器 `restart: unless-stopped` 会自动重启）。

裁决响应包含：`verdict_seq`、`status`（`ok`/`deadlock`）、`action`
（`began`/`granted`/`queued`/`released`/`committed`/`deadlock`）、
`victim`、`holders`、`queues`、`aborted` 以及完整 `snapshot`。

## 运行（Docker Compose）

```bash
# 宿主机端口可配置（默认 8080）
HOST_PORT=18080 docker compose up -d --build lock-coordinator
curl -s http://localhost:18080/healthz
```

验收（单元测试 + 构建检查 + API/HTTP 冒烟，退出码报告结果）：

```bash
docker compose up --build --abort-on-container-exit --exit-code-from verify verify
# 或：docker compose run --build verify ; echo $?
```

冒烟脚本会真实复现：FIFO 队列推进（兼容请求不越过队首）、相互升级成环后
撤销 begin 序号最大者并让对方取得独占锁，以及 `after-persist` /
`before-persist` 两次中断重启后的恢复查询与重放。

## 本地运行（无 Docker）

```bash
python3 -m unittest discover -s tests -p 'test_*.py' -v   # 单元测试
LOCK_PORT=8080 python3 -m app.server                      # 启动服务
BASE_URL=http://127.0.0.1:8080 python3 tests/smoke_http.py
```

## 目录

```
app/engine.py    锁协调引擎（队列、等待图、死锁裁决、原子持久化）
app/server.py    HTTP 服务（会话、事件提交、快照查询、故障注入）
tests/           单元测试、HTTP 测试与端到端冒烟、verify 入口
Dockerfile       纯标准库镜像
compose.yaml     lock-coordinator 服务 + verify 验收容器
```

# 本地可靠消息队列（Reliable Message Queue）

一个**本地可运行**的可靠消息队列，用本地文件持久化模拟 Broker 与消费组状态，
使「并发消费、失败重试、应用重启」后的投递结果可预期、可验证。

- 纯 Java 21 + Spring Boot 3.4，无外部中间件依赖
- JSON Lines 追加日志做持久化，每次写入 `force` 强制刷盘
- 提供 HTTP 接口，`curl` 即可完成发布 / 拉取 / 确认 / 重试 / 死信重放的端到端验证
- 18 个单元/集成测试覆盖并发、失败、重启与容量耗尽场景

---

## 1. 投递语义

| 语义 | 保证 |
|------|------|
| 投递级别 | **至少一次（at-least-once）**。租约超时、`nack`、进程异常退出 / 重启，未确认消息都会被再次投递 |
| 消费去重 | 由**幂等键（idempotencyKey）**实现：同一主题下相同幂等键只产生**第一次**可见投递，重复发布直接拒绝（HTTP 409） |
| 分区有序 | 分区由 `FNV-1a(partitionKey) mod partitionCount` 决定；**同一分区键永远落到同一分区** |
| 同分区相对顺序 | 每个 `(消费组, 分区)` 的未确认窗口为 **1**：前一条未确认（或仍在退避）时，同分区后续消息不交付，因此同分区严格按位点顺序 |
| 消费组独立 | 每个 group 各自维护位点、未确认租约、死信；不同 group 可独立消费同一主题全量消息 |
| 不阻塞其他分区 | 某条消息消费失败仅占用其所属分区的窗口；`poll` 轮询其余分区，其它分区照常交付 |

**至少一次意味着重复投递是正常现象**，消费方必须用幂等键（或业务侧唯一约束）做去重。

### 发布侧的拒绝（原因彼此可区分）

| 场景 | 异常 / HTTP reason | HTTP 状态码 |
|------|-------------------|-------------|
| 主题不存在 / 主题名非法 / 创建已存在但配置冲突的主题 | `InvalidTopicException` | 400 |
| 负载超过主题 `maxPayloadBytes` | `PayloadTooLargeException` | 400 |
| 幂等键重复 | `DuplicateMessageException` | 409 |
| 主题消息总数达到 `capacity` | `TopicCapacityExceededException` | 507 |

主题名 / 消费组名规则：字母或数字开头，仅含字母、数字、`._-`，长度 1–64。

---

## 2. 幂等范围

- 幂等键的去重范围是 **「主题」级别**：在同一 `topic` 内唯一；不同主题可以使用相同的幂等键而互不影响。
- 幂等键为可选项：不传（null/空）表示不做幂等约束。
- 判重发生在**发布时**：重复键不会写入消息日志、不会产生第二条消息，对所有消费组都不可见。
- 死信**重放也使用确定性幂等键**（`dlq-replay:<deadLetterId>`）：同一条死信重复重放返回同一消息回执，
  不产生第二次可见投递。
- 幂等索引持久化在 `idem/<topic>.log`，应用重启后继续生效。

> 注意：幂等键保证「发布不重复」；消费侧的「确认不重复」由位点 + 交付租约共同保证（见第 4 节）。

---

## 3. 重试与退避规则

消费失败调用 `nack(group, deliveryId, error)`：

1. 本次交付**立即失效**（旧 `deliveryId` 不能再 ack/nack，否则收到 `LeaseExpiredException`）。
2. 消息进入指数退避，在退避结束前 `poll` **不会**重新交付该分区：
   `backoff(attempt) = min(backoffBase * 2^(attempt-1), backoffMax)`
   默认 `backoffBase=200ms`、`backoffMax=30s`，即 200ms → 400ms → 800ms → … → 30s 封顶。
3. 退避结束后重新投递，`attempt` 加 1；消息的**分区与位点不变**。
4. 当 `attempt` 达到 `maxAttempts`（默认 5，含首次交付）时：
   - 消息进入该消费组的**死信队列（DLQ）**，记录尝试次数与最后错误；
   - 该消息位点随之推进（毒消息被"跳过"），**不阻塞同分区后续消息**；
   - 其它分区在整个重试期间始终可消费。

重试只阻塞「同一分区」的有序窗口，**不会阻塞同主题其他分区**。

---

## 4. 位点（Offset）规则

- 位点语义：`committedOffset` 是「该分区最后已确认消息的序号」，初始为 **-1**。
- 每条消息在分区内有单调递增序号（`seq`，从 0 开始），即其位点。
- **已确认位点必须单调**：只能严格 +1 前进（ack 路径），或显式提交到已存在的更大序号（管理接口）。
- 未确认消息的重新投递：
  租约超过 `rmq.visibility-timeout`（默认 30s）未确认，即视为消费者异常 / 超时；
  下次 `poll` 该分区时重新投递同一条（位点不变，attempt+1），旧交付 ID 立即失效。
  应用重启后，未确认租约从 `leases.log` 恢复，超时后同样重新投递，**消息不丢失**。

### 确认侧的拒绝（原因彼此可区分）

| 场景 | 异常 / HTTP reason | HTTP 状态码 |
|------|-------------------|-------------|
| 位点回退 / 跳跃（前序未确认） | `OffsetRegressionException` | 400 |
| 用 A 消费组提交 B 组持有的交付 | `CrossGroupCommitException` | 400 |
| 对已确认的交付重复确认（或交付不存在） | `DuplicateAckException` | 400 |
| 旧交付已被超时重投取代后再来确认 | `LeaseExpiredException` | 400 |

判定优先级：先查本组当前租约 → 再查是否属于其他组（跨组）→ 再判旧交付失效 → 最后判重复确认。

---

## 5. 死信（DLQ）与重放规则

- 死信按「消费组 + 主题」维度隔离，可通过 `deadLetters(group, topic)` / `allDeadLetters(group)` 或 HTTP 接口查询。
- 每条死信记录：`deadLetterId`、原消息（含分区键/负载/幂等键）、分区、位点、尝试次数、最后错误、入死信时间。
- **重放** `replayDeadLetter(group, deadLetterId)`：
  1. 以**新消息**形式追加到原分区的**队尾**，获得新位点（不移动、不覆盖任何历史位点）；
  2. 使用确定性幂等键，重复重放返回同一回执，不产生第二次可见投递；
  3. 重放消息的 `attempt` 从 1 重新开始；
  4. 重放标记持久化，应用重启后已重放的死信不会再次出现在死信列表中；
  5. 重放**不删除、不改写**历史日志，因此不会破坏已有位点、幂等语义，也不会造成消息丢失。
- 若重放时主题容量已满，会以 `TopicCapacityExceededException`（HTTP 507）明确拒绝；
  此时死信**不会**被标记为已重放，仍可查询，扩容/清理后可再次重放。

---

## 6. 本地持久化布局

数据根目录由 `rmq.data-dir` 决定（默认 `data/rmq/`）：

```
data/rmq/
├── topics/<topic>.json             主题配置
├── messages/<topic>/p<n>.log       分区消息顺序日志（每行一条 JSON，行号即位点）
├── idem/<topic>.log                幂等键索引（key -> messageId/partition/offset）
└── groups/<group>/
    ├── offsets.log                 已确认位点（topic/partition/offset，追加）
    ├── leases.log                  未确认租约（upsert / deleted 墓碑 / superseded 取代记录）
    └── dlq.log                     死信条目 + replayed 重放标记
```

- 所有日志为 append-only JSON Lines，写入后 `FileChannel.force(true)` 强制刷盘。
- 启动时重放日志，在进程内重建最新状态；因此**杀进程 / 重启 = 从日志恢复**。

---

## 7. 本地验证方法

### 7.1 运行测试

```bash
mvn test
```

覆盖（共 18 个用例）：

| 测试类 | 覆盖场景 |
|--------|----------|
| `PersistentStoreSmokeTest` | 追加日志、位点、租约、死信、幂等索引的持久化与重启恢复 |
| `PublishConsumeTest` | 同分区键有序、消费组独立、幂等键拒绝、三类发布拒绝原因可区分 |
| `RetryAndDlqTest` | 指数退避重试、重试上限进死信、不阻塞其他分区、死信重放不破坏位点/幂等、重放标记重启保持、容量满时重放被明确拒绝且死信保留 |
| `OffsetAndRestartTest` | 租约超时重投、应用重启后未确认消息再消费、位点回退/跨组/重复确认三类拒绝可区分 |
| `ConcurrencyTest` | 8 生产者 × 1000 消息 + 4 线程幂等竞争 + 6 消费者并发：不丢失、不重复、位点单调 |
| `QueueControllerTest` | HTTP 全流程冒烟与可区分错误码 |

测试日志会打印**主题、分区键、消息序号（seq）、尝试次数（attempt）与位点（offset）**，例如：

```
publish topic=orders partition=0 key=customer-7 idempotencyKey=idem-3 seq=3 attempt=- offset=3
deliver topic=orders key=customer-7 seq=3 partition=0 group=grp-a deliveryId=... attempt=1 offset=3 committed=2
nack    group=grp-a topic=orders partition=0 key=customer-7 seq=3 attempt=2 offset=3 backoffMs=400
ack     group=grp-a topic=orders partition=0 deliveryId=... attempt=2 offset=3
dead-letter group=grp-a ... attempts=5 offset=3 dlqId=...
```

### 7.2 启动应用并用 curl 验证

```bash
mvn -DskipTests package
java -jar target/reliable-message-queue-java-1.0-SNAPSHOT.jar
# 可选快速重试参数：
# --rmq.max-attempts=3 --rmq.backoff-base=100ms --rmq.backoff-max=200ms --rmq.visibility-timeout=2s
```

HTTP 接口（默认端口 8080）：

| 方法与路径 | 作用 |
|------------|------|
| `POST /api/queue/topics/{name}` | 创建主题，body：`{"partitions":2,"capacity":1000,"maxPayloadBytes":65536}` |
| `POST /api/queue/topics/{name}/messages` | 发布：`{"partitionKey":"k","idempotencyKey":"x","payload":"文本"}` 或 `payloadBase64` |
| `POST /api/queue/groups/{g}/topics/{t}/poll` | 拉取一条可交付消息 |
| `POST /api/queue/groups/{g}/acks/{deliveryId}` | 确认 |
| `POST /api/queue/groups/{g}/nacks/{deliveryId}?error=...` | 否认（重试 / 进死信） |
| `GET  /api/queue/groups/{g}/topics/{t}/dlq` | 查询死信 |
| `POST /api/queue/groups/{g}/dlq/{deadLetterId}/replay` | 死信重放 |
| `GET  /api/queue/groups/{g}/topics/{t}/partitions/{p}/offset` | 查询位点 |
| `POST /api/queue/groups/{g}/topics/{t}/partitions/{p}/offset` | 显式提交位点 `{"offset":n}` |

快速体验脚本：

```bash
B=http://localhost:8080/api/queue
curl -s -X POST $B/topics/demo -H 'Content-Type: application/json' \
  -d '{"partitions":1,"capacity":10,"maxPayloadBytes":10}'
curl -s -X POST $B/topics/demo/messages -H 'Content-Type: application/json' \
  -d '{"partitionKey":"k1","idempotencyKey":"m0","payload":"hello"}'
D=$(curl -s -X POST $B/groups/g1/topics/demo/poll)
ID=$(echo "$D" | python3 -c 'import sys,json;print(json.load(sys.stdin)["deliveryId"])')
curl -s -X POST $B/groups/g1/acks/$ID
```

### 7.3 验证重启恢复

1. 发布并 `poll` 一条消息，**不要 ack**；
2. `kill` 掉 Java 进程（模拟崩溃）；
3. 重新启动，等待超过 `rmq.visibility-timeout`；
4. 再次 `poll`：同一位点的消息以 `attempt=2` 被重新投递；
5. 已确认的位点、幂等索引、死信与重放标记都从 `data/rmq/` 恢复。

---

## 8. 配置项（application.properties，前缀 `rmq`）

| 配置 | 默认值 | 说明 |
|------|--------|------|
| `rmq.data-dir` | `data/rmq` | 本地持久化根目录 |
| `rmq.default-payload-bytes` | 65536 | 默认负载上限（创建主题可覆盖） |
| `rmq.default-capacity` | 100000 | 默认主题容量（条数，创建主题可覆盖） |
| `rmq.default-partitions` | 4 | 默认分区数 |
| `rmq.visibility-timeout` | `30s` | 消费租约时长，超时未确认即重新投递 |
| `rmq.max-attempts` | 5 | 最大尝试次数（含首次），超过进入死信 |
| `rmq.backoff-base` | `200ms` | 初始退避 |
| `rmq.backoff-max` | `30s` | 退避上限 |
| `rmq.redelivery-interval-ms` | 1000 | 超时租约扫描周期（重投在下次 poll 惰性发生） |

---

## 9. 代码结构

```
core/MessageQueue         门面：发布 / poll / ack / nack / 位点 / 死信重放 / 租约回收
core/RedeliveryService    定时扫描租约超时未确认消息（可观测；重投惰性发生在 poll）
storage/PersistentStore   JSON Lines 追加日志 + 强制刷盘 + 启动重放
model/                    Message / Delivery / InFlightLease / DeadLetterEntry / ...
exception/                8 类可区分拒绝原因异常
web/QueueController       本地 HTTP 接口
web/MqExceptionHandler    异常 -> HTTP 状态码 / reason 映射
config/QueueProperties    rmq.* 配置
```

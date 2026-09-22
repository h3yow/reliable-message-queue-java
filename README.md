# 本地可靠消息队列（Java / Spring Boot）

一个完全本地运行、无需外部中间件的可靠消息队列。消息、消费组位点、在途租约、失败退避状态与死信
都持久化在本地 JSON 快照文件中（默认 `data/queue-state.json`，写入采用临时文件 + 原子替换），
应用重启后投递结果可预期。

## 1. 投递语义

- **至少一次（At-least-once）投递**：消息只有在被 `ack` 后才不再投递；
  消费失败（`nack`）、租约超时或应用重启时，未确认消息都会被再次投递，因此**不丢消息**。
- 因为存在重投递，消费端若要求“业务上恰好一次”，应基于消息内容做业务幂等。
- 发布端通过**幂等键**保证“同一条消息不会发布两次”（见第 2 节）。
- 顺序：`分区 = hash(partitionKey) % 分区数`，**同一分区键的消息在同一分区内按位点严格有序**；
  消费时按位点升序投递，前一条未解决（未 ack 且未进死信）时，同分区后续消息不会被跳过。
- 消费组之间相互独立：不同 group 各自维护位点、租约、退避与死信，可独立消费同一主题。

## 2. 发布与幂等范围

- 幂等键的作用范围是**单个主题**：同一 topic 内重复的 `idempotencyKey` 只在首次发布成功，
  之后的重复发布被明确拒绝（`DUPLICATE_IDEMPOTENCY_KEY`，HTTP 409），**不会产生第二次可见投递**。
  不同 topic 的同名幂等键互不影响。
- 不提供幂等键时不做去重。
- 发布被拒绝时错误原因可区分：

| 情况 | 错误码 | HTTP |
| --- | --- | --- |
| 主题不存在 / 名称非法（空、含非法字符、超长） | `TOPIC_NOT_FOUND` / `INVALID_TOPIC` | 404 / 400 |
| 负载超过主题 `maxPayloadBytes` | `PAYLOAD_TOO_LARGE` | 400 |
| 超过主题容量 `capacity` | `TOPIC_CAPACITY_EXCEEDED` | 409 |
| 幂等键重复 | `DUPLICATE_IDEMPOTENCY_KEY` | 409 |

- 容量口径：没有任何消费组时，主题内所有消息都占位；存在消费组时，消息被**所有**消费组
  解决（确认或进入该死信）后才释放占用。

## 3. 未确认重投递与位点管理

- `poll` 返回消息时会生成带 `leaseToken` 的租约，租期为 `rmq.lease-millis`（默认 30s）。
- 租约到期未确认：后台回收器把消息重新置为可投递（下一次 `poll` 可见，attempt 反映当前失败次数）。
- **应用重启**：`start()` 时旧实例持久化下来的租约立即失效，未确认消息马上可再次消费。
- `ack` 必须携带正确租约令牌；租约超时重投后，用旧令牌确认会被拒绝（`DUPLICATE_ACK`）。
- 位点是**每个 group × topic × partition** 的已提交位点，语义为“该位点及之前都已解决”。
  `ack`/`nack` 进死信后，系统会把**连续已解决**的位置自动推进为提交位点。
- 位点规则（拒绝原因可区分）：
  - 提交比当前更小的位点 → `OFFSET_ROLLBACK`（位点回退，HTTP 409）；
  - 提交相同位点（重复提交）→ `DUPLICATE_ACK`（HTTP 409）；
  - 提交到尚未连续确认的位置（位点缺口）→ `OFFSET_ROLLBACK`（HTTP 409）；
  - 跨消费组确认他人持有的消息 → `CROSS_GROUP_COMMIT`（HTTP 409）；
  - 重复确认同一条消息 / 用失效或错误租约确认 → `DUPLICATE_ACK`（HTTP 409）。

## 4. 失败重试与退避

- 消费失败调用 `nack(delivery, error)`，失败次数 `attempts +1`。
- 退避为**有上限的指数退避**：

  ```
  下次等待 = min(rmq.backoff-base-millis * 2^(attempts-1), rmq.backoff-max-millis)
  ```

  默认 base=1s、max=30s。退避期内该消息不可见。
- 退避/重试只阻塞**同一分区**中排在它后面的消息；`poll` 按分区轮询，**其他分区的消费不受影响**。
- 当 `attempts >= rmq.max-attempts`（默认 5）时，消息进入**死信队列（DLQ）**，
  记录原始 topic/group/分区/位点/序号/分区键/负载/尝试次数/最后错误，并视为“已解决”，
  从而使同分区位点可以继续推进，不卡死后续消息。

## 5. 死信查询与重放

- `GET /api/queue/topics/{topic}/deadletters?group=...` 查询死信（group 可省略表示全部组）。
- 重放 `POST /api/queue/deadletters/{id}/replay?group=...`：
  - 把死信**原始负载**作为一条**全新消息**追加到同主题、同分区键（获得新 messageId、新位点、
    主题级新序号），按普通消息重新走投递/重试流程；
  - **旧消息、旧位点、死信记录原样保留**，因此不破坏已有位点；
  - 重放消息使用新的幂等键作用域（`replay:{dlqId}`），不与历史投递冲突，满足幂等语义；
  - 旧消息并未被删除，重放是“补发副本”，不会丢消息；
  - 同一死信只能重放一次，重复重放返回 `DUPLICATE_IDEMPOTENCY_KEY`。

## 6. 配置（`application.properties`）

| 属性 | 默认值 | 说明 |
| --- | --- | --- |
| `rmq.data-file` | `data/queue-state.json` | 本地持久化快照 |
| `rmq.lease-millis` | `30000` | 消费租约时长，超时重投 |
| `rmq.backoff-base-millis` | `1000` | 指数退避基数 |
| `rmq.backoff-max-millis` | `30000` | 退避上限 |
| `rmq.max-attempts` | `5` | 最大尝试次数，达到进死信 |
| `rmq.reaper-period-millis` | `1000` | 租约回收扫描周期 |

## 7. 本地验证方法

### 运行测试

```bash
mvn test
```

覆盖：并发发布/消费不丢不重、同分区键顺序、多消费组独立、失败退避重试与不阻塞其他分区、
租约超时重投、应用重启恢复、死信进入/查询/重放、容量耗尽，以及位点回退/重复/跨组等
可区分的拒绝原因。单测日志统一打印 `topic / group / key(分区键) / partition / seq(消息序号)
/ offset(位点) / attempt(尝试次数)`。

### 启动应用并用 curl 验证

```bash
mvn spring-boot:run
```

启动时会自动创建 `demo-topic`（4 分区）并发布一条欢迎消息。

```bash
# 1) 建主题
curl -s -X POST localhost:8080/api/queue/topics \
  -H 'Content-Type: application/json' \
  -d '{"name":"orders","partitionCount":2,"capacity":1000,"maxPayloadBytes":1048576}'

# 2) 发布（带幂等键，重复发第二次会被 409 拒绝）
curl -s -X POST localhost:8080/api/queue/topics/orders/messages \
  -H 'Content-Type: application/json' \
  -d '{"partitionKey":"user-1","idempotencyKey":"k-1","payload":"hello"}'

# 3) 消费（注意保存返回的 leaseToken/messageId）
curl -s 'localhost:8080/api/queue/topics/orders/poll?group=g1&max=10'

# 4a) 确认
curl -s -X POST 'localhost:8080/api/queue/topics/orders/ack?group=g1' \
  -H 'Content-Type: application/json' \
  -d '{"partition":0,"offset":0,"seq":1,"partitionKey":"user-1","messageId":"...","attempt":1,"leaseToken":"..."}'

# 4b) 或失败（触发退避重试，超过上限进死信）
curl -s -X POST 'localhost:8080/api/queue/topics/orders/nack?group=g1' \
  -H 'Content-Type: application/json' \
  -d '{...同上...,"error":"boom"}'

# 5) 查位点 / 查死信 / 重放死信
curl -s 'localhost:8080/api/queue/topics/orders/offset?group=g1&partition=0'
curl -s 'localhost:8080/api/queue/topics/orders/deadletters?group=g1'
curl -s -X POST 'localhost:8080/api/queue/deadletters/{dlqId}/replay?group=g1'
```

重启应用（`Ctrl+C` 后再次 `mvn spring-boot:run`）即可观察未确认消息被重新投递、已提交位点保持。

## 8. 代码结构

- `queue/model`：消息、投递视图、发布/重放结果、死信、主题配置等领域模型。
- `queue/exception`：各类可区分错误码异常（基类 `QueueException`）。
- `queue/state`：可持久化状态（消息、消费组、租约、退避、死信）。
- `queue/storage`：JSON 快照存储 `StateStore` 与退避算法 `BackoffPolicy`。
- `queue/ReliableMessageQueue`：核心实现（单锁串行 + 每次变更原子落盘）。
- `queue/web`：Spring Boot 装配、REST 接口与异常到 HTTP 状态码的映射。

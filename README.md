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

## 5. 死信查询、单条重放与批量重放

- `GET /api/queue/topics/{topic}/deadletters?group=...` 查询死信（group 可省略表示全部组）。
- 单条重放 `POST /api/queue/deadletters/{id}/replay?group=...`：
  - 把死信**原始负载**作为一条**全新消息**追加到同主题、同分区键（获得新 messageId、新位点、
    主题级新序号），按普通消息重新走投递/重试流程；
  - **旧消息、旧位点、死信记录原样保留**，因此不破坏已有位点；
  - 重放消息使用新的幂等键作用域（`replay:{dlqId}`），不与历史投递冲突，满足幂等语义；
  - 旧消息并未被删除，重放是“补发副本”，不会丢消息；
  - 同一死信只能重放一次，重复重放返回 `DUPLICATE_IDEMPOTENCY_KEY`。

### 5.1 批量重放

运维场景下不必一条条点重放，可按主题把一批死信一次性重放：

```
POST /api/queue/topics/{topic}/deadletters/replay-batch
```

筛选条件（都可选，除主题外）既可走查询参数，也可放在 JSON body 里（查询参数优先）：

| 参数 | 含义 |
| --- | --- |
| `group` | 只重放该消费组的死信；省略表示该主题下所有消费组 |
| `partition` | 只重放该分区的死信 |
| `deadAtFrom` / `deadAtTo` | 进入死信时间范围（epoch 毫秒，**含边界**） |

核心方法 `ReliableMessageQueue.replayDeadLetters(DeadLetterFilter)` 返回
`BatchReplayResult{topic, selected, items}`，`items` 按 **分区 → 原位点 → 死信 id** 确定排序。

**整批原子、要么全进要么一条不进**

- 批次由“筛选条件命中的全部死信”定义（**包括此前已经被单条重放过的记录**，不会把它们静默排除）。
  提交后先在**不改动任何状态**的前提下做全部预检（负载大小、是否已重放/重放幂等键冲突、主题容量），
  只有全部通过才会真正追加消息；整批只落盘一次。
- 只要其中一条此刻进不去，整批以 `BATCH_REPLAY_REJECTED`（HTTP 409）**原样拒绝，一条都不会生效**：
  不写盘、不新增消息、不占用容量、不修改任何死信记录、不推进或回退已确认进度，
  批次内外其他消息的退避/重试/租约状态也完全不变。响应信息会列出具体冲突
  （已重放的死信 id，或当前 pending、批量大小、容量）。
- **拒绝后的重试规则**：
  - **容量不足（可恢复）**：把在途消息确认/消费掉腾出容量后，用**完全相同的筛选条件**再次提交即可
    整体成功；在容量问题解决前重复提交会继续被拒，且状态依旧不变。
  - **范围内含已重放死信（不能原样重试）**：系统不做“跳过已重放那条、只重放其余”的部分生效。
    需要**缩小筛选范围**（例如补充 `group`、`partition` 或更精确的 `deadAtFrom/deadAtTo`
    时间窗）把已重放记录排除后再提交。若筛选范围内本来就一条死信都没有，返回 `selected=0`，
    属于无操作，不算拒绝。
- 单条接口的“重复重放”仍明确拒绝（`DUPLICATE_IDEMPOTENCY_KEY`）。
- 条件不合法（分区越界、时间范围 from > to）返回 `INVALID_TOPIC`（HTTP 400）。

**不破坏已有进度与分区顺序**

- 每条重放都是追加到分区尾部的全新消息（新位点、新 messageId、`replay:{dlqId}` 幂等键，
  消息上的 `replayOf` 指向来源死信 id），旧位点与已确认进度完全不变；重放本身也不会推进或回退进度。
- 同分区一批多条严格按它们**进死信之前的原位点升序**追加（选择排序为
  分区 → 原位点 → 死信 id，完全确定），因此重新消费时保持原先后关系；
  多消费组、多分区下同样适用：每个消费组在同一分区收到的重放副本顺序与其死信原位点顺序一致，
  相同筛选条件多跑几次，先后关系一致、可复现。
- 重放消息和普通消息一样走投递、失败退避重试、进死信全流程。

## 6. 分区卡点查询

### 6.1 指定消费组查询

给定主题、分区和消费组，直接查看当前“挡住同分区后续投递”的那一条消息：

```
GET /api/queue/topics/{topic}/partitions/{partition}/blocker?group={group}
```

对应方法 `ReliableMessageQueue.getPartitionBlocker(group, topic, partition)`，返回
`PartitionBlocker`。没有卡点时 `blocked=false` 且其余字段为 null；有卡点时 `blocked=true`，并带：

| 字段 | 含义 |
| --- | --- |
| `state` | `BACKOFF`（退避等待中）/ `LEASED`（租约在途）/ `LEASE_EXPIRED`（租约超时未确认，待重投）/ `READY`（立即可投递，例如重启后尚未重新拉取） |
| `messageId` / `offset` / `seq` / `partitionKey` | 卡住的是哪条消息（分区队首） |
| `attempts` / `lastError` | 已经失败多少次、最后一次错误 |
| `nextDeliverableAt` | 下一次可被投递的时间（epoch 毫秒） |
| `leaseExpiresAt` | 在途租约的到期时间（仅 `LEASED`/`LEASE_EXPIRED` 有值） |
| `publishedAt` | 消息最初发布时间 |

### 6.2 只给主题和分区（消费组可省略）

消费组**不是必填参数**：不传 `group` 时返回该分区上**每个消费组各自**的卡点聚合视图：

```
GET /api/queue/topics/{topic}/partitions/{partition}/blocker
```

对应方法 `ReliableMessageQueue.getPartitionBlockers(group, topic, partition)`（`group` 传 null），
返回 `PartitionBlockerReport{topic, partition, blocked, blockers}`：

- `blockers` 是各消费组卡点（`PartitionBlocker`）的列表，**按消费组名升序**，结果稳定可复现；
  同一分区里不同消费组各自卡在不同消息（退避/在途/超时/重启后未投递）时，可以分别看到对应信息。
- 该分区上**所有消费组都没有卡点**时，`blocked=false`、`blockers=[]`，明确表达“没有卡点”
  （即使消费组尚未建立消费也是如此）。
- 传入具体 `group` 时等价于 6.1 的单组查询，只是包成单元素列表返回。

### 6.3 共同口径

- 判定口径与 `poll` 完全一致：位点大于已提交位点、既未确认也未进死信的**分区队首**。
  因此退避等待、租约在途未确认、租约已超时待重投、应用刚重启尚未重新投递这几种真实状态都能如实反映。
- 查询是**只读**的，不会回收租约或改变状态。
- 在该卡点被解决（确认或进死信）之前，同分区排在它后面的消息不会被跨过投递；
  其他分区不受影响（队首阻塞只作用于同分区）。
- 进了死信的队首视为已解决、不再是卡点，查询会自动指向它后面第一条未解决消息。

## 7. 配置（`application.properties`）

| 属性 | 默认值 | 说明 |
| --- | --- | --- |
| `rmq.data-file` | `data/queue-state.json` | 本地持久化快照 |
| `rmq.lease-millis` | `30000` | 消费租约时长，超时重投 |
| `rmq.backoff-base-millis` | `1000` | 指数退避基数 |
| `rmq.backoff-max-millis` | `30000` | 退避上限 |
| `rmq.max-attempts` | `5` | 最大尝试次数，达到进死信 |
| `rmq.reaper-period-millis` | `1000` | 租约回收扫描周期 |

## 8. 本地验证方法

### 运行测试

```bash
mvn test
```

覆盖：并发发布/消费不丢不重、同分区键顺序、多消费组独立、失败退避重试与不阻塞其他分区、
租约超时重投、应用重启恢复、死信进入/查询/单条重放、容量耗尽，以及位点回退/重复/跨组等
可区分的拒绝原因。新增运维能力的用例：

- `BatchDeadLetterReplayTest`：批量重放成功且按原位点顺序消费、容量不足时整批被拒且消息数量/死信状态/
  已确认进度/容量占用/重试状态均不变、被拒后腾出容量用相同条件原样再提交整体成功、
  单条重放后再用覆盖同主题的条件批量提交时整批拒绝（不跳过已重放记录）且可缩小范围重试、
  按消费组/分区/死信时间范围筛选、多消费组多分区下结果顺序确定且各组按原位点顺序消费、
  相同筛选条件顺序确定可复现；
- `PartitionBlockerQueryTest`：无卡点、退避卡点（含下次可投递时间/失败次数）、租约在途与租约超时、
  应用重启后卡点仍在（READY）且不会跳过队首、死信队首不构成卡点；以及只给主题+分区（不传消费组）的
  聚合查询：无任何消费组/全部解决时明确返回无卡点、两个消费组分别处于在途与退避/超时能分别看到、
  重启后各组 READY 且确认后整体无卡点等场景。

单测日志统一打印 `topic / group / key(分区键) / partition / seq(消息序号)
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

# 5) 查位点 / 查死信 / 单条重放死信
curl -s 'localhost:8080/api/queue/topics/orders/offset?group=g1&partition=0'
curl -s 'localhost:8080/api/queue/topics/orders/deadletters?group=g1'
curl -s -X POST 'localhost:8080/api/queue/deadletters/{dlqId}/replay?group=g1'

# 6) 批量重放：按消费组 + 分区 + 进入死信时间范围筛选（参数可走 query，也可放 JSON body）
curl -s -X POST \
  'localhost:8080/api/queue/topics/orders/deadletters/replay-batch?group=g1&partition=0&deadAtFrom=1700000000000&deadAtTo=1800000000000'
# 容量不足时返回 409 BATCH_REPLAY_REJECTED（整批未生效，状态不变）；腾出容量后用相同条件重发即可成功。
# 若 409 信息提示某条死信“已重放过”，说明筛选范围里混入了此前单条重放的记录：
# 不要期望系统跳过它只处理其余（整批就是会被拒），缩小 group/partition/时间窗后再提交。

# 7) 查询分区卡点
# 7a) 指定消费组
curl -s 'localhost:8080/api/queue/topics/orders/partitions/0/blocker?group=g1'
# 7b) 只给主题和分区（group 可省略）：返回该分区每个消费组各自的卡点；
#     都没有卡点时 {"blocked":false,"blockers":[]}
curl -s 'localhost:8080/api/queue/topics/orders/partitions/0/blocker'
```

重启应用（`Ctrl+C` 后再次 `mvn spring-boot:run`）即可观察未确认消息被重新投递、已提交位点保持。

## 9. 代码结构

- `queue/model`：消息、投递视图、发布/重放结果、死信、主题配置等领域模型。
- `queue/exception`：各类可区分错误码异常（基类 `QueueException`）。
- `queue/state`：可持久化状态（消息、消费组、租约、退避、死信）。
- `queue/storage`：JSON 快照存储 `StateStore` 与退避算法 `BackoffPolicy`。
- `queue/ReliableMessageQueue`：核心实现（单锁串行 + 每次变更原子落盘）。
- `queue/web`：Spring Boot 装配、REST 接口与异常到 HTTP 状态码的映射。

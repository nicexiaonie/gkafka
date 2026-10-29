# gkafka

基于 [IBM/sarama](https://github.com/IBM/sarama) 封装的 Kafka 工具库，目标是**开箱即用**：把消费组协调、分区分配、offset 提交、重试、认证等复杂度挡在封装层内部，使用者只需注册实例、写业务函数。

## 设计理念

- **使用者视角**：API 按使用意图组织（发消息、写业务函数），不暴露底层库概念。
- **分层**：默认零配置即可运行，覆盖多数场景；高级能力（异步高吞吐、死信、SASL）按需叠加，不用就完全无感。
- **不静默吞错**：涉及数据正确性的语义（异步发送错误、失败重试/丢弃）都以使用者能理解的方式暴露。

## 安装

```bash
go get github.com/nicexiaonie/gkafka
```

依赖：`github.com/IBM/sarama`，以及 OpenTelemetry 链路追踪核心库 `go.opentelemetry.io/otel`（trace + propagation API）。要求 Go 1.25+（随 sarama v1.50.3 依赖）。SCRAM 认证所需的客户端实现由调用方自行引入（见 SASL 章节）。

> OTel 仅是 **API 依赖**，不引入任何具体后端（Jaeger/Zipkin 等）。不配置 `Tracing` 时全程不创建 span、不读写 header，零运行时开销（见链路追踪章节）。

## 快速开始

### 生产者

```go
err := gkafka.RegisterProduction("demo", &gkafka.ProductionConf{
    Brokers:  []string{"127.0.0.1:9092"},
    Topic:    "my_topic",
    Balancer: gkafka.BalancerRoundRobin,
})
if err != nil {
    log.Fatal(err)
}

p := gkafka.GetProduction("demo")
ctx := context.Background() // 接链路追踪时传入携带上游 span 的 ctx
p.Send(ctx, "key", "value")                       // 发默认 topic
p.SendTopic(ctx, "other_topic", "key", "value")   // 发指定 topic
p.SendPartition(ctx, 0, "key", "value")           // 发默认 topic 指定分区（仅当 Balancer="manual"）
p.SendAsync(ctx, "my_topic", "key", "value")      // 异步高吞吐发送
```

> 所有发送方法的首参均为 `context.Context`。不接链路追踪时传 `context.Background()` 即可；接入后传携带上游 span 的 ctx，框架会自动把链路注入消息 header（见链路追踪章节）。

### 消费者（自动模式）

```go
err := gkafka.RegisterConsumer("demo", &gkafka.ConsumerConf{
    Brokers: []string{"127.0.0.1:9092"},
    Topics:  []string{"my_topic"},
    GroupId: "my_group",
    CallFunc: func(ctx context.Context, m gkafka.Message) error {
        log.Printf("topic=%s partition=%d offset=%d key=%s value=%s",
            m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
        return nil // 返回 nil 提交 offset；返回 error 触发重试
    },
})
if err != nil {
    log.Fatal(err)
}

c := gkafka.GetConsumer("demo")
c.RunCommit() // 阻塞运行，自动消费 + 自动提交
```

## 生产者配置 `ProductionConf`

| 字段 | 类型 | 说明 | 默认 |
|---|---|---|---|
| `NetWork` | string | 已废弃：sarama 当前连接入口不使用该字段，保留仅为兼容旧配置 | - |
| `Address` | string | 已废弃：单 broker 地址，保留仅为兼容旧配置；推荐使用 `Brokers` | - |
| `Brokers` | []string | broker 地址列表，生产环境建议至少配置两个 bootstrap broker | - |
| `Topic` | string | 默认 topic，`Send`/`SendPartition` 使用 | - |
| `Balancer` | Balancer | 分区策略，见下表 | `BalancerRandom` |
| `Acks` | Acks | 发送确认级别，见下表 | `AcksLocal` |
| `Idempotent` | bool | 幂等生产者（恰好一次写入，自动配齐 acks=all 等） | `false` |
| `SASL` | SASLConf | 认证配置，见 SASL 章节 | 不认证 |
| `TLS` | TLSConf | 传输加密，见 TLS 章节 | 不加密 |
| `BatchSize` | int | 攒够多少条触发发送 | sarama 默认 |
| `BatchBytes` | int | 攒够多少字节触发发送 | sarama 默认 |
| `BatchTimeout` | Duration | 距上次发送超时则强制发出 | 约 1s |
| `WriteTimeout` | Duration | 单次网络写超时 | sarama 默认 |
| `AsyncErrHandler` | func(Message, error) | 异步发送错误回调，不设则写日志。回调在 gkafka 内部的单个后台协程中逐条执行，见下方“`AsyncErrHandler` 不要阻塞” | 写日志 |
| `Tracing` | TracingConf | 链路追踪配置，见链路追踪章节；零值（`TracerProvider` 为 nil）= 关闭 | 关闭 |
| `Logger` | Logger | 日志输出，不设用标准库 log | 标准 log |

### 分区策略 `Balancer`

| 常量 | 等价字符串 | 算法 | 适用 |
|---|---|---|---|
| `BalancerHash` | `"hash"` | 按 key 哈希取模 | 相同 key 落同一分区，保证分区内有序 |
| `BalancerReferenceHash` | `"referencehash"` | 参考哈希 | 与部分其它语言客户端对齐 |
| `BalancerRoundRobin` | `"roundrobin"` | 轮询 | 均匀分布，忽略 key |
| `BalancerRandom`（默认） | `"random"` | 随机 | 无序要求时 |
| `BalancerManual` | `"manual"` | 手动指定 | 必须配合 `SendPartition` |

> 推荐用常量；写裸字符串也合法，但**写错的非法值在 Connect 时报错**（不会静默回退到默认）。

### 确认级别 `Acks`

| 常量 | 等价字符串 | 含义 | 取舍 |
|---|---|---|---|
| `AcksLocal`（默认） | `"local"` | 等 leader 写入即确认 | 吞吐与可靠的折中 |
| `AcksAll` | `"all"` | 等所有同步副本(ISR)写入 | 最可靠、不丢，吞吐略低 |
| `AcksNone` | `"none"` | 不等确认 | 最快，但 broker 异常会丢，慎用 |

> 推荐用常量；写裸字符串也合法，**非法值在 Connect 时报错**。

> 同步异步共用同一个底层连接与同一份配置（含 `Acks`），区别仅在发送方式。

### 生产者方法

| 方法 | 模式 | 说明 |
|---|---|---|
| `Send(ctx, key, value)` | 同步 | 发默认 topic，等确认返回 error |
| `SendPartition(ctx, part, key, value)` | 同步 | 发默认 topic 指定分区（需 `Balancer="manual"`，否则返回错误） |
| `SendTopic(ctx, topic, key, value)` | 同步 | 发指定 topic |
| `SendAsync(ctx, topic, key, value)` | 异步 | 高吞吐，等 sarama 接收即返回、不等 broker 确认；积压时等待，可用 ctx 超时控制；broker 侧错误走 `AsyncErrHandler` |
| `SendMessage(ctx, m Message)` | 同步 | 发完整 Message（支持二进制 Key/Value 与 Headers），`m.Topic` 为空走默认 |
| `SendMessageAsync(ctx, m Message)` | 异步 | 同上，异步版（等待与错误语义同 `SendAsync`） |
| `Close()` | - | 关闭并刷出缓冲消息；期间异步发送的失败仍走 `AsyncErrHandler`，不计入返回值 |

> **所有发送方法首参为 `context.Context`**：用于链路追踪上下文传递与取消信号。不接 trace 时传 `context.Background()`。

> **简单字符串场景**用 `Send` / `SendTopic` / `SendAsync` 等便利方法。
> **需要 Headers**(trace 透传、租户 ID、消息元数据等)或 **value 是二进制**(protobuf / 压缩 / 加密)时,改用 `SendMessage` / `SendMessageAsync`——它们直接收 `Message`,`Key` / `Value` 是 `[]byte`,`Headers` 透传到 broker。`m.Partition` 仅在 `Balancer="manual"` 时生效。

> **同步 vs 异步**：`Send` 系列要等 sarama 接收、再等 broker 返回这条消息的结果（毫秒级），失败直接返回 error；`SendAsync` 系列只等 sarama 接收（平时微秒级）即返回，吞吐更高，但返回那一刻不代表已落盘——成功不通知，broker 侧失败异步回调。用异步务必处理 `AsyncErrHandler`。
>
> **异步发送的等待与超时**：sarama 内部积压（broker 慢或不可达、`AsyncErrHandler` 处理过慢等）时，异步发送会等待 sarama 接收（反压），而不是丢弃消息。ctx 取消或超时则放弃等待并返回错误（可用 `errors.Is(err, context.DeadlineExceeded)` / `context.Canceled` 判断），此时消息未交给 sarama，也不会走 `AsyncErrHandler`，需由调用方自行处理。对延迟敏感的场景请给 ctx 设置超时。

> **`AsyncErrHandler` 不要阻塞**：回调在 gkafka 内部的单个后台协程中逐条执行，执行完一条才会读取下一条发送结果。回调耗时过长会造成两个后果：
> - **平时**：发送结果积压，sarama 无法继续接收新消息，后续 `SendAsync` / `SendMessageAsync` 会被拖慢、等待（ctx 设了超时则超时返回错误）。
> - **关闭时**：`Close()` 会等关闭期间所有回调执行完才返回；回调越慢，服务退出越慢，回调卡死则 `Close()` 不返回。
>
> 回调里只做快速操作（打日志、计数、把失败消息放入业务自己的队列），耗时处理（写库、调接口、重发）交给业务自己的协程异步完成。

## 消费者配置 `ConsumerConf`

| 字段 | 类型 | 说明 | 默认 |
|---|---|---|---|
| `NetWork` | string | 已废弃：sarama 当前连接入口不使用该字段，保留仅为兼容旧配置 | - |
| `Address` | string | 已废弃：单 broker 地址，保留仅为兼容旧配置；推荐使用 `Brokers` | - |
| `Brokers` | []string | broker 地址列表，生产环境建议至少配置两个 bootstrap broker | - |
| `Topics` | []string | 要消费的 topic 列表，**至少一个**（支持多 topic） | - |
| `GroupId` | string | 消费组 ID，**必填** | - |
| `Ctx` | context.Context | 业务上下文（传值），传入 `CallFunc` 的 ctx 以它为父 | `Background` |
| `MinBytes` | int | 单次拉取期望最小字节 | sarama 默认 |
| `MaxBytes` | int | 单分区单次拉取最大字节（`Fetch.Max`） | 不限 |
| `MaxWait` | Duration | 拉取等待新数据最长时间 | sarama 默认 |
| `SASL` | SASLConf | 认证配置 | 不认证 |
| `TLS` | TLSConf | 传输加密，见 TLS 章节 | 不加密 |
| `MaxRetry` | int | 失败最大重试次数，`0`=无限 | `0` |
| `OnExceed` | OnExceed | 超限动作，`MaxRetry>0` 时必为 `OnExceedSkip`/`OnExceedDLQ` | `""` |
| `DLQTopic` | string | 死信 topic，`OnExceed=OnExceedDLQ` 时必填 | - |
| `InitialOffset` | InitialOffset | 新组首次消费起点，见下文 | `OffsetOldest` |
| `CallFunc` | func(ctx, Message) error | 业务处理函数，返回 nil 提交、返回 error 原地重试 | - |
| `Tracing` | TracingConf | 链路追踪配置，见链路追踪章节；零值 = 关闭 | 关闭 |
| `Logger` | Logger | 日志输出，不设用标准库 log | 标准 log |

> 同一 `GroupId` 的多个实例会自动分摊分区（rebalance）；不同 `GroupId` 各自独立消费全量。

### 消费者方法

| 方法 | 说明 |
|---|---|
| `RunCommit()` | 阻塞运行的自动消费循环（推模式）；同一消费者同时只能运行一个，`Close()` 之后调用返回错误 |
| `Close()` | 停止消费并关闭连接；会等处理中的消息结束、最后位点提交后再关闭；可重复调用，仅首次生效 |

> **`Close()` 会等处理中的消息结束**：关闭时先发出取消信号，等 `RunCommit` 完全退出（所有分区的 `CallFunc` 返回、最后一批位点提交）后，再离开消费组、关闭连接与死信 producer，避免最后一批已处理消息的位点提交失败导致重启后重复消费。因此：
> - `CallFunc` 中的长耗时处理应监听 `ctx.Done()` 及时返回，否则 `Close()` 会一直等待。
> - **不要在 `CallFunc` 内同步调用 `Close()`**：`Close()` 等 `CallFunc` 返回、`CallFunc` 等 `Close()` 返回，会互相等待而卡死。需要在处理中触发关闭时，请另起协程调用。

## 投递语义与失败处理（重要）

本库默认 **至少一次（at-least-once）**：业务函数返回 nil 时,框架先在 sarama 里**标记**该条可提交,真正提交到 broker 是由 **sarama 后台异步定时进行,默认 1 秒一次**。因此存在两段可能重复的窗口:

- **正常处理崩溃窗口**:业务函数刚返回 nil、标记完成、但 sarama 还没到下一次 ticker(最多 1 秒)前进程崩溃 → 已"成功处理"的消息因 offset 未真正提交到 broker,**重启后会被重新消费**(最多重放 1 秒的已处理消息)。
- **rebalance 窗口**:分区被重新分配时,已处理但未提交的消息会被新 owner 重新消费。

因此**业务函数必须幂等**(同一条消息重复处理不出错)——这是 at-least-once 的硬契约,而不是可选优化。

> **为什么不让它逐条同步提交以消除 1 秒窗口**:那样每条消息要多一次同步网络往返提交,吞吐从每秒几万跌到几百。这个 1 秒窗口本来就在 at-least-once 契约内,业务幂等就能覆盖,没必要为它付出几十倍吞吐损失。

业务函数返回 `error` 时按 `MaxRetry` + `OnExceed` 处理。关键权衡：**「不丢」+「不阻塞后续」+「不重复」三者最多同时满足两个。**

| 策略 | 配置 | 丢消息 | 阻塞后续 | 说明 |
|---|---|---|---|---|
| 无限重试 | `MaxRetry=0`（默认） | 否 | **是**（毒消息卡死分区） | 与旧实现行为一致 |
| 超限跳过 | `MaxRetry=N, OnExceed=OnExceedSkip` | **是** | 否 | 跳过并告警后提交 |
| 超限死信 | `MaxRetry=N, OnExceed=OnExceedDLQ, DLQTopic=...` | 否 | 否 | 转投死信 topic 后提交 |

> `MaxRetry` 与 `OnExceed` 必须配对:`MaxRetry=0` 时 `OnExceed` 须为空(无限重试);`MaxRetry>0` 时 `OnExceed` 须为 `OnExceedSkip` 或 `OnExceedDLQ`。配错在注册时即报错,不会静默忽略 `MaxRetry`。

重试退避：前 3 次立即重试，超过 3 次每次间隔 1s，超过 100 次每次间隔 5s。

> **死信队列**：把处理不动的消息投递到另一个 topic 后提交 offset，主流程继续。死信消息会保留原始 Key/Value/Headers，并追加 `gkafka-dlq-source-topic`、`gkafka-dlq-source-partition`、`gkafka-dlq-source-offset`、`gkafka-dlq-attempt`、`gkafka-dlq-error` headers 方便排查。既不丢也不卡，但死信 topic 里的消息需自行写程序消费或人工处理，否则只是换个地方堆积。死信投递本身失败时不提交、继续重试，避免丢消息。配置 `OnExceed=OnExceedDLQ` 时，死信 producer 在注册（`RegisterConsumer`）阶段即创建，创建失败则注册返回错误——死信相关的连接、认证、权限问题在启动时暴露，而不是等到第一条消息失败才发现；代价是即使从未用到死信也会多占一个到 broker 的连接。

### 消费起点 `InitialOffset`

新消费组(`GroupId`)首次连接、**没有任何已提交 offset** 时,要决定从 topic 的哪个位置开始消费:

| 常量 | 等价字符串 | 行为 |
|---|---|---|
| `OffsetOldest`(默认) | `"oldest"` | 从 topic 最早一条消息开始消费 —— 会**回放全部历史**,接老 topic 谨慎 |
| `OffsetNewest` | `"newest"` | 从消费组加入时刻之后的**最新消息**开始 —— 跳过全部历史,只看新消息 |

> ⚠️ **本字段仅在没有已提交位点时生效**(新组、或位点被重置后)。一旦消费组**正常消费过、提交过 offset**,Kafka 从已提交位点继续,本字段**不再起作用**。所以这不是"运行时跳到最新"的开关。

> 推荐用常量;写裸字符串也合法,**非法值在 Connect 时报错**(不会静默回退)。

### 业务上下文与取消信号

`CallFunc` 的首个参数 `ctx context.Context`：
- **取值**：来自 `ConsumerConf.Ctx`（你注册时传入，可携带 traceID 等 request 级值）。
- **取消**：在 `Close()` 或 rebalance 时被取消。长耗时处理应监听 `ctx.Done()` 提前退出，避免在分区即将被收走时空跑，也避免拖住 `Close()`（它会等 `CallFunc` 返回）。

```go
CallFunc: func(ctx context.Context, m gkafka.Message) error {
    select {
    case <-ctx.Done():
        return ctx.Err() // 收到关闭/rebalance 信号，提前让路
    default:
    }
    return doWork(ctx, m)
}
```

### 日志注入

生产/消费的 `Logger` 字段接受任何实现 `Printf(string, ...interface{})` 的对象（Go 标准库 `*log.Logger` 天然满足）。不设时用标准库 `log`；传入自定义实现可把 gkafka 日志接进统一体系，传 no-op 实现可静默。

## SASL 认证

支持 sarama 官方的全部 5 种机制。生产/消费的 `SASL` 字段通用。

| 常量(`Mechanism`) | 等价字符串 | 需填字段 | 说明 |
|---|---|---|---|
| `SASLPlain` | `"plain"` | `User` + `Password` | 明文凭据,建议配合 TLS |
| `SASLScramSHA256` | `"scram-sha-256"` | `User` + `Password` + `SCRAMClientGenerator` | SCRAM 客户端由调用方提供,本库不内置 |
| `SASLScramSHA512` | `"scram-sha-512"` | `User` + `Password` + `SCRAMClientGenerator` | 同上 |
| `SASLOAuth` | `"oauthbearer"` | `TokenProvider` | token 由你的 OAuth 服务提供,需实现回调 |
| `SASLGSSAPI` | `"gssapi"` | `GSSAPI` 子配置 | Kerberos 认证 |

`Mechanism` 大小写不敏感(裸字符串时),留空且 `Enable=true` 时按 `SASLPlain` 处理。推荐用常量。

```go
// PLAIN 示例
SASL: gkafka.SASLConf{
    Enable:    true,
    Mechanism: gkafka.SASLPlain,
    User:      "admin",
    Password:  "secret",
},
```

> **SCRAM 为什么要自己提供实现**：sarama 自带 PLAIN / OAUTHBEARER / GSSAPI 的完整实现，但 SCRAM 的客户端算法（需 SASLprep + PBKDF2）被它设计成可插拔接口、不内置。本库不替你引入第三方密码学依赖，使用 SCRAM 时通过 `SCRAMClientGenerator` 提供一个 `sarama.SCRAMClient` 实现即可（社区常用 `github.com/xdg-go/scram`）：

```go
import (
    "github.com/IBM/sarama"
    "github.com/xdg-go/scram" // 你自己的项目按需引入
)

// 实现 sarama.SCRAMClient（Begin/Step/Done）
type scramClient struct {
    *scram.Client
    *scram.ClientConversation
    hashGen scram.HashGeneratorFcn
}
func (s *scramClient) Begin(user, pass, authz string) error {
    c, err := s.hashGen.NewClient(user, pass, authz)
    if err != nil { return err }
    s.Client = c
    s.ClientConversation = c.NewConversation()
    return nil
}
func (s *scramClient) Step(ch string) (string, error) { return s.ClientConversation.Step(ch) }
func (s *scramClient) Done() bool                      { return s.ClientConversation.Done() }

// 配置
SASL: gkafka.SASLConf{
    Enable:    true,
    Mechanism: gkafka.SASLScramSHA512,
    User:      "admin",
    Password:  "secret",
    SCRAMClientGenerator: func() sarama.SCRAMClient {
        return &scramClient{hashGen: scram.SHA512}
    },
},
```

`SASLConf` 字段：

| 字段 | 说明 |
|---|---|
| `Enable` | 是否启用，`false`（默认）=不认证 |
| `Mechanism` | 机制名，见上表 |
| `User` / `Password` | PLAIN/SCRAM 凭据 |
| `AuthzID` | 授权身份（authzid），一般留空 |
| `TokenProvider` | `oauthbearer` 用，`sarama.AccessTokenProvider` |
| `SCRAMClientGenerator` | `scram-*` 用，返回 `sarama.SCRAMClient` 的工厂函数 |
| `GSSAPI` | `gssapi` 用，Kerberos 子配置 |

`GSSAPIConf` 字段：`AuthType`、`KeyTabPath`、`CCachePath`、`KerberosConfigPath`、`ServiceName`、`Username`、`Password`、`Realm`、`DisablePAFXFAST`。

> ⚠️ **安全**：PLAIN 下凭据为明文传输。生产环境连接公网 broker 时应同时启用 TLS。明文 + 无认证仅适用于内网可信环境。

## TLS 加密

生产/消费的 `TLS` 字段通用。启用后与 SASL 配合可实现 SASL/SSL。

```go
TLS: gkafka.TLSConf{
    Enable: true,
    // Config 为 nil 时用 Go 默认（按系统根证书校验服务端）；
    // 需双向认证或自定义 CA 时传入自定义 *tls.Config。
    Config: &tls.Config{
        RootCAs:      caCertPool,
        Certificates: []tls.Certificate{clientCert},
    },
},
```

| 字段 | 说明 |
|---|---|
| `Enable` | 是否启用 TLS，`false`（默认）= 明文 |
| `Config` | `*tls.Config`（Go 标准库），nil 且 `Enable=true` 时用系统默认校验 |

## 链路追踪（Tracing）

基于 [OpenTelemetry](https://opentelemetry.io/) 的分布式链路追踪。Kafka 在调用链里是个天然断点——生产者发完即返回，消费者在另一进程、另一时刻才处理。本库做的就是把链路上下文（W3C `traceparent`/`tracestate`）**在生产侧打包进消息 header、随消息送达消费侧、再拆出来续上链路**，使一次跨 Kafka 的调用在 trace 后端串成完整的一条。

**设计原则：默认关闭、零侵入、不绑后端。**

- **默认关闭**：`TracerProvider` 为 nil（零值）时，整套逻辑完全空转——不创建 span、不读写 header、无任何运行时开销与行为变化。
- **不绑后端**：只依赖 OTel 的 trace/propagation **API**，业务用什么后端（Jaeger、Zipkin、SkyWalking 等经 OTel 接入）都行。
- **业务无感 header**：`traceparent` 的注入/提取由框架完成，业务代码不接触 header。

### 配置 `TracingConf`

生产/消费的 `Tracing` 字段通用。

| 字段 | 类型 | 说明 | 默认 |
|---|---|---|---|
| `TracerProvider` | trace.TracerProvider | 提供 Tracer；为 nil = **关闭 trace** | nil（关闭） |
| `Propagator` | propagation.TextMapPropagator | header 上的注入/提取策略 | nil 时用 W3C TraceContext |

通常传 `otel.GetTracerProvider()`（已全局初始化 OTel SDK 时）或自建的 SDK provider。`Propagator` 留空即用 W3C 标准 `traceparent`/`tracestate`，与主流后端兼容。

### 生产侧

发送方法的首参 `ctx` 携带上游 span，框架在发送前从 ctx 提取 span 注入消息 header，并为本次发送创建一个 producer span。

```go
err := gkafka.RegisterProduction("demo", &gkafka.ProductionConf{
    Brokers: []string{"127.0.0.1:9092"},
    Topic:   "my_topic",
    Tracing: gkafka.TracingConf{
        TracerProvider: otel.GetTracerProvider(), // 启用
        // Propagator 留空 = W3C TraceContext
    },
})
// ...
p := gkafka.GetProduction("demo")

// ctx 携带上游 span（如来自 HTTP 入站、上一段业务 span）
p.SendMessage(ctx, gkafka.Message{Value: []byte("payload")})
```

> **同步 vs 异步的 span 生命周期**：同步发送的 producer span 在 `SendMessage` 返回（拿到分区/offset 或 error）后立即结束；异步发送（`SendAsync`/`SendMessageAsync`）是"投进队列即返回"，span 不在投递时结束，而是由后台在收到 broker 的成功/失败结果时结束——这样 span 才能反映真实的发送耗时与结果。

### 消费侧

消费侧无需改任何调用代码：开启 `Tracing` 后，框架从消息 header 提取父链路、创建 consumer span，并把带 span 的 ctx 传给 `CallFunc`。业务在该 ctx 上创建的下游 span（DB、RPC 等）会自动挂在 consumer span 之下。

```go
err := gkafka.RegisterConsumer("demo", &gkafka.ConsumerConf{
    Brokers: []string{"127.0.0.1:9092"},
    Topics:  []string{"my_topic"},
    GroupId: "my_group",
    Tracing: gkafka.TracingConf{
        TracerProvider: otel.GetTracerProvider(),
    },
    CallFunc: func(ctx context.Context, m gkafka.Message) error {
        // ctx 已挂在 consumer span 下；在此 ctx 上起子 span 即自动归入同一条链路
        return handle(ctx, m)
    },
})
```

> consumer span 覆盖**整条消息的处理过程**（含重试），在 `CallFunc` 最终成功、跳过或转入死信后结束。

### 工作原理小结

| 环节 | 动作 | 载体 |
|---|---|---|
| 生产 | 从 ctx 提取 span → 注入 header → 起 producer span | `messaging` 语义属性 + `traceparent` header |
| 传输 | header 随消息存入 broker、随消息送达消费侧 | Kafka 消息 header |
| 消费 | 从 header 提取父 span → 起 consumer span → 注入传给 `CallFunc` 的 ctx | — |

> **死信消息的链路**：转入 DLQ 的消息保留原始 header，因此 DLQ 消费者读到的是**原始生产者**的链路上下文（而非失败那次的 consumer span）——便于回溯消息最初的来源。

## 消息类型 `Message`

业务函数收到的中性消息类型（不暴露 sarama 类型）：

```go
type Message struct {
    Topic     string
    Partition int
    Offset    int64
    Key       []byte
    Value     []byte
    Headers   []Header // 键值对；trace 上下文（traceparent 等）经此透传
}

type Header struct {
    Key   string
    Value []byte
}
```

## 注意事项

- **业务函数幂等**：默认至少一次语义，重复消费不可避免，处理逻辑需幂等。
- **生命周期**：
  - 批量关闭：`CloseProduction()` / `CloseConsumer()`（关闭并清空注册表）。
  - 按名关闭：`CloseProductionByName(name)` / `CloseConsumerByName(name)`（关闭并从注册表移除单个实例；未注册时返回 nil，幂等）。
  - 异步生产者退出前会刷出缓冲消息。
- **同名重注册**：`RegisterProduction` / `RegisterConsumer` 在同名已存在时返回错误（避免静默泄漏旧连接与后台协程）。要替换请先调用对应的 `CloseXxxByName(name)` 显式关闭旧实例，再 `Register` 新的。
- **并发安全**：注册中心已加锁，多 goroutine 注册/获取/关闭安全。

## 与旧版本（segmentio/kafka-go）的差异

| 项 | 旧 | 新 |
|---|---|---|
| 底层库 | segmentio/kafka-go | IBM/sarama |
| 分区策略 | `kafka.LeastBytes{}` 等类型 | 字符串枚举（无 `leastbytes`，用 `roundrobin` 替代） |
| 消息类型 | `kafka.Message` | `gkafka.Message`（中性） |
| 业务函数 | `func(Message) error` | `func(ctx, Message) error`（带上下文/取消信号） |
| 消费 topic | 单 topic | `Topics []string`，支持多 topic |
| 批量拉取 | `FetchMessage` | 已移除（拉取桥接脱离消费循环，提交时机不可控） |
| SASL | 字段存在未实现 | 5 种机制全部支持（SCRAM 由调用方提供客户端实现） |
| TLS | 无 | `TLSConf` 可配 |
| 确认级别 | 写死 `RequireOne` | `Acks` 可配（`local`/`all`/`none`） |
| 幂等写入 | 无 | `Idempotent` 可配（恰好一次写入） |
| 失败处理 | 无限重试（写死） | `MaxRetry` + `OnExceed` 可配 |
| 日志 | 写死标准 log | `Logger` 可注入 |
| 生产连接 | — | 同步/异步共享一个底层连接与配置 |
| 发送方法签名 | `Send(key, value)` 等 | **首参加 `ctx`**：`Send(ctx, key, value)` 等（链路追踪/取消传递） |
| 链路追踪 | 无 | OpenTelemetry，生产注入/消费提取，默认关闭零开销 |





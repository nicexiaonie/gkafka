# gkafka

基于 [IBM/sarama](https://github.com/IBM/sarama) 封装的 Kafka 客户端库。把消费组协调、分区分配、位点提交、失败重试、认证加密等复杂度挡在封装层内部，使用者只需**注册实例、写业务函数**。

**特性一览**

- **生产**：同步 / 异步发送、5 种分区策略、确认级别、幂等写入、攒批调优、二进制消息与 Headers。
- **消费**：消费组自动分摊分区、至少一次语义、失败重试 / 跳过 / 死信队列、可配置消费起点、优雅关闭。
- **安全**：SASL（PLAIN / SCRAM / OAUTHBEARER / GSSAPI）与 TLS。
- **可观测**：OpenTelemetry 链路追踪（默认关闭、零开销），日志可注入。
- **不静默吞错**：配置非法在注册时报错；发送、重试、丢弃等影响数据正确性的结果都明确暴露给使用者。

## 目录

- [安装](#安装)
- [快速开始](#快速开始)
- [实例管理](#实例管理)
- [生产者](#生产者)
- [消费者](#消费者)
- [认证与加密](#认证与加密)
- [链路追踪](#链路追踪)
- [日志注入](#日志注入)
- [与旧版本的差异](#与旧版本segmentiokafka-go的差异)

## 安装

```bash
go get github.com/nicexiaonie/gkafka
```

- 要求 Go 1.25+（sarama v1.50.3 的要求）。
- 依赖 `github.com/IBM/sarama` 与 OpenTelemetry API（`go.opentelemetry.io/otel`）。OTel 只是 API 依赖，不引入任何追踪后端；不开启追踪时零运行时开销。
- 使用 SCRAM 认证时，SCRAM 客户端实现由调用方自行引入（见 [SASL](#sasl-认证)）。

## 快速开始

可运行的完整示例见 [`examples/producer`](examples/producer/main.go) 与 [`examples/consumer`](examples/consumer/main.go)。

### 生产者

```go
err := gkafka.RegisterProduction("demo", &gkafka.ProductionConf{
    Brokers: []string{"127.0.0.1:9092"},
    Topic:   "my_topic", // 默认 topic
})
if err != nil {
    log.Fatal(err)
}

p := gkafka.GetProduction("demo")
ctx := context.Background()

// 同步：等 broker 确认后返回
if err := p.Send(ctx, "key", "value"); err != nil {
    log.Println("send failed:", err)
}

// 异步：交给 sarama 即返回，broker 侧失败走 AsyncErrHandler
if err := p.SendAsync(ctx, "", "key", "value"); err != nil {
    log.Println("async send not accepted:", err)
}
```

### 消费者

```go
err := gkafka.RegisterConsumer("demo", &gkafka.ConsumerConf{
    Brokers: []string{"127.0.0.1:9092"},
    Topics:  []string{"my_topic"},
    GroupId: "my_group",
    CallFunc: func(ctx context.Context, m gkafka.Message) error {
        log.Printf("partition=%d offset=%d value=%s", m.Partition, m.Offset, m.Value)
        return nil // 返回 nil 提交位点；返回 error 触发重试
    },
})
if err != nil {
    log.Fatal(err)
}

// RunCommit 阻塞运行，直到 Close 后返回
_ = gkafka.GetConsumer("demo").RunCommit()
```

### 优雅退出（推荐写法）

`RunCommit` 是阻塞的，应放到单独协程运行；收到退出信号后**先关消费者、再关生产者**：

```go
func main() {
    ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
    defer stop()

    // ... RegisterProduction / RegisterConsumer ...

    runErr := make(chan error, 1)
    go func() { runErr <- gkafka.GetConsumer("demo").RunCommit() }()

    select {
    case <-ctx.Done(): // 收到退出信号
    case err := <-runErr: // RunCommit 启动失败（如未连接、重复启动）
        log.Println("RunCommit:", err)
    }

    // 消费者：等处理中的消息结束、提交最后一批位点后再断开
    if err := gkafka.CloseConsumer(); err != nil {
        log.Println("close consumer:", err)
    }
    // 生产者：刷出缓冲中的消息，处理完关闭期间的异步发送结果
    if err := gkafka.CloseProduction(); err != nil {
        log.Println("close production:", err)
    }
}
```

> 先关消费者的原因：消费处理中可能还在调用生产者发消息，先关生产者会让这些发送失败。

## 实例管理

生产者、消费者都按**名字**注册到全局注册表，之后在任意位置按名取用。注册表并发安全。

| 函数 | 说明 |
|---|---|
| `RegisterProduction(name, conf)` / `RegisterConsumer(name, conf)` | 校验配置并建立连接；配置非法或连接失败返回 error |
| `GetProduction(name)` / `GetConsumer(name)` | 按名取实例，未注册返回 nil |
| `CloseProductionByName(name)` / `CloseConsumerByName(name)` | 关闭并注销单个实例；未注册返回 nil |
| `CloseProduction()` / `CloseConsumer()` | 关闭并注销全部实例 |

- **同名重复注册会返回错误**，避免旧连接和后台协程被悄悄泄漏。要替换实例，先 `CloseXxxByName(name)` 再重新注册。
- **枚举类配置**（`Balancer`、`Acks`、`InitialOffset`、`OnExceed`、SASL `Mechanism`）推荐使用常量，也可写等价字符串；写错的值在注册时报错，不会静默回退到默认值。

### 消息类型 `Message`

收发共用的中性消息类型，不暴露 sarama 类型：

```go
type Message struct {
    Topic     string
    Partition int
    Offset    int64
    Key       []byte
    Value     []byte
    Headers   []Header
}

type Header struct {
    Key   string
    Value []byte
}
```

- 发送时 `Topic` 为空则使用默认 topic；`Partition` 仅在 `Balancer=BalancerManual` 时生效；`Offset` 忽略。
- 消费时各字段均由 broker 填充。

## 生产者

### 配置 `ProductionConf`

| 字段 | 类型 | 说明 | 默认 |
|---|---|---|---|
| `Brokers` | []string | broker 地址列表，生产环境建议至少两个 | 必填 |
| `Topic` | string | 默认 topic | - |
| `Balancer` | Balancer | 分区策略，见[分区策略](#分区策略) | `BalancerRandom` |
| `Acks` | Acks | 确认级别，见[确认级别与幂等写入](#确认级别与幂等写入) | `AcksLocal` |
| `Idempotent` | bool | 幂等写入 | `false` |
| `BatchSize` / `BatchBytes` / `BatchTimeout` | int / int / Duration | 攒批参数，见[攒批](#攒批) | 不攒批 |
| `WriteTimeout` | Duration | 单次网络写超时 | 30s |
| `AsyncErrHandler` | func(Message, error) | 异步发送的 broker 侧失败回调，见[异步发送](#异步发送) | 写日志 |
| `SASL` / `TLS` | SASLConf / TLSConf | 认证与加密，见[认证与加密](#认证与加密) | 关闭 |
| `Tracing` | TracingConf | 链路追踪，见[链路追踪](#链路追踪) | 关闭 |
| `Logger` | Logger | 日志输出，见[日志注入](#日志注入) | 标准库 log |
| `Address` / `NetWork` | string | 已废弃，仅为兼容旧配置保留；`Brokers` 为空时使用 `Address` | - |

### 发送方法

所有发送方法首参都是 `context.Context`，用于链路追踪与取消。不需要时传 `context.Background()`。

| 方法 | 模式 | 说明 |
|---|---|---|
| `Send(ctx, key, value)` | 同步 | 发默认 topic |
| `SendTopic(ctx, topic, key, value)` | 同步 | 发指定 topic |
| `SendPartition(ctx, part, key, value)` | 同步 | 发默认 topic 的指定分区，**要求 `Balancer=BalancerManual`**，否则返回错误 |
| `SendMessage(ctx, m)` | 同步 | 发完整 `Message`：二进制 Key/Value、Headers |
| `SendAsync(ctx, topic, key, value)` | 异步 | `topic` 为空发默认 topic |
| `SendMessageAsync(ctx, m)` | 异步 | 发完整 `Message` |
| `Close()` | - | 关闭，见[关闭生产者](#关闭生产者) |

> 简单字符串用 `Send` 系列；需要 Headers（租户 ID、业务元数据等）或 Value 是二进制（protobuf、压缩、加密）时用 `SendMessage` / `SendMessageAsync`。

### 同步发送

等 sarama 接收、再等 broker 返回这条消息的结果（毫秒级）后返回。失败直接返回 error，最可靠也最直观。

### 异步发送

只等 sarama **接收**消息（平时微秒级）就返回，不等 broker 结果；sarama 在后台攒批发送，吞吐远高于同步。

**结果通知**

- 成功：不通知。
- broker 侧失败：调用 `AsyncErrHandler`；未设置时写日志。回调里的 `Message` 含 Topic、Partition、Offset、Key、Value，不含 Headers。

**等待与超时**

- sarama 内部积压时（broker 慢或不可达、`AsyncErrHandler` 处理过慢），异步发送会**等待** sarama 接收（反压），而不是丢弃消息。
- `ctx` 取消或超时则放弃等待，**通过返回值**返回错误（可用 `errors.Is(err, context.DeadlineExceeded)` / `context.Canceled` 判断）。此时消息没有交给 sarama，也**不会**走 `AsyncErrHandler`，需调用方自行处理。
- 对延迟敏感的场景请给 ctx 设置超时：

```go
ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
defer cancel()
if err := p.SendAsync(ctx, "", "key", "value"); err != nil {
    // 100ms 内 sarama 未接收：记录、降级或改走同步发送
}
```

**`AsyncErrHandler` 不要阻塞**

回调在 gkafka 内部的**单个后台协程**中逐条执行，执行完一条才读取下一条发送结果。回调耗时过长会导致：

- 平时：发送结果积压，sarama 无法接收新消息，后续异步发送被拖慢、等待（设了 ctx 超时则超时报错）。
- 关闭时：`Close()` 要等关闭期间的回调全部执行完；回调卡死则 `Close()` 不返回。

回调里只做快速操作（打日志、计数、放入业务自己的队列），耗时处理（写库、调接口、重发）交给业务自己的协程。

### 分区策略

| 常量 | 等价字符串 | 行为 | 适用 |
|---|---|---|---|
| `BalancerRandom`（默认） | `"random"` | 随机 | 无顺序要求 |
| `BalancerRoundRobin` | `"roundrobin"` | 轮询，忽略 key | 均匀分布 |
| `BalancerHash` | `"hash"` | 按 key 哈希 | 相同 key 落同一分区，保证同 key 有序 |
| `BalancerReferenceHash` | `"referencehash"` | 参考哈希 | 与部分其它语言客户端的分区结果对齐 |
| `BalancerManual` | `"manual"` | 手动指定 | 配合 `SendPartition` / `Message.Partition` |

### 确认级别与幂等写入

| 常量 | 等价字符串 | 含义 | 取舍 |
|---|---|---|---|
| `AcksLocal`（默认） | `"local"` | leader 写入即确认 | 吞吐与可靠的折中 |
| `AcksAll` | `"all"` | 所有同步副本（ISR）写入才确认 | 最可靠 |
| `AcksNone` | `"none"` | 不等确认 | 最快，broker 异常时会丢 |

`Idempotent: true` 开启幂等生产者，避免 sarama 内部重试造成的重复写入。开启后自动配齐前置条件（`acks=all`、单连接在途请求 1、重试次数 ≥1）；若同时显式设置了非 `AcksAll` 的 `Acks`，注册时报错。

```go
gkafka.RegisterProduction("orders", &gkafka.ProductionConf{
    Brokers:    []string{"127.0.0.1:9092"},
    Topic:      "orders",
    Balancer:   gkafka.BalancerHash, // 同一订单号有序
    Idempotent: true,                // 隐含 AcksAll
})
```

> 幂等只保证"同一个生产者实例的重试不产生重复"，业务层重复调用发送仍会写入多条。

### 攒批

默认不攒批：有消息就尽快发送，延迟最低。高吞吐场景可攒批以减少请求数：

| 字段 | 触发条件 |
|---|---|
| `BatchSize` | 攒够多少条 |
| `BatchBytes` | 攒够多少字节 |
| `BatchTimeout` | 距上次发送超过该时长 |

> ⚠️ **设置 `BatchSize` 或 `BatchBytes` 时必须同时设置 `BatchTimeout`**。否则只有攒够数量才发送，低流量时消息会一直停在缓冲里：同步 `Send` 会一直卡住，异步消息迟迟不到 broker。

```go
BatchSize:    500,
BatchTimeout: 50 * time.Millisecond, // 不满 500 条时最多等 50ms
```

### 关闭生产者

`Close()` 刷出缓冲中的消息，处理完关闭期间回来的异步发送结果后，再断开连接：

- 关闭期间的异步发送失败仍走 `AsyncErrHandler`，不计入 `Close()` 的返回值。
- 可重复调用，仅首次生效；关闭后再发送返回错误。

## 消费者

### 配置 `ConsumerConf`

| 字段 | 类型 | 说明 | 默认 |
|---|---|---|---|
| `Brokers` | []string | broker 地址列表 | 必填 |
| `Topics` | []string | 消费的 topic，至少一个，可多个 | 必填 |
| `GroupId` | string | 消费组 ID | 必填 |
| `CallFunc` | func(ctx, Message) error | 业务处理函数，返回 nil 提交、返回 error 重试 | 必填 |
| `MaxRetry` / `OnExceed` / `DLQTopic` | int / OnExceed / string | 失败处理策略，见[失败处理](#失败处理重试跳过死信) | 无限重试 |
| `InitialOffset` | InitialOffset | 新消费组的起点，见[消费起点](#消费起点) | `OffsetOldest` |
| `Ctx` | context.Context | 业务上下文，其中的值会传给 `CallFunc` 的 ctx | `Background` |
| `MinBytes` / `MaxBytes` / `MaxWait` | int / int / Duration | 拉取调优，见[拉取调优](#拉取调优) | sarama 默认 |
| `SASL` / `TLS` | SASLConf / TLSConf | 认证与加密 | 关闭 |
| `Tracing` | TracingConf | 链路追踪 | 关闭 |
| `Logger` | Logger | 日志输出 | 标准库 log |
| `Address` / `NetWork` | string | 已废弃，仅为兼容旧配置保留 | - |

### 消费方法

| 方法 | 说明 |
|---|---|
| `RunCommit()` | 阻塞运行消费循环，`Close()` 后返回 nil。同一消费者同时只能运行一个；`Close()` 之后调用返回错误 |
| `Close()` | 优雅关闭，见[关闭消费者](#关闭消费者) |

### 消费模型

- 同一 `GroupId` 的多个实例自动分摊分区，实例增减时自动重新分配（rebalance）；不同 `GroupId` 各自独立消费全量消息。
- **每个分区一个处理协程，分区内逐条串行处理**：一条消息处理完（含全部重试）才处理下一条。因此：
  - 消费并发度上限 = 分配到本实例的分区数，要提升吞吐需增加分区或实例。
  - 一条持续失败的消息会阻塞所在分区，见[失败处理](#失败处理重试跳过死信)。

### 投递语义：至少一次

`CallFunc` 返回 nil 时，该条被标记为可提交；sarama 在后台**每秒**把标记的位点批量提交到 broker。以下情况会重复消费：

- 处理成功后、位点提交前进程崩溃：重启后重放最多约 1 秒已处理的消息。
- rebalance 时正在处理、尚未完成的消息：会被新接手的实例重新消费。

**`CallFunc` 必须幂等**（同一条消息重复处理不出错），这是至少一次语义的硬性要求。

> 没有改为逐条同步提交：那样每条消息多一次同步网络往返，吞吐会大幅下降；而上述重复窗口在至少一次语义之内，业务幂等即可覆盖。

### 失败处理：重试、跳过、死信

`CallFunc` 返回 error 时**原地重试该条**（不前进、不丢），按 `MaxRetry` + `OnExceed` 决定何时停止。「不丢」「不阻塞后续」不可兼得时，用死信队列兼顾：

| 策略 | 配置 | 丢消息 | 阻塞分区 |
|---|---|---|---|
| 无限重试（默认） | `MaxRetry: 0` | 否 | **是**，持续失败的消息卡住分区 |
| 超限跳过 | `MaxRetry: N, OnExceed: OnExceedSkip` | **是**，告警后提交 | 否 |
| 超限死信 | `MaxRetry: N, OnExceed: OnExceedDLQ, DLQTopic: "..."` | 否 | 否 |

- `MaxRetry` 是 `CallFunc` 的总尝试次数。`MaxRetry` 与 `OnExceed` 必须配对：`MaxRetry=0` 时 `OnExceed` 须为空，`MaxRetry>0` 时须为 `OnExceedSkip` 或 `OnExceedDLQ`，否则注册时报错。
- **重试间隔**：前 3 次立即重试，第 4 次起每次间隔 1s，第 100 次起每次间隔 5s。等待期间收到 `Close()` 或 rebalance 信号会立即停止，该条不提交，之后由新接手的实例或重启后重新消费。

**死信队列**

```go
gkafka.RegisterConsumer("orders", &gkafka.ConsumerConf{
    Brokers:  []string{"127.0.0.1:9092"},
    Topics:   []string{"orders"},
    GroupId:  "order_service",
    MaxRetry: 5,
    OnExceed: gkafka.OnExceedDLQ,
    DLQTopic: "orders_dlq",
    CallFunc: handleOrder,
})
```

- 超限后把消息投递到 `DLQTopic` 再提交位点，主流程继续。死信消息保留原始 Key/Value/Headers，并追加诊断 Header：`gkafka-dlq-source-topic`、`gkafka-dlq-source-partition`、`gkafka-dlq-source-offset`、`gkafka-dlq-attempt`、`gkafka-dlq-error`。
- 死信投递本身失败时不提交，继续重试（含再次调用 `CallFunc`），不丢消息。
- 死信 producer 在**注册时创建**，连接、认证、权限问题在启动时即暴露；代价是即使从未用到死信，也会多占一个到 broker 的连接。
- 死信 topic 需自行创建，并有程序或人工处理，否则消息只是换个地方堆积。

### 消费起点

新消费组首次连接、**没有任何已提交位点**时，从哪里开始消费：

| 常量 | 等价字符串 | 行为 |
|---|---|---|
| `OffsetOldest`（默认） | `"oldest"` | 从最早的消息开始，**会回放全部历史** |
| `OffsetNewest` | `"newest"` | 从加入时刻之后的新消息开始，跳过历史 |

> ⚠️ 仅对没有已提交位点的消费组生效。消费组提交过位点后，总是从上次提交处继续，本字段不再起作用。

### 业务上下文与取消信号

`CallFunc` 的 ctx：

- **取值**来自 `ConsumerConf.Ctx`（可携带 traceID 等请求级值）。
- **取消**发生在 `Close()` 或 rebalance 时。长耗时处理应监听 `ctx.Done()` 及时返回，避免分区被收走后空跑，也避免拖住 `Close()`。

```go
CallFunc: func(ctx context.Context, m gkafka.Message) error {
    select {
    case <-ctx.Done():
        return ctx.Err() // 收到关闭 / rebalance 信号，提前返回
    default:
    }
    return doWork(ctx, m)
},
```

### 拉取调优

| 字段 | 说明 | 默认 |
|---|---|---|
| `MinBytes` | 单次拉取期望的最小字节数，broker 攒够或到 `MaxWait` 才返回；调大可提升吞吐、增加延迟 | 1 |
| `MaxWait` | 拉取时等待新数据的最长时间 | 500ms |
| `MaxBytes` | 单分区单次拉取的最大字节数；**须不小于最大单条消息**，否则该消息无法消费 | 不限 |

### 关闭消费者

`Close()` 先发出取消信号，等 `RunCommit` 完全退出（所有分区的 `CallFunc` 返回、最后一批位点提交）后，再离开消费组、关闭连接与死信 producer：

- 可重复、并发调用，仅首次生效。
- `CallFunc` 的长耗时处理应监听 `ctx.Done()`，否则 `Close()` 会一直等待。
- **不要在 `CallFunc` 内同步调用 `Close()`**：两者互相等待会卡死。需要在处理中触发关闭时，另起协程调用。

## 认证与加密

生产者、消费者的 `SASL`、`TLS` 字段通用。生产环境连接公网 broker 时应同时启用 SASL 与 TLS（PLAIN 下凭据为明文传输）。

### SASL 认证

| 常量（`Mechanism`） | 等价字符串 | 需填字段 |
|---|---|---|
| `SASLPlain` | `"plain"` | `User`、`Password` |
| `SASLScramSHA256` | `"scram-sha-256"` | `User`、`Password`、`SCRAMClientGenerator` |
| `SASLScramSHA512` | `"scram-sha-512"` | `User`、`Password`、`SCRAMClientGenerator` |
| `SASLOAuth` | `"oauthbearer"` | `TokenProvider`（`sarama.AccessTokenProvider`，令牌的获取与刷新由调用方实现） |
| `SASLGSSAPI` | `"gssapi"` | `GSSAPI`（Kerberos 子配置） |

- `Enable: false`（默认）为不认证；`Enable: true` 且 `Mechanism` 留空时按 PLAIN 处理；字符串大小写不敏感。
- `AuthzID`：授权身份，一般留空。
- `GSSAPI` 子配置字段：`AuthType`、`KeyTabPath`、`CCachePath`、`KerberosConfigPath`、`ServiceName`、`Username`、`Password`、`Realm`、`DisablePAFXFAST`，语义同 `sarama.GSSAPIConfig`。

```go
SASL: gkafka.SASLConf{
    Enable:    true,
    Mechanism: gkafka.SASLPlain,
    User:      "admin",
    Password:  "secret",
},
```

**SCRAM**：sarama 未内置 SCRAM 客户端算法，本库也不引入第三方密码学依赖，需通过 `SCRAMClientGenerator` 提供 `sarama.SCRAMClient` 实现（社区常用 `github.com/xdg-go/scram`）：

```go
import (
    "github.com/IBM/sarama"
    "github.com/xdg-go/scram"
)

type scramClient struct {
    *scram.Client
    *scram.ClientConversation
    hashGen scram.HashGeneratorFcn
}

func (s *scramClient) Begin(user, pass, authz string) error {
    c, err := s.hashGen.NewClient(user, pass, authz)
    if err != nil {
        return err
    }
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

### TLS 加密

```go
TLS: gkafka.TLSConf{
    Enable: true,
    // Config 为 nil 时使用 Go 默认配置（按系统根证书校验服务端）；
    // 自定义 CA 或双向认证时传入 *tls.Config。
    Config: &tls.Config{
        RootCAs:      caCertPool,
        Certificates: []tls.Certificate{clientCert},
    },
},
```

## 链路追踪

基于 [OpenTelemetry](https://opentelemetry.io/)。生产侧把链路上下文（W3C `traceparent` / `tracestate`）注入消息 Header，消费侧取出并续上，使跨 Kafka 的调用在追踪后端串成一条完整链路。

- **默认关闭**：`TracerProvider` 为 nil 时不创建 span、不读写 Header，无任何开销。
- **不绑后端**：只依赖 OTel API，Jaeger、Zipkin、SkyWalking 等经 OTel 接入均可。
- **业务无感**：Header 的注入与提取由框架完成。

### 配置 `TracingConf`

生产者、消费者的 `Tracing` 字段通用。

| 字段 | 说明 | 默认 |
|---|---|---|
| `TracerProvider` | 提供 Tracer，nil 表示关闭；通常传 `otel.GetTracerProvider()` | nil |
| `Propagator` | Header 的注入 / 提取策略 | W3C TraceContext |

### 生产侧

发送方法的 ctx 携带上游 span 即可，框架为每次发送创建 producer span 并注入 Header：

```go
gkafka.RegisterProduction("demo", &gkafka.ProductionConf{
    Brokers: []string{"127.0.0.1:9092"},
    Topic:   "my_topic",
    Tracing: gkafka.TracingConf{TracerProvider: otel.GetTracerProvider()},
})

// ctx 来自 HTTP 入站请求或上一段业务 span
gkafka.GetProduction("demo").SendMessage(ctx, gkafka.Message{Value: payload})
```

- 同步发送的 span 在拿到 broker 结果后结束。
- 异步发送的 span 在后台收到 broker 的成功 / 失败结果时结束，以反映真实发送耗时。

### 消费侧

开启后无需改业务代码：框架从 Header 提取父链路、创建 consumer span，并通过 `CallFunc` 的 ctx 下传，业务在该 ctx 上创建的子 span（DB、RPC 等）自动归入同一链路。

```go
gkafka.RegisterConsumer("demo", &gkafka.ConsumerConf{
    Brokers:  []string{"127.0.0.1:9092"},
    Topics:   []string{"my_topic"},
    GroupId:  "my_group",
    Tracing:  gkafka.TracingConf{TracerProvider: otel.GetTracerProvider()},
    CallFunc: func(ctx context.Context, m gkafka.Message) error {
        return handle(ctx, m) // ctx 已挂在 consumer span 下
    },
})
```

- consumer span 覆盖一条消息的整个处理过程（含重试），在处理结束（成功、跳过、转入死信，或因关闭 / rebalance 中断）时结束。
- 转入死信的消息保留原始 Header，死信消费者读到的是**原始生产者**的链路，便于回溯来源。

## 日志注入

`Logger` 字段接受任何实现 `Printf(format string, v ...interface{})` 的对象，标准库 `*log.Logger` 天然满足。不设置时使用标准库 `log`；传入自定义实现可接入统一日志体系，传入空实现可静默。

## 与旧版本（segmentio/kafka-go）的差异

| 项 | 旧 | 新 |
|---|---|---|
| 底层库 | segmentio/kafka-go | IBM/sarama |
| 分区策略 | `kafka.LeastBytes{}` 等类型 | 字符串枚举（无 `leastbytes`，可用 `roundrobin` 替代） |
| 消息类型 | `kafka.Message` | `gkafka.Message`（中性） |
| 业务函数 | `func(Message) error` | `func(ctx, Message) error`（带上下文与取消信号） |
| 消费 topic | 单 topic | `Topics []string`，支持多 topic |
| 批量拉取 | `FetchMessage` | 已移除（提交时机脱离消费循环、不可控） |
| SASL | 字段存在但未实现 | 5 种机制全部支持 |
| TLS | 无 | `TLSConf` |
| 确认级别 | 固定 `RequireOne` | `Acks` 可配 |
| 幂等写入 | 无 | `Idempotent` |
| 失败处理 | 固定无限重试 | `MaxRetry` + `OnExceed`（跳过 / 死信） |
| 日志 | 固定标准 log | `Logger` 可注入 |
| 发送方法签名 | `Send(key, value)` 等 | 首参加 `ctx`：`Send(ctx, key, value)` 等 |
| 链路追踪 | 无 | OpenTelemetry，默认关闭 |

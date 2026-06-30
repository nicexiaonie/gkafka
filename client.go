package gkafka

import (
	"context"
	"crypto/tls"
	"errors"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

// Logger 是日志注入接口。Go 标准库的 *log.Logger 天然满足它，
// 使用者可传入自己的 logger 把 gkafka 的日志接进统一日志体系，或传 no-op 实现以静默。
type Logger interface {
	Printf(format string, v ...interface{})
}

// defaultLogger 是未注入 logger 时的默认实现，行为同旧版（打到标准 log）。
type defaultLogger struct{}

func (defaultLogger) Printf(format string, v ...interface{}) { log.Printf(format, v...) }

// resolveLogger 返回可用的 logger：未设置时回退到默认实现。
func resolveLogger(l Logger) Logger {
	if l == nil {
		return defaultLogger{}
	}
	return l
}

// Message 是对外暴露的中性消息类型，调用方不直接接触 sarama 的类型。
// Headers 字段为后续接入 trace（traceparent 等）预留载体，第一版不解析。
type Message struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Headers   []Header
}

// Header 是中性的消息头键值对。
type Header struct {
	Key   string
	Value []byte
}

// Balancer 是分区策略的具名类型，配合下面 Balancer* 常量使用。
// 留空（零值）表示走默认（Random）。配置非法值在 Connect 时报错。
type Balancer string

const (
	BalancerRandom        Balancer = "random"        // 随机分区（默认）
	BalancerHash          Balancer = "hash"          // 按 key 哈希取模（相同 key 同分区，保证分区内有序）
	BalancerReferenceHash Balancer = "referencehash" // 参考哈希（与部分其它语言客户端对齐）
	BalancerRoundRobin    Balancer = "roundrobin"    // 轮询（均匀分布，忽略 key）
	BalancerManual        Balancer = "manual"        // 手动指定分区（必须配合 SendPartition）
)

// Acks 是发送确认级别的具名类型，配合下面 Acks* 常量使用。
// 留空（零值）表示走默认（Local）。配置非法值在 Connect 时报错。
type Acks string

const (
	AcksLocal Acks = "local" // 等 leader 写入即确认（默认，吞吐与可靠折中）
	AcksAll   Acks = "all"   // 等所有同步副本(ISR)写入才确认（最可靠，不丢）
	AcksNone  Acks = "none"  // 不等确认（最快，但 broker 异常时会丢）
)

// SASLMechanism 是 SASL 认证机制的具名类型，配合下面 SASL* 常量使用。
// 留空且 SASLConf.Enable=true 时按 SASLPlain 处理。大小写不敏感。
type SASLMechanism string

const (
	SASLPlain       SASLMechanism = "plain"
	SASLScramSHA256 SASLMechanism = "scram-sha-256"
	SASLScramSHA512 SASLMechanism = "scram-sha-512"
	SASLOAuth       SASLMechanism = "oauthbearer"
	SASLGSSAPI      SASLMechanism = "gssapi"
)

// OnExceed 是消费端超过 MaxRetry 后的处理动作类型，配合下面 OnExceed* 常量使用。
// 留空（零值）仅当 MaxRetry=0 时合法（表示无限重试）。
// OnExceed 是超过 MaxRetry 后的处理动作类型，推荐用 OnExceedSkip / OnExceedDLQ 常量。
// 留空（零值）仅当 MaxRetry=0 时合法（表示无限重试）。
type OnExceed string

const (
	OnExceedSkip OnExceed = "skip" // 跳过这条并告警后提交 offset（会丢这条，但不卡）
	OnExceedDLQ  OnExceed = "dlq"  // 转投 DLQTopic 死信队列后提交 offset（不丢、不卡）
)

// InitialOffset 是新消费组首次消费时的起点类型，推荐用 OffsetOldest / OffsetNewest 常量。
// 仅当消费组没有任何已提交位点时生效；已有提交记录时从上次提交处继续。
type InitialOffset string

const (
	OffsetOldest InitialOffset = "oldest" // 从 topic 最早消息开始消费（默认）
	OffsetNewest InitialOffset = "newest" // 从消费组加入时刻之后的最新消息开始消费，跳过全部历史
)

// SASLConf 是 SASL 认证配置，覆盖 sarama 官方支持的全部 5 种机制。
//
// 生产环境连接公网 broker 时强烈建议同时开启 TLS（SASL 凭据在 PLAIN 下是明文传输的）；
// 仅在内网可信环境下才适合明文 + 无认证。
//
// 各机制需要填的字段不同（按使用成本从低到高）：
//   - "plain"          : 只需 User + Password
//   - "scram-sha-256"  : 需 User + Password + 提供 SCRAMClientGenerator（SCRAM 客户端由调用方提供，本库不内置）
//   - "scram-sha-512"  : 需 User + Password + 提供 SCRAMClientGenerator
//   - "oauthbearer"    : 需提供 TokenProvider（token 来自你的 OAuth 服务，本库无法代生成）
//   - "gssapi"         : Kerberos 认证，需填 GSSAPI 子配置（keytab/krb5.conf/realm 等）
type SASLConf struct {
	// Enable 是否启用 SASL 认证。false（默认）= 不认证。
	Enable bool

	// Mechanism 认证机制，可选值（大小写不敏感），推荐用 SASLPlain / SASLScramSHA256 / SASLScramSHA512 / SASLOAuth / SASLGSSAPI 常量。
	// 留空且 Enable=true 时默认按 SASLPlain 处理。
	Mechanism SASLMechanism

	// User 认证用户名（authcid）。PLAIN 与 SCRAM 机制必填。
	User string

	// Password 认证密码。PLAIN 与 SCRAM 机制必填。
	Password string

	// AuthzID 授权身份（authzid）。可选；用于 PLAIN/SCRAM 下"以另一身份代理"的场景，
	// 一般留空即可。
	AuthzID string

	// TokenProvider 仅 "oauthbearer" 机制使用：返回访问令牌的回调，由调用方实现，
	// 需自行处理令牌的缓存与刷新。其它机制忽略此字段。
	TokenProvider sarama.AccessTokenProvider

	// SCRAMClientGenerator 仅 "scram-sha-256" / "scram-sha-512" 机制使用：
	// 返回一个 sarama.SCRAMClient 实现，用于完成 SCRAM 握手。本库不内置 SCRAM 实现，
	// 以避免引入第三方密码学依赖；使用 SCRAM 时由调用方提供（可用 github.com/xdg-go/scram
	// 等库实现 Begin/Step/Done 接口）。其它机制忽略此字段。
	SCRAMClientGenerator func() sarama.SCRAMClient

	// GSSAPI 仅 "gssapi"（Kerberos）机制使用的子配置。其它机制忽略此字段。
	GSSAPI GSSAPIConf
}

// GSSAPIConf 是 Kerberos（GSSAPI）认证的子配置，字段语义与 sarama.GSSAPIConfig 对齐。
// 仅当 SASLConf.Mechanism = "gssapi" 时生效。
type GSSAPIConf struct {
	// AuthType 认证方式：1 = 用 keytab 文件；2 = 用用户名/密码；3 = 用已有 ccache 缓存。
	// 对应 sarama 的 KRB5_USER_AUTH / KRB5_KEYTAB_AUTH / KRB5_CCACHE_AUTH 常量值。
	AuthType int
	// KeyTabPath keytab 文件路径（AuthType 为 keytab 方式时使用）。
	KeyTabPath string
	// CCachePath 凭据缓存路径（AuthType 为 ccache 方式时使用）。
	CCachePath string
	// KerberosConfigPath krb5.conf 配置文件路径。
	KerberosConfigPath string
	// ServiceName Kerberos 服务名（通常为 "kafka"）。
	ServiceName string
	// Username Kerberos 主体用户名（AuthType 为用户名/密码方式时使用）。
	Username string
	// Password Kerberos 密码（AuthType 为用户名/密码方式时使用）。
	Password string
	// Realm Kerberos 域。
	Realm string
	// DisablePAFXFAST 是否禁用 PA-FX-FAST（部分 KDC 不支持时设为 true）。
	DisablePAFXFAST bool
}

// TLSConf 是传输层加密配置，生产/消费通用。
// 生产环境使用 SASL 认证时通常应同时启用 TLS，避免凭据明文传输。
type TLSConf struct {
	// Enable 是否启用 TLS。false（默认）= 明文连接。
	Enable bool
	// Config 自定义 TLS 配置（证书、CA、是否校验服务端等）。
	// 为 nil 且 Enable=true 时使用 Go 默认 TLS 配置（按系统根证书校验服务端）。
	Config *tls.Config
}

var (
	kafkaProduction = make(map[string]*Production)
	kafkaConsumer   = make(map[string]*Consumer)
	// mu 保护两个注册表的并发读写，避免多 goroutine 注册时 panic。
	mu sync.RWMutex
)

// ProductionConf 生产者注册配置。
type ProductionConf struct {
	// NetWork 连接网络类型，一般为 "tcp"。留空默认 "tcp"。
	NetWork string
	// Address broker 地址，格式 "host:port"，如 "10.20.0.200:9092"。必填。
	Address string
	// Topic 默认发送的 topic。Send / SendPartition 使用此 topic；
	// SendTopic / SendAsync 可显式指定其它 topic。
	Topic string

	// Balancer 分区策略，推荐用 Balancer* 常量（也可写等价字符串）：
	//   BalancerHash          按 key 哈希取模分区（相同 key 落同一分区，保证分区内有序）
	//   BalancerReferenceHash 参考哈希（与部分其它语言客户端对齐的哈希实现）
	//   BalancerRoundRobin    轮询分区（均匀分布，忽略 key）
	//   BalancerRandom        随机分区（默认；留空即此值）
	//   BalancerManual        手动指定分区（必须配合 SendPartition 显式给分区号）
	// 配置非法值在 Connect 时报错。
	Balancer Balancer

	// SASL 认证配置。不填或 Enable=false 时为明文无认证连接。详见 SASLConf。
	SASL SASLConf

	// TLS 传输层加密配置。不填或 Enable=false 时为明文连接。详见 TLSConf。
	TLS TLSConf

	// Acks 发送确认级别，推荐用 Acks* 常量（也可写等价字符串）：
	//   AcksLocal（默认） 等 leader 写入即确认（吞吐与可靠折中）
	//   AcksAll           等所有同步副本(ISR)写入才确认（最可靠，不丢，吞吐略低）
	//   AcksNone          不等确认（最快，但 broker 异常时会丢，慎用）
	// 配置非法值在 Connect 时报错。
	Acks Acks

	// Idempotent 开启幂等生产者，实现“恰好一次写入”（不丢且不重）。
	// 开启时本库会自动满足其前置条件（acks=all、单连接在途请求、重试），
	// 因此与 Acks=AcksLocal/AcksNone 冲突——若同时显式指定非 AcksAll 的 Acks 将在注册时报错。
	Idempotent bool

	// BatchSize 一批攒够多少条消息就触发发送。0 = 用 sarama 默认。
	// 注意：低流量下若设得过大，消息会等到 BatchTimeout 才发出，增加单条延迟。
	BatchSize int
	// BatchBytes 一批累计到多少字节就触发发送。0 = 用 sarama 默认。
	BatchBytes int
	// BatchTimeout 距上次发送超过该时长则强制把未满的批次发出。0 = 用 sarama 默认（约 1s）。
	BatchTimeout time.Duration
	// WriteTimeout 单次网络写操作的超时时间。0 = 用 sarama 默认。
	WriteTimeout time.Duration

	// AsyncErrHandler 异步发送（SendAsync）的错误回调。
	// 不设置时默认写日志，绝不静默吞掉错误。同步发送（Send 系列）不走此回调，直接返回 error。
	AsyncErrHandler func(msg Message, err error)

	// Logger 日志输出。为 nil 时使用默认实现（标准库 log）。详见 Logger 接口。
	Logger Logger
}

// ConsumerConf 消费者注册配置。
type ConsumerConf struct {
	// NetWork 连接网络类型，一般为 "tcp"。留空默认 "tcp"。
	NetWork string
	// Address broker 地址，格式 "host:port"。必填。
	Address string
	// Topics 要消费的 topic 列表。至少一个。同一消费组可同时消费多个 topic。
	Topics []string
	// GroupId 消费组 ID。必填。
	// 同一 GroupId 的多个实例会自动分摊分区（rebalance）；不同 GroupId 各自独立消费全量。
	GroupId string

	// Ctx 业务上下文，由使用者传入（携带 traceID 等 request 级值）。
	// 框架会以它为父派生出传给业务函数的 ctx，并在 Close/rebalance 时附加取消信号。
	// 为 nil 时使用 context.Background()。注意：它仅用于传值，生命周期由本库内部管理。
	Ctx context.Context

	// MinBytes 单次拉取请求期望的最小字节数。broker 会等到攒够这么多数据或到 MaxWait 才返回。
	// 0 = 用 sarama 默认。调大可减少请求次数、提升吞吐，代价是单次延迟变高。
	MinBytes int
	// MaxBytes 单次拉取单个分区接受的最大字节数（映射 sarama Consumer.Fetch.Max）。
	// 0 = 用 sarama 默认（不限，仅受全局 MaxResponseSize 约束）。须不小于最大单条消息，否则该消息无法消费。
	MaxBytes int
	// MaxWait 拉取时等待新数据的最长时间（配合 MinBytes 使用）。0 = 用 sarama 默认（约 250ms~500ms）。
	MaxWait time.Duration

	// SASL 认证配置。不填或 Enable=false 时为明文无认证连接。详见 SASLConf。
	SASL SASLConf

	// TLS 传输层加密配置。不填或 Enable=false 时为明文连接。详见 TLSConf。
	TLS TLSConf

	// MaxRetry 业务函数返回 error 时的最大重试次数。
	//   0（默认）= 无限重试，永不放弃，不丢消息，但毒消息会卡住分区（与旧实现行为一致）。
	//   >0       = 重试达到该次数后，按 OnExceed 处理；此时 OnExceed 必须是 OnExceedSkip 或 OnExceedDLQ。
	MaxRetry int

	// OnExceed 超过 MaxRetry 后的动作（推荐用 OnExceed* 常量）：
	//   ""（默认）     仅当 MaxRetry=0 时合法，表示无限重试。
	//   OnExceedSkip   跳过这条并告警后提交 offset —— 会丢这条，但不卡后续。
	//   OnExceedDLQ    转投 DLQTopic 死信队列后提交 offset —— 不丢、不卡，但需自行消费死信。
	// 注意：MaxRetry>0 与 OnExceed 必须配对设置，否则注册时报错（避免 MaxRetry 被静默忽略）。
	OnExceed OnExceed

	// DLQTopic 仅当 OnExceed=OnExceedDLQ 时必填，死信消息投递的目标 topic。
	// 死信 topic 需自行创建并有程序/人工后续处理，否则消息只是换个地方堆积。
	DLQTopic string

	// InitialOffset 消费组首次连接（无已提交 offset）时的起点：
	//   OffsetOldest（默认） 从 topic 最早消息开始 —— 会回放历史
	//   OffsetNewest         从消费组加入时刻之后的最新消息开始 —— 跳过全部历史
	// 仅在没有已提交位点时生效；消费组已提交过 offset 后从已提交位点继续，本字段不再起作用。
	// 配置非法值在 Connect 时报错。
	InitialOffset InitialOffset

	// CallFunc 业务处理函数：
	// 返回 nil 视为成功并提交 offset；返回 error 则原地重试该条（不前进、不丢），按 MaxRetry/OnExceed 策略处理。
	// 默认语义为至少一次（at-least-once），业务需保证幂等（同一条重复处理不出错）。
	// ctx 携带 ConsumerConf.Ctx 的值，并在 Close/rebalance 时被取消——长处理可监听 ctx.Done() 提前退出。
	CallFunc func(ctx context.Context, message Message) error

	// Logger 日志输出。为 nil 时使用默认实现（标准库 log）。详见 Logger 接口。
	Logger Logger
}

// RegisterProduction 按 name 注册并初始化一个生产者实例（建立连接）。
// 之后在任意位置用 GetProduction(name) 取出使用。连接失败时返回 error。
//
// 同名重注册会拒绝并返回错误——要替换请先调用 CloseProductionByName(name) 显式关闭旧实例。
// 这样设计是避免静默泄漏旧连接与后台协程。
func RegisterProduction(name string, pc *ProductionConf) error {
	mu.RLock()
	_, exists := kafkaProduction[name]
	mu.RUnlock()
	if exists {
		return errors.New("gkafka: production already registered: " + name + " (call CloseProductionByName first)")
	}
	p := &Production{
		NetWork:         pc.NetWork,
		Address:         pc.Address,
		Topic:           pc.Topic,
		Balancer:        pc.Balancer,
		Acks:            pc.Acks,
		Idempotent:      pc.Idempotent,
		SASL:            pc.SASL,
		TLS:             pc.TLS,
		BatchSize:       pc.BatchSize,
		BatchBytes:      pc.BatchBytes,
		BatchTimeout:    pc.BatchTimeout,
		WriteTimeout:    pc.WriteTimeout,
		AsyncErrHandler: pc.AsyncErrHandler,
		logger:          resolveLogger(pc.Logger),
	}
	if err := p.Connect(); err != nil {
		return err
	}
	mu.Lock()
	if _, exists := kafkaProduction[name]; exists {
		// 并发场景：检查后到此处之间有别的 goroutine 抢先注册了。
		mu.Unlock()
		_ = p.Close()
		return errors.New("gkafka: production already registered: " + name + " (call CloseProductionByName first)")
	}
	kafkaProduction[name] = p
	mu.Unlock()
	p.logger.Printf("gkafka RegisterProduction: name=%s addr=%s topic=%s", name, p.Address, p.Topic)
	return nil
}

// CloseProduction 关闭所有已注册的生产者，返回最后一个出现的错误（若有）。
// 注意：会同时把它们从注册表里清掉。
func CloseProduction() error {
	mu.Lock()
	all := kafkaProduction
	kafkaProduction = make(map[string]*Production)
	mu.Unlock()
	var e error
	for _, v := range all {
		if err := v.Close(); err != nil {
			e = err
		}
	}
	return e
}

// CloseProductionByName 关闭并注销指定 name 的生产者实例。
// 未注册时返回 nil（幂等）。配合 RegisterProduction 的"同名拒绝"行为使用：
// 想用同名替换时先调用本函数清掉旧的，再 Register 新的。
func CloseProductionByName(name string) error {
	mu.Lock()
	p, ok := kafkaProduction[name]
	if ok {
		delete(kafkaProduction, name)
	}
	mu.Unlock()
	if !ok {
		return nil
	}
	return p.Close()
}

// GetProduction 按 name 取出已注册的生产者；未注册时返回 nil。
func GetProduction(name string) *Production {
	mu.RLock()
	defer mu.RUnlock()
	return kafkaProduction[name]
}

// RegisterConsumer 按 name 注册并初始化一个消费者实例（建立消费组连接）。
// 注册成功后用 GetConsumer(name) 取出，再调用 RunCommit 启动自动消费。
// 连接失败或配置非法（如缺 GroupId）时返回 error。
//
// 同名重注册会拒绝并返回错误——要替换请先调用 CloseConsumerByName(name) 显式关闭旧实例。
func RegisterConsumer(name string, cc *ConsumerConf) error {
	mu.RLock()
	_, exists := kafkaConsumer[name]
	mu.RUnlock()
	if exists {
		return errors.New("gkafka: consumer already registered: " + name + " (call CloseConsumerByName first)")
	}
	c := &Consumer{
		NetWork:        cc.NetWork,
		Address:        cc.Address,
		Topics:         cc.Topics,
		GroupId:        cc.GroupId,
		userCtx:        cc.Ctx,
		MinBytes:       cc.MinBytes,
		MaxBytes:       cc.MaxBytes,
		MaxWait:        cc.MaxWait,
		SASL:           cc.SASL,
		TLS:            cc.TLS,
		MaxRetry:       cc.MaxRetry,
		OnExceed:       cc.OnExceed,
		DLQTopic:       cc.DLQTopic,
		InitialOffset:  cc.InitialOffset,
		CallFunc:       cc.CallFunc,
		logger:         resolveLogger(cc.Logger),
	}
	if err := c.Connect(); err != nil {
		return err
	}
	mu.Lock()
	if _, exists := kafkaConsumer[name]; exists {
		// 并发场景：检查后到此处之间有别的 goroutine 抢先注册了。
		mu.Unlock()
		_ = c.Close()
		return errors.New("gkafka: consumer already registered: " + name + " (call CloseConsumerByName first)")
	}
	kafkaConsumer[name] = c
	mu.Unlock()
	return nil
}

// CloseConsumer 关闭所有已注册的消费者，返回最后一个出现的错误（若有）。
// 注意：会同时把它们从注册表里清掉。
func CloseConsumer() error {
	mu.Lock()
	all := kafkaConsumer
	kafkaConsumer = make(map[string]*Consumer)
	mu.Unlock()
	var e error
	for _, v := range all {
		if err := v.Close(); err != nil {
			e = err
		}
	}
	return e
}

// CloseConsumerByName 关闭并注销指定 name 的消费者实例。
// 未注册时返回 nil（幂等）。配合 RegisterConsumer 的"同名拒绝"行为使用：
// 想用同名替换时先调用本函数清掉旧的，再 Register 新的。
func CloseConsumerByName(name string) error {
	mu.Lock()
	c, ok := kafkaConsumer[name]
	if ok {
		delete(kafkaConsumer, name)
	}
	mu.Unlock()
	if !ok {
		return nil
	}
	return c.Close()
}

// GetConsumer 按 name 取出已注册的消费者；未注册时返回 nil。
func GetConsumer(name string) *Consumer {
	mu.RLock()
	defer mu.RUnlock()
	return kafkaConsumer[name]
}

// buildConfig 构造 sarama 基础配置，生产/消费共用认证、加密与版本设置。
func buildConfig(sc SASLConf, tc TLSConf) (*sarama.Config, error) {
	cfg := sarama.NewConfig()

	if tc.Enable {
		cfg.Net.TLS.Enable = true
		cfg.Net.TLS.Config = tc.Config
	}

	if !sc.Enable {
		return cfg, nil
	}

	cfg.Net.SASL.Enable = true
	cfg.Net.SASL.User = sc.User
	cfg.Net.SASL.Password = sc.Password
	cfg.Net.SASL.SCRAMAuthzID = sc.AuthzID

	switch SASLMechanism(strings.ToLower(string(sc.Mechanism))) {
	case SASLPlain, "":
		cfg.Net.SASL.Mechanism = sarama.SASLTypePlaintext
	case SASLScramSHA256:
		if sc.SCRAMClientGenerator == nil {
			return nil, errors.New("gkafka: scram-sha-256 requires SASL.SCRAMClientGenerator")
		}
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA256
		cfg.Net.SASL.SCRAMClientGeneratorFunc = sc.SCRAMClientGenerator
	case SASLScramSHA512:
		if sc.SCRAMClientGenerator == nil {
			return nil, errors.New("gkafka: scram-sha-512 requires SASL.SCRAMClientGenerator")
		}
		cfg.Net.SASL.Mechanism = sarama.SASLTypeSCRAMSHA512
		cfg.Net.SASL.SCRAMClientGeneratorFunc = sc.SCRAMClientGenerator
	case SASLOAuth:
		if sc.TokenProvider == nil {
			return nil, errors.New("gkafka: oauthbearer requires SASL.TokenProvider")
		}
		cfg.Net.SASL.Mechanism = sarama.SASLTypeOAuth
		cfg.Net.SASL.TokenProvider = sc.TokenProvider
	case SASLGSSAPI:
		cfg.Net.SASL.Mechanism = sarama.SASLTypeGSSAPI
		cfg.Net.SASL.GSSAPI = sarama.GSSAPIConfig{
			AuthType:           sc.GSSAPI.AuthType,
			KeyTabPath:         sc.GSSAPI.KeyTabPath,
			CCachePath:         sc.GSSAPI.CCachePath,
			KerberosConfigPath: sc.GSSAPI.KerberosConfigPath,
			ServiceName:        sc.GSSAPI.ServiceName,
			Username:           sc.GSSAPI.Username,
			Password:           sc.GSSAPI.Password,
			Realm:              sc.GSSAPI.Realm,
			DisablePAFXFAST:    sc.GSSAPI.DisablePAFXFAST,
		}
	default:
		return nil, errors.New("gkafka: unsupported SASL mechanism: " + string(sc.Mechanism) +
			" (supported: plain, scram-sha-256, scram-sha-512, oauthbearer, gssapi)")
	}
	return cfg, nil
}

// partitionerFor 把对外的 Balancer 枚举映射到 sarama 的 partitioner 构造器。
// 留空走默认 Random；非法值返回 error（与 SASL 机制校验一致，不静默回退）。
func partitionerFor(b Balancer) (sarama.PartitionerConstructor, error) {
	switch b {
	case BalancerHash:
		return sarama.NewHashPartitioner, nil
	case BalancerReferenceHash:
		return sarama.NewReferenceHashPartitioner, nil
	case BalancerRoundRobin:
		return sarama.NewRoundRobinPartitioner, nil
	case BalancerManual:
		return sarama.NewManualPartitioner, nil
	case BalancerRandom, "":
		return sarama.NewRandomPartitioner, nil
	default:
		return nil, errors.New("gkafka: unsupported Balancer: " + string(b) +
			" (supported: hash, referencehash, roundrobin, random, manual)")
	}
}

// acksFor 把对外的 Acks 枚举映射到 sarama 的确认级别。
// 留空走默认 Local；非法值返回 error（与 SASL 机制校验一致，不静默回退）。
func acksFor(a Acks) (sarama.RequiredAcks, error) {
	switch a {
	case AcksNone:
		return sarama.NoResponse, nil
	case AcksAll:
		return sarama.WaitForAll, nil
	case AcksLocal, "":
		return sarama.WaitForLocal, nil
	default:
		return 0, errors.New("gkafka: unsupported Acks: " + string(a) +
			" (supported: local, all, none)")
	}
}

// initialOffsetFor 把对外的 InitialOffset 枚举映射到 sarama 的消费起点。
// 留空走默认 OffsetOldest；非法值返回 error（与其它枚举校验一致，不静默回退）。
func initialOffsetFor(io InitialOffset) (int64, error) {
	switch io {
	case OffsetNewest:
		return sarama.OffsetNewest, nil
	case OffsetOldest, "":
		return sarama.OffsetOldest, nil
	default:
		return 0, errors.New("gkafka: unsupported InitialOffset: " + string(io) +
			" (supported: oldest, newest)")
	}
}

// fromConsumerMessage 把 sarama 消费消息转成对外的中性 Message。
func fromConsumerMessage(m *sarama.ConsumerMessage) Message {
	var hs []Header
	if len(m.Headers) > 0 {
		hs = make([]Header, 0, len(m.Headers))
		for _, h := range m.Headers {
			hs = append(hs, Header{Key: string(h.Key), Value: h.Value})
		}
	}
	return Message{
		Topic:     m.Topic,
		Partition: int(m.Partition),
		Offset:    m.Offset,
		Key:       m.Key,
		Value:     m.Value,
		Headers:   hs,
	}
}

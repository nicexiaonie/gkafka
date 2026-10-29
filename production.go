package gkafka

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/trace"
)

// Production 管理一个 Kafka 连接（sarama.Client），在其之上提供同步与异步两种发送方式。
// 同步（Send 系列）与异步（SendAsync）共享同一个底层连接与同一份配置，区别仅在发送方式。
type Production struct {
	NetWork string
	Address string
	Brokers []string
	Topic   string

	Balancer   Balancer
	Acks       Acks
	Idempotent bool
	SASL       SASLConf
	TLS        TLSConf

	BatchSize    int
	BatchBytes   int
	BatchTimeout time.Duration
	WriteTimeout time.Duration

	AsyncErrHandler func(msg Message, err error)
	logger          Logger

	Tracing TracingConf
	tr      tracer

	client sarama.Client
	sync   sarama.SyncProducer
	async  sarama.AsyncProducer
	// drained 在 drainAsync 读完异步结果并退出时关闭，Close 据此等待结果处理完毕。
	drained chan struct{}

	mu     sync.RWMutex
	closed bool
}

// Connect 建立底层连接（一个 sarama.Client），并在其之上派生同步与异步 producer。
// 两者共享该连接与配置。注册时自动调用。
func (p *Production) Connect() error {
	p.logger = resolveLogger(p.logger)
	p.tr = resolveTracing(p.Tracing)
	cfg, err := buildConfig(p.SASL, p.TLS)
	if err != nil {
		return err
	}
	// 同步发送语义：等待 broker 确认后返回，失败直接返回 error。
	// SyncProducer 要求 Return.Successes/Errors 为 true；异步共用此配置，由 drainAsync 消费两个 channel。
	cfg.Producer.Return.Successes = true
	cfg.Producer.Return.Errors = true
	acks, err := acksFor(p.Acks)
	if err != nil {
		return err
	}
	cfg.Producer.RequiredAcks = acks
	part, err := partitionerFor(p.Balancer)
	if err != nil {
		return err
	}
	cfg.Producer.Partitioner = part
	if p.Idempotent {
		// 幂等生产者要求 acks=all、单连接在途请求、重试>=1。这里自动配齐；
		// 若使用者显式指定了非 all 的 Acks，属冲突，fail-fast 报错而非静默改写。
		if p.Acks != "" && p.Acks != AcksAll {
			return errors.New("gkafka: Idempotent requires Acks=AcksAll (got \"" + string(p.Acks) + "\")")
		}
		cfg.Producer.Idempotent = true
		cfg.Producer.RequiredAcks = sarama.WaitForAll
		cfg.Net.MaxOpenRequests = 1
		if cfg.Producer.Retry.Max < 1 {
			cfg.Producer.Retry.Max = 3
		}
	}
	if p.BatchSize > 0 {
		cfg.Producer.Flush.Messages = p.BatchSize
	}
	if p.BatchBytes > 0 {
		cfg.Producer.Flush.Bytes = p.BatchBytes
	}
	if p.BatchTimeout > 0 {
		cfg.Producer.Flush.Frequency = p.BatchTimeout
	}
	if p.WriteTimeout > 0 {
		cfg.Net.WriteTimeout = p.WriteTimeout
	}

	brokers, err := resolveBrokers(p.Brokers, p.Address)
	if err != nil {
		return err
	}

	client, err := sarama.NewClient(brokers, cfg)
	if err != nil {
		return err
	}

	sp, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		_ = client.Close()
		return err
	}

	ap, err := sarama.NewAsyncProducerFromClient(client)
	if err != nil {
		_ = sp.Close()
		_ = client.Close()
		return err
	}

	p.client = client
	p.sync = sp
	p.async = ap
	p.drained = make(chan struct{})
	p.closed = false
	go p.drainAsync(ap, p.drained)

	p.logger.Printf("gkafka Connect production: brokers=%v topic=%s", brokers, p.Topic)
	return nil
}

// drainAsync 持续读取异步发送的成功/失败结果。
// 错误必须被消费，否则 sarama 会阻塞；失败走 AsyncErrHandler，未设置则写日志。
// 若消息关联了 producer span（异步发送时存入 Metadata），在此处回收结束——
// 异步发送是"投进 channel 即返回"，span 不能在投递时结束，否则测不到真实发送耗时。
// 只读取启动时传入的 ap，直到其成功/失败两个通道都关闭才退出，退出时关闭 done。
// 它是结果的唯一读者：Close 期间在途消息的结果也由这里处理，不会被 sarama 代读而漏掉回调。
func (p *Production) drainAsync(ap sarama.AsyncProducer, done chan struct{}) {
	defer close(done)
	successes, errs := ap.Successes(), ap.Errors()
	for successes != nil || errs != nil {
		select {
		case s, ok := <-successes:
			if !ok {
				successes = nil
				continue
			}
			if s != nil {
				finishProduce(spanFromMetadata(s.Metadata), s.Partition, s.Offset, nil)
			}
		case e, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			finishProduce(spanFromMetadata(e.Msg.Metadata), e.Msg.Partition, e.Msg.Offset, e.Err)
			msg := fromProducerMessage(e.Msg)
			if p.AsyncErrHandler != nil {
				p.AsyncErrHandler(msg, e.Err)
			} else {
				p.logger.Printf("gkafka async send error: topic=%s key=%s err=%v", msg.Topic, string(msg.Key), e.Err)
			}
		}
	}
}

// Send 向默认 Topic 同步发送一条消息，等待 broker 确认后返回，失败返回 error。
// 分区由注册时的 Balancer 策略决定。ctx 用于链路追踪（启用 Tracing 时）与取消传递。
func (p *Production) Send(ctx context.Context, key, value string) error {
	if len(p.Topic) == 0 {
		return errors.New("gkafka: topic is required")
	}
	return p.sendSync(ctx, p.Topic, false, -1, key, value)
}

// SendPartition 向默认 Topic 的指定分区 part 同步发送一条消息。
// 必须在注册时配置 Balancer="manual"，否则返回错误——非 manual 策略下
// sarama 的分区器会忽略手动指定的分区号，消息会被错路由到别的分区。
func (p *Production) SendPartition(ctx context.Context, part int, key, value string) error {
	if len(p.Topic) == 0 {
		return errors.New("gkafka: topic is required")
	}
	if p.Balancer != BalancerManual {
		return errors.New(`gkafka: SendPartition requires Balancer="manual"`)
	}
	return p.sendSync(ctx, p.Topic, true, int32(part), key, value)
}

// SendTopic 向指定的 topic 同步发送一条消息（不使用默认 Topic）。
func (p *Production) SendTopic(ctx context.Context, topic, key, value string) error {
	if len(topic) == 0 {
		return errors.New("gkafka: topic is required")
	}
	return p.sendSync(ctx, topic, false, -1, key, value)
}

// sendAsyncMessage 异步高吞吐发送：等 sarama 接收消息即返回，不等待 broker 确认。
// sarama 的异步入口通道无缓冲，平时接收只需微秒级；仅当 sarama 内部积压时才会等待（反压）。
// ctx 结束（取消/超时）时放弃等待并返回包装了 ctx.Err() 的错误，消息不会交给 sarama，
// 也不走 AsyncErrHandler；ctx 为 nil 时视为不限时等待。
// 等待期间持有读锁，保证 Close 发起 AsyncClose 之后不会再有消息写入入口通道。
// 成功不另行通知，broker 侧失败通过注册时的 AsyncErrHandler 回调（未设置则写日志）。
// 启用 Tracing 时：起 producer span 并注入 header，把 span 暂存于 msg.Metadata，
// 由 drainAsync 在收到成功/失败结果时回收结束（异步无法在投递时结束 span）。
func (p *Production) sendAsyncMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	if ctx == nil {
		ctx = context.Background()
	}
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errors.New("gkafka: production is closed")
	}
	if p.async == nil {
		return errors.New("gkafka: production is not connected")
	}
	// 先判断 ctx 是否已结束：否则 ctx 已结束且 sarama 恰好空闲时，select 会随机选择分支。
	if err := ctx.Err(); err != nil {
		return fmt.Errorf("gkafka: async send canceled: %w", err)
	}
	if p.tr.enabled {
		msg.Metadata = p.tr.startProduce(ctx, msg)
	}
	select {
	case p.async.Input() <- msg:
		return nil
	case <-ctx.Done():
		err := fmt.Errorf("gkafka: async send canceled: %w", ctx.Err())
		// 未交给 sarama，span 不会被 drainAsync 回收，就地结束避免泄漏。
		finishProduce(spanFromMetadata(msg.Metadata), msg.Partition, msg.Offset, err)
		return err
	}
}

// SendAsync 向指定 topic 异步发送一条消息：等 sarama 接收即返回，不等待 broker 确认。
// sarama 积压时会等待，可通过 ctx 的超时/取消控制最长等待时间。
// topic 为空时回退到注册时的默认 Topic（仍空则返回错误）。
// broker 侧失败通过注册时的 AsyncErrHandler 回调处理；启用 Tracing 时会传递 span。
func (p *Production) SendAsync(ctx context.Context, topic, key, value string) error {
	if len(topic) == 0 {
		topic = p.Topic
	}
	if len(topic) == 0 {
		return errors.New("gkafka: topic is required")
	}
	return p.sendAsyncMessage(ctx, &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	})
}

// sendSync 是 Send/SendPartition/SendTopic 的统一内部实现。
// withPartition=true 时将 partition 写入消息；最终委托给 sendSyncMessage 处理 tracing 与发送。
func (p *Production) sendSync(ctx context.Context, topic string, withPartition bool, partition int32, key, value string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}
	if withPartition {
		msg.Partition = partition
	}
	return p.sendSyncMessage(ctx, msg)
}

// sendSyncMessage 同步发送一条消息并等待确认。启用 Tracing 时围绕本次发送起止 producer span。
func (p *Production) sendSyncMessage(ctx context.Context, msg *sarama.ProducerMessage) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errors.New("gkafka: production is closed")
	}
	if p.sync == nil {
		return errors.New("gkafka: production is not connected")
	}
	var span trace.Span
	if p.tr.enabled {
		span = p.tr.startProduce(ctx, msg)
	}
	partition, offset, err := p.sync.SendMessage(msg)
	finishProduce(span, partition, offset, err)
	if err != nil {
		p.logger.Printf("gkafka send err: %v", err)
		return err
	}
	return nil
}

// toProducerMessage 将对外 Message 转换为 sarama.ProducerMessage。
// 若 m.Topic 为空则回退到 p.Topic（仍空则返回错误）。
// Key/Value 用 ByteEncoder 传，支持二进制；Headers 透传。
// Partition 仅当 Balancer=manual 时生效（由 sarama 分区器行为保证）。
func (p *Production) toProducerMessage(m Message) (*sarama.ProducerMessage, error) {
	topic := m.Topic
	if len(topic) == 0 {
		topic = p.Topic
	}
	if len(topic) == 0 {
		return nil, errors.New("gkafka: topic is required")
	}
	msg := &sarama.ProducerMessage{
		Topic:     topic,
		Key:       sarama.ByteEncoder(m.Key),
		Value:     sarama.ByteEncoder(m.Value),
		Partition: int32(m.Partition),
	}
	if len(m.Headers) > 0 {
		msg.Headers = make([]sarama.RecordHeader, len(m.Headers))
		for i, h := range m.Headers {
			msg.Headers[i].Key = []byte(h.Key)
			msg.Headers[i].Value = h.Value
		}
	}
	return msg, nil
}

// SendMessage 同步发送一条完整的 Message，等待 broker 确认后返回。
// 支持二进制 Key/Value、透传 Headers；m.Topic 为空时回退到注册时的默认 Topic。
// m.Partition 仅当 Balancer=manual 时生效（非 manual 策略下由 sarama 自动分配）。
// ctx 用于链路追踪（启用 Tracing 时）与取消传递。
func (p *Production) SendMessage(ctx context.Context, m Message) error {
	msg, err := p.toProducerMessage(m)
	if err != nil {
		return err
	}
	return p.sendSyncMessage(ctx, msg)
}

// SendMessageAsync 异步高吞吐发送一条完整的 Message：等 sarama 接收即返回，不等待 broker 确认。
// sarama 积压时会等待，可通过 ctx 的超时/取消控制最长等待时间。
// 成功不另行通知，broker 侧失败通过注册时的 AsyncErrHandler 回调（未设置则写日志）。
// 字段语义与 SendMessage 一致：支持二进制、Headers 透传、m.Topic 为空回退默认。
func (p *Production) SendMessageAsync(ctx context.Context, m Message) error {
	msg, err := p.toProducerMessage(m)
	if err != nil {
		return err
	}
	return p.sendAsyncMessage(ctx, msg)
}

// fromProducerMessage 把 sarama.ProducerMessage 转换为对外的 Message。
// 用于异步发送失败回调中还原原始消息与发送结果信息。
func fromProducerMessage(m *sarama.ProducerMessage) Message {
	out := Message{
		Topic:     m.Topic,
		Partition: int(m.Partition),
		Offset:    m.Offset,
	}
	if m.Key != nil {
		if b, err := m.Key.Encode(); err == nil {
			out.Key = b
		}
	}
	if m.Value != nil {
		if b, err := m.Value.Encode(); err == nil {
			out.Value = b
		}
	}
	return out
}

// Close 关闭异步与同步 producer（刷出缓冲消息），最后关闭共享的底层连接。
// 顺序固定：先异步、再同步、最后 client。异步 producer 只发起关闭（AsyncClose），
// 并等待 drainAsync 处理完在途消息的结果——期间的失败仍走 AsyncErrHandler，
// 因此不会出现在本方法的返回值中。返回同步 producer 与 client 关闭时的最后一个错误。
func (p *Production) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	syncProducer := p.sync
	asyncProducer := p.async
	drained := p.drained
	client := p.client
	p.sync = nil
	p.async = nil
	p.drained = nil
	p.client = nil
	p.mu.Unlock()

	if asyncProducer != nil {
		asyncProducer.AsyncClose()
		<-drained
	}
	var e error
	if syncProducer != nil {
		if err := syncProducer.Close(); err != nil {
			e = err
		}
	}
	if client != nil {
		if err := client.Close(); err != nil {
			e = err
		}
	}
	return e
}

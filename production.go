package gkafka

import (
	"errors"
	"sync"
	"time"

	"github.com/IBM/sarama"
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

	client sarama.Client
	sync   sarama.SyncProducer
	async  sarama.AsyncProducer

	mu     sync.RWMutex
	closed bool
}

// Connect 建立底层连接（一个 sarama.Client），并在其之上派生同步与异步 producer。
// 两者共享该连接与配置。注册时自动调用。
func (p *Production) Connect() error {
	p.logger = resolveLogger(p.logger)
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
	p.closed = false
	go p.drainAsync()

	p.logger.Printf("gkafka Connect production: addr=%s topic=%s", p.Address, p.Topic)
	return nil
}

// drainAsync 持续读取异步发送的成功/失败结果。
// 错误必须被消费，否则 sarama 会阻塞；失败走 AsyncErrHandler，未设置则写日志。
func (p *Production) drainAsync() {
	for {
		select {
		case _, ok := <-p.async.Successes():
			if !ok {
				return
			}
		case e, ok := <-p.async.Errors():
			if !ok {
				return
			}
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
// 分区由注册时的 Balancer 策略决定。
func (p *Production) Send(key, value string) error {
	if len(p.Topic) == 0 {
		return errors.New("gkafka: topic is required")
	}
	return p.sendSync(p.Topic, false, -1, key, value)
}

// SendPartition 向默认 Topic 的指定分区 part 同步发送一条消息。
// 必须在注册时配置 Balancer="manual"，否则返回错误——非 manual 策略下
// sarama 的分区器会忽略手动指定的分区号，消息会被错路由到别的分区。
func (p *Production) SendPartition(part int, key, value string) error {
	if len(p.Topic) == 0 {
		return errors.New("gkafka: topic is required")
	}
	if p.Balancer != BalancerManual {
		return errors.New(`gkafka: SendPartition requires Balancer="manual"`)
	}
	return p.sendSync(p.Topic, true, int32(part), key, value)
}

// SendTopic 向指定的 topic 同步发送一条消息（不使用默认 Topic）。
func (p *Production) SendTopic(topic, key, value string) error {
	if len(topic) == 0 {
		return errors.New("gkafka: topic is required")
	}
	return p.sendSync(topic, false, -1, key, value)
}

// SendAsync 异步高吞吐发送，立即返回不等待。
// 成功不另行通知，失败通过注册时的 AsyncErrHandler 回调（未设置则写日志）。
func (p *Production) sendAsyncMessage(msg *sarama.ProducerMessage) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errors.New("gkafka: production is closed")
	}
	if p.async == nil {
		return errors.New("gkafka: production is not connected")
	}
	select {
	case p.async.Input() <- msg:
		return nil
	default:
		return errors.New("gkafka: async producer input is full")
	}
}

func (p *Production) SendAsync(topic, key, value string) error {
	if len(topic) == 0 {
		topic = p.Topic
	}
	if len(topic) == 0 {
		return errors.New("gkafka: topic is required")
	}
	return p.sendAsyncMessage(&sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	})
}

func (p *Production) sendSync(topic string, withPartition bool, partition int32, key, value string) error {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.StringEncoder(key),
		Value: sarama.StringEncoder(value),
	}
	if withPartition {
		msg.Partition = partition
	}
	return p.sendSyncMessage(msg)
}

func (p *Production) sendSyncMessage(msg *sarama.ProducerMessage) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return errors.New("gkafka: production is closed")
	}
	if p.sync == nil {
		return errors.New("gkafka: production is not connected")
	}
	_, _, err := p.sync.SendMessage(msg)
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
func (p *Production) SendMessage(m Message) error {
	msg, err := p.toProducerMessage(m)
	if err != nil {
		return err
	}
	return p.sendSyncMessage(msg)
}

// SendMessageAsync 异步高吞吐发送一条完整的 Message，立即返回不等待。
// 成功不另行通知，失败通过注册时的 AsyncErrHandler 回调（未设置则写日志）。
// 字段语义与 SendMessage 一致：支持二进制、Headers 透传、m.Topic 为空回退默认。
func (p *Production) SendMessageAsync(m Message) error {
	msg, err := p.toProducerMessage(m)
	if err != nil {
		return err
	}
	return p.sendAsyncMessage(msg)
}

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

// Close 关闭同步与异步 producer（刷出缓冲消息），最后关闭共享的底层连接。
// 顺序固定：先关 producer 再关 client。返回过程中出现的最后一个错误。
func (p *Production) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true
	syncProducer := p.sync
	asyncProducer := p.async
	client := p.client
	p.sync = nil
	p.async = nil
	p.client = nil
	p.mu.Unlock()

	var e error
	if syncProducer != nil {
		if err := syncProducer.Close(); err != nil {
			e = err
		}
	}
	if asyncProducer != nil {
		if err := asyncProducer.Close(); err != nil {
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

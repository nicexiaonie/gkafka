/*
说明：消费端以 sarama ConsumerGroup 为底座。
  - RunCommit：推模式，框架自动消费、自动提交 offset，业务函数只管处理。
  - 业务函数 CallFunc 返回 nil 才提交 offset；返回 error 则原地重试该条，按 MaxRetry/OnExceed 策略处理。
*/

package gkafka

import (
	"context"
	"errors"
	"strconv"
	"sync"
	"time"

	"github.com/IBM/sarama"
)

type Consumer struct {
	NetWork string
	Address string
	Brokers []string
	Topics  []string
	GroupId string

	MinBytes int
	MaxBytes int
	MaxWait  time.Duration

	SASL SASLConf
	TLS  TLSConf

	MaxRetry      int
	OnExceed      OnExceed
	DLQTopic      string
	InitialOffset InitialOffset

	CallFunc func(ctx context.Context, message Message) error

	logger Logger

	group sarama.ConsumerGroup

	// dlq 死信生产者，按需懒加载。
	dlq     sarama.SyncProducer
	dlqOnce sync.Once
	dlqErr  error

	// userCtx 是使用者传入的业务上下文（仅传值）；lifeCtx 是内部生命周期上下文（Close 时取消）。
	userCtx context.Context
	lifeCtx context.Context
	cancel  context.CancelFunc
}

// Connect 建立消费组连接并初始化配置。注册时自动调用。缺少 GroupId/Topics 时返回 error。
func (c *Consumer) Connect() error {
	c.logger = resolveLogger(c.logger)
	if len(c.GroupId) == 0 {
		return errors.New("gkafka: consumer requires GroupId")
	}
	if len(c.Topics) == 0 {
		return errors.New("gkafka: consumer requires at least one topic in Topics")
	}
	if c.userCtx == nil {
		c.userCtx = context.Background()
	}
	if err := c.validateRetryPolicy(); err != nil {
		return err
	}
	cfg, err := buildConfig(c.SASL, c.TLS)
	if err != nil {
		return err
	}
	cfg.Consumer.Return.Errors = true
	// 仅在无已提交位点时生效（新组或重置后）；已有位点则从上次继续。
	// 配合"成功才提交"实现至少一次，尽量不丢。
	initial, err := initialOffsetFor(c.InitialOffset)
	if err != nil {
		return err
	}
	cfg.Consumer.Offsets.Initial = initial
	if c.MinBytes > 0 {
		cfg.Consumer.Fetch.Min = int32(c.MinBytes)
	}
	if c.MaxBytes > 0 {
		cfg.Consumer.Fetch.Max = int32(c.MaxBytes)
	}
	if c.MaxWait > 0 {
		cfg.Consumer.MaxWaitTime = c.MaxWait
	}

	brokers, err := resolveBrokers(c.Brokers, c.Address)
	if err != nil {
		return err
	}

	group, err := sarama.NewConsumerGroup(brokers, c.GroupId, cfg)
	if err != nil {
		return err
	}
	c.group = group

	ctx, cancel := context.WithCancel(context.Background())
	c.lifeCtx = ctx
	c.cancel = cancel

	// 后台消费 group 级错误，避免阻塞。
	go func() {
		for e := range group.Errors() {
			c.logger.Printf("gkafka consumer group error: %v", e)
		}
	}()
	return nil
}

// validateRetryPolicy 校验 MaxRetry 与 OnExceed 的配对，避免 MaxRetry 被静默忽略。
//
//	MaxRetry=0  : OnExceed 必须为空（无限重试）。
//	MaxRetry>0  : OnExceed 必须为 "skip" 或 "dlq"；为 "dlq" 时 DLQTopic 必填。
func (c *Consumer) validateRetryPolicy() error {
	switch {
	case c.MaxRetry < 0:
		return errors.New("gkafka: MaxRetry must be >= 0")
	case c.MaxRetry == 0:
		if c.OnExceed != "" {
			return errors.New("gkafka: OnExceed is only valid when MaxRetry > 0; set MaxRetry or clear OnExceed")
		}
	default: // MaxRetry > 0
		switch c.OnExceed {
		case OnExceedSkip:
		case OnExceedDLQ:
			if len(c.DLQTopic) == 0 {
				return errors.New("gkafka: OnExceed=dlq requires DLQTopic")
			}
		default:
			return errors.New("gkafka: MaxRetry>0 requires OnExceed to be \"skip\" or \"dlq\"")
		}
	}
	return nil
}

// RunCommit 启动自动消费循环（推模式），阻塞运行直到 Close。
// 业务函数返回 nil 提交 offset；返回 error 按 MaxRetry/OnExceed 处理。
func (c *Consumer) RunCommit() error {
	if c.CallFunc == nil {
		return errors.New("gkafka: CallFunc is required")
	}
	if c.group == nil {
		return errors.New("gkafka: consumer is not connected")
	}

	h := &groupHandler{c: c}
	for {
		// Consume 在每次 rebalance 后返回，需重新进入；lifeCtx 取消时退出。
		if err := c.group.Consume(c.lifeCtx, c.Topics, h); err != nil {
			if errors.Is(err, sarama.ErrClosedConsumerGroup) || c.lifeCtx.Err() != nil {
				return nil
			}
			c.logger.Printf("gkafka consume error, will retry: %v", err)
			time.Sleep(time.Second)
		}
		if c.lifeCtx.Err() != nil {
			return nil
		}
	}
}

// groupHandler 实现 sarama.ConsumerGroupHandler，承载重试/确认/死信逻辑。
type groupHandler struct {
	c *Consumer
}

func (h *groupHandler) Setup(sarama.ConsumerGroupSession) error   { return nil }
func (h *groupHandler) Cleanup(sarama.ConsumerGroupSession) error { return nil }

func (h *groupHandler) ConsumeClaim(sess sarama.ConsumerGroupSession, claim sarama.ConsumerGroupClaim) error {
	c := h.c
	// 合并上下文：取值来自使用者的 userCtx，取消信号来自 session（rebalance/Close 时触发）。
	// 这样业务函数既能读 userCtx 的值，又能在 rebalance/Close 时收到 ctx.Done()。
	ctx := mergeCtx(c.userCtx, sess.Context())
	for {
		select {
		case msg, ok := <-claim.Messages():
			if !ok {
				return nil
			}
			m := fromConsumerMessage(msg)

			if h.handleAuto(ctx, sess, msg, m) {
				sess.MarkMessage(msg, "")
			}
		case <-sess.Context().Done():
			return nil
		}
	}
}

// handleAuto 执行业务处理的重试与超限策略，返回 true 表示应提交 offset。
func (h *groupHandler) handleAuto(ctx context.Context, sess sarama.ConsumerGroupSession, raw *sarama.ConsumerMessage, m Message) bool {
	c := h.c
	attempt := 0
	for {
		attempt++
		err := c.CallFunc(ctx, m)
		if err == nil {
			return true
		}

		// MaxRetry=0 表示无限重试（validateRetryPolicy 已保证此时 OnExceed 为空）。
		if c.MaxRetry > 0 && attempt >= c.MaxRetry {
			switch c.OnExceed {
			case OnExceedSkip:
				c.logger.Printf("gkafka skip message after %d retries: topic=%s partition=%d offset=%d err=%v",
					attempt, m.Topic, m.Partition, m.Offset, err)
				return true // 提交 = 跳过，会丢这条
			case OnExceedDLQ:
				if derr := c.toDLQ(raw, attempt, err); derr != nil {
					c.logger.Printf("gkafka send to DLQ failed, keep retrying: %v", derr)
					// 死信失败不提交，继续重试，避免丢消息。
				} else {
					c.logger.Printf("gkafka message moved to DLQ=%s: topic=%s offset=%d", c.DLQTopic, m.Topic, m.Offset)
					return true
				}
			}
		}

		// 退避：与旧实现保持一致。
		if attempt > 100 {
			time.Sleep(5 * time.Second)
		} else if attempt > 3 {
			time.Sleep(1 * time.Second)
		}

		if sess.Context().Err() != nil {
			return false
		}
	}
}

// toDLQ 把处理不动的消息投递到死信 topic，保留原始 Key/Value/Headers，并追加来源与失败信息。
func (c *Consumer) toDLQ(msg *sarama.ConsumerMessage, attempt int, cause error) error {
	if len(c.DLQTopic) == 0 {
		return errors.New("gkafka: OnExceed=dlq requires DLQTopic")
	}
	c.dlqOnce.Do(func() {
		pcfg, err := buildConfig(c.SASL, c.TLS)
		if err != nil {
			c.dlqErr = err
			return
		}
		pcfg.Producer.Return.Successes = true
		pcfg.Producer.Return.Errors = true
		pcfg.Producer.RequiredAcks = sarama.WaitForLocal
		brokers, err := resolveBrokers(c.Brokers, c.Address)
		if err != nil {
			c.dlqErr = err
			return
		}
		c.dlq, c.dlqErr = sarama.NewSyncProducer(brokers, pcfg)
	})
	if c.dlqErr != nil {
		return c.dlqErr
	}
	_, _, err := c.dlq.SendMessage(c.toDLQMessage(msg, attempt, cause))
	return err
}

func (c *Consumer) toDLQMessage(msg *sarama.ConsumerMessage, attempt int, cause error) *sarama.ProducerMessage {
	causeText := ""
	if cause != nil {
		causeText = cause.Error()
	}
	out := &sarama.ProducerMessage{
		Topic: c.DLQTopic,
		Key:   sarama.ByteEncoder(msg.Key),
		Value: sarama.ByteEncoder(msg.Value),
	}
	if len(msg.Headers) > 0 {
		out.Headers = make([]sarama.RecordHeader, 0, len(msg.Headers)+5)
		for _, h := range msg.Headers {
			out.Headers = append(out.Headers, sarama.RecordHeader{Key: h.Key, Value: h.Value})
		}
	} else {
		out.Headers = make([]sarama.RecordHeader, 0, 5)
	}
	out.Headers = append(out.Headers,
		sarama.RecordHeader{Key: []byte("gkafka-dlq-source-topic"), Value: []byte(msg.Topic)},
		sarama.RecordHeader{Key: []byte("gkafka-dlq-source-partition"), Value: []byte(strconv.FormatInt(int64(msg.Partition), 10))},
		sarama.RecordHeader{Key: []byte("gkafka-dlq-source-offset"), Value: []byte(strconv.FormatInt(msg.Offset, 10))},
		sarama.RecordHeader{Key: []byte("gkafka-dlq-attempt"), Value: []byte(strconv.Itoa(attempt))},
		sarama.RecordHeader{Key: []byte("gkafka-dlq-error"), Value: []byte(causeText)},
	)
	return out
}

// Close 取消消费循环并关闭消费组与死信生产者。返回过程中出现的最后一个错误。
func (c *Consumer) Close() error {
	if c.cancel != nil {
		c.cancel()
	}
	var e error
	if c.group != nil {
		if err := c.group.Close(); err != nil {
			e = err
		}
	}
	if c.dlq != nil {
		if err := c.dlq.Close(); err != nil {
			e = err
		}
	}
	return e
}

// mergeCtx 返回一个 context：取值（Value）来自 valueCtx，取消信号（Done/Err）来自 cancelCtx。
// 用于把使用者的业务上下文（携带 traceID 等）与 sarama session 的取消信号合二为一，
// 让业务函数既能读到值，又能在 rebalance/Close 时感知取消。
func mergeCtx(valueCtx, cancelCtx context.Context) context.Context {
	if valueCtx == nil || valueCtx == context.Background() {
		return cancelCtx
	}
	return mergedCtx{valueCtx: valueCtx, cancelCtx: cancelCtx}
}

type mergedCtx struct {
	valueCtx  context.Context
	cancelCtx context.Context
}

func (m mergedCtx) Deadline() (time.Time, bool)       { return m.cancelCtx.Deadline() }
func (m mergedCtx) Done() <-chan struct{}             { return m.cancelCtx.Done() }
func (m mergedCtx) Err() error                        { return m.cancelCtx.Err() }
func (m mergedCtx) Value(key interface{}) interface{} { return m.valueCtx.Value(key) }

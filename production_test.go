package gkafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/IBM/sarama"
	"github.com/IBM/sarama/mocks"
)

func TestFromProducerMessage(t *testing.T) {
	src := &sarama.ProducerMessage{
		Topic:     "t",
		Partition: 2,
		Offset:    7,
		Key:       sarama.StringEncoder("k"),
		Value:     sarama.StringEncoder("v"),
	}
	m := fromProducerMessage(src)
	if m.Topic != "t" || m.Partition != 2 || m.Offset != 7 {
		t.Errorf("基础字段映射错误: %+v", m)
	}
	if string(m.Key) != "k" || string(m.Value) != "v" {
		t.Errorf("Key/Value 解码错误: %+v", m)
	}
}

func TestFromProducerMessage_NilKeyValue(t *testing.T) {
	m := fromProducerMessage(&sarama.ProducerMessage{Topic: "t"})
	if m.Key != nil || m.Value != nil {
		t.Errorf("nil Encoder 时 Key/Value 应为 nil: %+v", m)
	}
}

// SendPartition 在非 manual 策略下必须返回错误，且该校验发生在触及底层
// sync producer 之前——所以这里无需 Connect 即可断言。
func TestSendPartition_RequiresManual(t *testing.T) {
	for _, balancer := range []Balancer{"", BalancerHash, BalancerRoundRobin, BalancerRandom, BalancerReferenceHash} {
		p := &Production{Topic: "t", Balancer: balancer}
		if err := p.SendPartition(context.Background(), 0, "k", "v"); err == nil {
			t.Errorf("Balancer=%q 时 SendPartition 应返回错误", balancer)
		}
	}
}

func TestToProducerMessage(t *testing.T) {
	p := &Production{Topic: "default_topic"}

	msg, err := p.toProducerMessage(Message{
		Topic:     "explicit",
		Partition: 5,
		Key:       []byte("k"),
		Value:     []byte("v"),
		Headers:   []Header{{Key: "h1", Value: []byte("v1")}, {Key: "h2", Value: []byte("v2")}},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Topic != "explicit" {
		t.Errorf("Topic=%s, want explicit", msg.Topic)
	}
	if msg.Partition != 5 {
		t.Errorf("Partition=%d, want 5", msg.Partition)
	}
	if k, _ := msg.Key.Encode(); string(k) != "k" {
		t.Errorf("Key=%s, want k", string(k))
	}
	if v, _ := msg.Value.Encode(); string(v) != "v" {
		t.Errorf("Value=%s, want v", string(v))
	}
	if len(msg.Headers) != 2 ||
		string(msg.Headers[0].Key) != "h1" || string(msg.Headers[0].Value) != "v1" ||
		string(msg.Headers[1].Key) != "h2" || string(msg.Headers[1].Value) != "v2" {
		t.Errorf("Headers 映射错误: %+v", msg.Headers)
	}
}

func TestToProducerMessage_DefaultTopicFallback(t *testing.T) {
	p := &Production{Topic: "default_topic"}
	msg, err := p.toProducerMessage(Message{Value: []byte("v")})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if msg.Topic != "default_topic" {
		t.Errorf("Topic=%s, want default_topic", msg.Topic)
	}
}

func TestToProducerMessage_EmptyTopic(t *testing.T) {
	p := &Production{} // 无默认 Topic
	if _, err := p.toProducerMessage(Message{}); err == nil {
		t.Error("Topic 为空时应返回错误")
	}
}

func TestToProducerMessage_NoHeaders(t *testing.T) {
	p := &Production{Topic: "t"}
	msg, _ := p.toProducerMessage(Message{Value: []byte("v")})
	if msg.Headers != nil {
		t.Errorf("无 Headers 时 msg.Headers 应为 nil, got %+v", msg.Headers)
	}
}

// SendMessage 在 Topic 为空时应在触及 sync producer 之前返回错误，
// 因此构造未 Connect 的 Production 即可断言。
func TestSendMessage_EmptyTopic(t *testing.T) {
	p := &Production{}
	if err := p.SendMessage(context.Background(), Message{}); err == nil {
		t.Error("Topic 为空时 SendMessage 应返回错误")
	}
}

func TestSend_NotConnected(t *testing.T) {
	p := &Production{Topic: "t"}
	if err := p.Send(context.Background(), "k", "v"); err == nil {
		t.Fatal("expected error when production is not connected, got nil")
	}
}

func TestSendMessage_NotConnected(t *testing.T) {
	p := &Production{Topic: "t"}
	if err := p.SendMessage(context.Background(), Message{Value: []byte("v")}); err == nil {
		t.Fatal("expected error when production is not connected, got nil")
	}
}

func TestSendAsync_NotConnected(t *testing.T) {
	p := &Production{Topic: "t"}
	if err := p.SendAsync(context.Background(), "", "k", "v"); err == nil {
		t.Fatal("expected error when production is not connected, got nil")
	}
}

func TestSendMessageAsync_NotConnected(t *testing.T) {
	p := &Production{Topic: "t"}
	if err := p.SendMessageAsync(context.Background(), Message{Value: []byte("v")}); err == nil {
		t.Fatal("expected error when production is not connected, got nil")
	}
}

func TestClose_Idempotent(t *testing.T) {
	p := &Production{}
	if err := p.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if err := p.Close(); err != nil {
		t.Fatalf("unexpected error on second close: %v", err)
	}
}

// chanLogger 把每条日志格式化后写入 channel，供断言默认日志路径。
type chanLogger chan string

func (l chanLogger) Printf(format string, v ...interface{}) { l <- fmt.Sprintf(format, v...) }

func TestSend_Success(t *testing.T) {
	sp := mocks.NewSyncProducer(t, nil)
	sp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		k, _ := msg.Key.Encode()
		v, _ := msg.Value.Encode()
		if msg.Topic != "t" || string(k) != "k" || string(v) != "v" {
			return fmt.Errorf("消息内容错误: topic=%s key=%s value=%s", msg.Topic, k, v)
		}
		return nil
	})
	p := &Production{Topic: "t", sync: sp, logger: &recordingLogger{}}

	if err := p.Send(context.Background(), "k", "v"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = sp.Close()
}

func TestSend_FailureReturnsError(t *testing.T) {
	errBoom := errors.New("boom")
	sp := mocks.NewSyncProducer(t, nil)
	sp.ExpectSendMessageAndFail(errBoom)
	p := &Production{Topic: "t", sync: sp, logger: &recordingLogger{}}

	if err := p.Send(context.Background(), "k", "v"); !errors.Is(err, errBoom) {
		t.Fatalf("err=%v, want %v", err, errBoom)
	}
	_ = sp.Close()
}

func TestSendPartition_SetsPartition(t *testing.T) {
	cfg := mocks.NewTestConfig()
	cfg.Producer.Partitioner = sarama.NewManualPartitioner
	sp := mocks.NewSyncProducer(t, cfg)
	sp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		if msg.Partition != 3 {
			return fmt.Errorf("Partition=%d, want 3", msg.Partition)
		}
		return nil
	})
	p := &Production{Topic: "t", Balancer: BalancerManual, sync: sp, logger: &recordingLogger{}}

	if err := p.SendPartition(context.Background(), 3, "k", "v"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	_ = sp.Close()
}

// startDrain 模拟 Connect 中异步部分的装配：挂上 ap 并启动 drainAsync。
func startDrain(p *Production, ap sarama.AsyncProducer) {
	p.async = ap
	p.drained = make(chan struct{})
	go p.drainAsync(ap, p.drained)
}

func TestSendAsync_ErrorGoesToHandler(t *testing.T) {
	errBoom := errors.New("boom")
	ap := mocks.NewAsyncProducer(t, nil)
	ap.ExpectInputAndFail(errBoom)
	type result struct {
		msg Message
		err error
	}
	got := make(chan result, 1)
	p := &Production{
		Topic:  "t",
		logger: &recordingLogger{},
		AsyncErrHandler: func(msg Message, err error) {
			got <- result{msg, err}
		},
	}
	startDrain(p, ap)

	if err := p.SendAsync(context.Background(), "", "k", "v"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	select {
	case r := <-got:
		if !errors.Is(r.err, errBoom) {
			t.Errorf("err=%v, want %v", r.err, errBoom)
		}
		if r.msg.Topic != "t" || string(r.msg.Key) != "k" || string(r.msg.Value) != "v" {
			t.Errorf("回调消息错误: %+v", r.msg)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("AsyncErrHandler 未被调用")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSendAsync_ErrorDefaultsToLogger(t *testing.T) {
	ap := mocks.NewAsyncProducer(t, nil)
	ap.ExpectInputAndFail(errors.New("boom"))
	logs := make(chanLogger, 1)
	p := &Production{Topic: "t", logger: logs}
	startDrain(p, ap)

	if err := p.SendAsync(context.Background(), "", "k", "v"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	select {
	case line := <-logs:
		if !strings.Contains(line, "gkafka async send error") || !strings.Contains(line, "boom") {
			t.Errorf("日志内容错误: %s", line)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("未设置 AsyncErrHandler 时应写日志")
	}
	if err := p.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// closingAsync 模拟真实 sarama 关闭行为：AsyncClose 后仍有在途消息的结果送达，
// 送完才关闭两个结果通道；Close 则像 sarama 一样自行读走剩余结果。
type closingAsync struct {
	sarama.AsyncProducer
	succ chan *sarama.ProducerMessage
	errs chan *sarama.ProducerError
	late error
}

func newClosingAsync(late error) *closingAsync {
	return &closingAsync{
		succ: make(chan *sarama.ProducerMessage),
		errs: make(chan *sarama.ProducerError),
		late: late,
	}
}

func (a *closingAsync) Successes() <-chan *sarama.ProducerMessage { return a.succ }
func (a *closingAsync) Errors() <-chan *sarama.ProducerError      { return a.errs }

func (a *closingAsync) AsyncClose() {
	go func() {
		a.succ <- &sarama.ProducerMessage{Topic: "t"}
		a.errs <- &sarama.ProducerError{Msg: &sarama.ProducerMessage{Topic: "t", Key: sarama.StringEncoder("k")}, Err: a.late}
		close(a.errs)
		close(a.succ)
	}()
}

func (a *closingAsync) Close() error {
	a.AsyncClose()
	go func() {
		for range a.succ {
		}
	}()
	for range a.errs {
	}
	return nil
}

// 关闭期间送达的结果必须全部由 drainAsync 处理：不崩溃，且失败结果交给 AsyncErrHandler。
func TestClose_DrainsInflightResults(t *testing.T) {
	errLate := errors.New("late")
	ap := newClosingAsync(errLate)
	var got []error
	p := &Production{
		logger:          &recordingLogger{},
		AsyncErrHandler: func(_ Message, err error) { got = append(got, err) },
	}
	startDrain(p, ap)

	if err := p.Close(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 1 || !errors.Is(got[0], errLate) {
		t.Fatalf("AsyncErrHandler 收到 %v, want [%v]", got, errLate)
	}
}

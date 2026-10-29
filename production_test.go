package gkafka

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
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

// newMockBrokerProduction 用 sarama 模拟 broker 驱动真实的 sarama producer，
// 与 mocks.AsyncProducer 不同，其异步入口通道与生产环境一样是无缓冲的。
func newMockBrokerProduction(t *testing.T, onErr func(Message, error)) *Production {
	t.Helper()
	mb := sarama.NewMockBroker(t, 1)
	t.Cleanup(mb.Close)
	mb.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(mb.Addr(), mb.BrokerID()).
			SetLeader("t", 0, mb.BrokerID()),
		"ProduceRequest": sarama.NewMockProduceResponse(t),
	})
	p := &Production{Brokers: []string{mb.Addr()}, Topic: "t", logger: &recordingLogger{}, AsyncErrHandler: onErr}
	if err := p.Connect(); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	return p
}

// 连续与并发异步发送不应因 sarama 正在处理上一条消息而失败。
func TestSendAsync_BurstWithRealProducer(t *testing.T) {
	var handlerCalls int64
	p := newMockBrokerProduction(t, func(Message, error) { atomic.AddInt64(&handlerCalls, 1) })

	const n = 2000
	for i := 0; i < n; i++ {
		if err := p.SendAsync(context.Background(), "", "k", "v"); err != nil {
			t.Fatalf("顺序发送第 %d 条失败: %v", i, err)
		}
	}

	var failed int64
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < n/8; i++ {
				if err := p.SendAsync(context.Background(), "", "k", "v"); err != nil {
					atomic.AddInt64(&failed, 1)
				}
			}
		}()
	}
	wg.Wait()
	if failed != 0 {
		t.Fatalf("并发发送失败 %d 条, want 0", failed)
	}

	if err := p.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if handlerCalls != 0 {
		t.Fatalf("AsyncErrHandler 被调用 %d 次, want 0", handlerCalls)
	}
}

// inputAsync 只提供可控的入口通道：无缓冲且无人读取时可模拟 sarama 一直不接收。
type inputAsync struct {
	sarama.AsyncProducer
	in chan *sarama.ProducerMessage
}

func (a *inputAsync) Input() chan<- *sarama.ProducerMessage { return a.in }
func (a *inputAsync) AsyncClose()                           {}

// newInputProduction 装配一个使用 inputAsync 的 Production；drained 预先关闭，使 Close 不等待结果。
func newInputProduction(in chan *sarama.ProducerMessage) *Production {
	drained := make(chan struct{})
	close(drained)
	return &Production{
		Topic:   "t",
		async:   &inputAsync{in: in},
		drained: drained,
		logger:  &recordingLogger{},
	}
}

func TestSendAsync_TimeoutWhileWaiting(t *testing.T) {
	p := newInputProduction(make(chan *sarama.ProducerMessage))
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	err := p.SendAsync(ctx, "", "k", "v")
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("err=%v, want DeadlineExceeded", err)
	}
	if d := time.Since(start); d > 2*time.Second {
		t.Fatalf("等待 %v 才返回，ctx 超时未生效", d)
	}
}

func TestSendAsync_AlreadyCanceled(t *testing.T) {
	in := make(chan *sarama.ProducerMessage, 1)
	p := newInputProduction(in)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := p.SendAsync(ctx, "", "k", "v"); !errors.Is(err, context.Canceled) {
		t.Fatalf("err=%v, want Canceled", err)
	}
	if len(in) != 0 {
		t.Fatalf("ctx 已取消时不应投递消息，入口通道中有 %d 条", len(in))
	}
}

func TestSendAsync_NilContext(t *testing.T) {
	in := make(chan *sarama.ProducerMessage, 1)
	p := newInputProduction(in)

	// 显式持有 nil context 以测试兼容处理（直接写字面量 nil 会触发 SA1012）。
	var nilCtx context.Context
	if err := p.SendAsync(nilCtx, "", "k", "v"); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(in) != 1 {
		t.Fatalf("入口通道中有 %d 条, want 1", len(in))
	}
}

// 有发送在等待 sarama 接收时，Close 等它结束（此处为 ctx 超时）后完成，之后发送返回已关闭。
func TestClose_WaitsBlockedSender(t *testing.T) {
	p := newInputProduction(make(chan *sarama.ProducerMessage))
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	sendErr := make(chan error, 1)
	go func() { sendErr <- p.SendAsync(ctx, "", "k", "v") }()
	time.Sleep(50 * time.Millisecond) // 确保发送已进入等待

	closeDone := make(chan error, 1)
	go func() { closeDone <- p.Close() }()

	select {
	case err := <-sendErr:
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("send err=%v, want DeadlineExceeded", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("等待中的发送未在 ctx 超时后返回")
	}
	select {
	case err := <-closeDone:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Close 未完成")
	}
	if err := p.SendAsync(context.Background(), "", "k", "v"); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("关闭后发送 err=%v, want closed", err)
	}
}

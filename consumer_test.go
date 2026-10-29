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

func TestValidateRetryPolicy(t *testing.T) {
	cases := []struct {
		name     string
		maxRetry int
		onExceed OnExceed
		dlqTopic string
		wantErr  bool
	}{
		{"negative", -1, "", "", true},
		{"zero-empty-ok", 0, "", "", false},
		{"zero-with-onexceed", 0, OnExceedSkip, "", true},
		{"retry-skip-ok", 3, OnExceedSkip, "", false},
		{"retry-dlq-ok", 3, OnExceedDLQ, "dlq_topic", false},
		{"retry-dlq-missing-topic", 3, OnExceedDLQ, "", true},
		{"retry-empty-onexceed", 3, "", "", true},
		{"retry-bad-onexceed", 3, "bogus", "", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cc := &Consumer{MaxRetry: c.maxRetry, OnExceed: c.onExceed, DLQTopic: c.dlqTopic}
			err := cc.validateRetryPolicy()
			if c.wantErr && err == nil {
				t.Errorf("expected error, got nil")
			}
			if !c.wantErr && err != nil {
				t.Errorf("unexpected error: %v", err)
			}
		})
	}
}

type ctxKey string

func TestToDLQMessage_PreservesHeadersAndAddsMetadata(t *testing.T) {
	c := &Consumer{DLQTopic: "dlq"}
	src := &sarama.ConsumerMessage{
		Topic:     "source_topic",
		Partition: 3,
		Offset:    42,
		Key:       []byte("k"),
		Value:     []byte("v"),
		Headers: []*sarama.RecordHeader{
			{Key: []byte("traceparent"), Value: []byte("trace-1")},
		},
	}

	msg := c.toDLQMessage(src, 5, errors.New("boom"))

	if msg.Topic != "dlq" {
		t.Fatalf("Topic=%s, want dlq", msg.Topic)
	}
	if k, _ := msg.Key.Encode(); string(k) != "k" {
		t.Fatalf("Key=%s, want k", string(k))
	}
	if v, _ := msg.Value.Encode(); string(v) != "v" {
		t.Fatalf("Value=%s, want v", string(v))
	}

	headers := make(map[string]string, len(msg.Headers))
	for _, h := range msg.Headers {
		headers[string(h.Key)] = string(h.Value)
	}
	cases := map[string]string{
		"traceparent":                 "trace-1",
		"gkafka-dlq-source-topic":     "source_topic",
		"gkafka-dlq-source-partition": "3",
		"gkafka-dlq-source-offset":    "42",
		"gkafka-dlq-attempt":          "5",
		"gkafka-dlq-error":            "boom",
	}
	for key, want := range cases {
		if got := headers[key]; got != want {
			t.Fatalf("header %s=%q, want %q", key, got, want)
		}
	}
}

func TestToDLQMessage_NilCause(t *testing.T) {
	c := &Consumer{DLQTopic: "dlq"}
	msg := c.toDLQMessage(&sarama.ConsumerMessage{Topic: "source"}, 1, nil)

	for _, h := range msg.Headers {
		if string(h.Key) == "gkafka-dlq-error" && string(h.Value) != "" {
			t.Fatalf("gkafka-dlq-error=%q, want empty", string(h.Value))
		}
	}
}

func TestRunCommit_NotConnected(t *testing.T) {
	c := &Consumer{CallFunc: func(context.Context, Message) error { return nil }}
	if err := c.RunCommit(); err == nil {
		t.Fatal("expected error when consumer is not connected, got nil")
	}
}

func TestMergeCtx_FallbackToCancelCtx(t *testing.T) {
	cancelCtx := context.Background()
	// 显式持有 nil context 以测试 mergeCtx 的 nil 分支（直接写字面量 nil 会触发 vet SA1012）。
	var nilCtx context.Context
	if got := mergeCtx(nilCtx, cancelCtx); got != cancelCtx {
		t.Error("valueCtx 为 nil 时应直接返回 cancelCtx")
	}
	if got := mergeCtx(context.Background(), cancelCtx); got != cancelCtx {
		t.Error("valueCtx 为 Background 时应直接返回 cancelCtx")
	}
}

func TestMergeCtx_ValueFromValueCancelFromCancel(t *testing.T) {
	key := ctxKey("trace")
	valueCtx := context.WithValue(context.Background(), key, "abc")
	cancelCtx, cancel := context.WithCancel(context.Background())

	merged := mergeCtx(valueCtx, cancelCtx)

	// 取值来自 valueCtx
	if got := merged.Value(key); got != "abc" {
		t.Errorf("Value(%v) = %v, want abc", key, got)
	}
	// 取消信号来自 cancelCtx：取消前未触发
	if merged.Err() != nil {
		t.Errorf("取消前 Err 应为 nil, got %v", merged.Err())
	}
	// 取消后触发
	cancel()
	<-merged.Done()
	if merged.Err() == nil {
		t.Error("取消后 Err 应非 nil")
	}
}

// fakeSession 是 sarama.ConsumerGroupSession 的最小实现，只记录 MarkMessage。
type fakeSession struct {
	ctx    context.Context
	mu     sync.Mutex
	marked []*sarama.ConsumerMessage
}

func (s *fakeSession) Claims() map[string][]int32               { return nil }
func (s *fakeSession) MemberID() string                         { return "" }
func (s *fakeSession) GenerationID() int32                      { return 0 }
func (s *fakeSession) MarkOffset(string, int32, int64, string)  {}
func (s *fakeSession) Commit()                                  {}
func (s *fakeSession) ResetOffset(string, int32, int64, string) {}
func (s *fakeSession) Context() context.Context                 { return s.ctx }
func (s *fakeSession) MarkMessage(msg *sarama.ConsumerMessage, _ string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.marked = append(s.marked, msg)
}

// fakeClaim 是 sarama.ConsumerGroupClaim 的最小实现，消息来自预置 channel。
type fakeClaim struct{ msgs chan *sarama.ConsumerMessage }

func (c *fakeClaim) Topic() string                            { return "t" }
func (c *fakeClaim) Partition() int32                         { return 0 }
func (c *fakeClaim) InitialOffset() int64                     { return 0 }
func (c *fakeClaim) HighWaterMarkOffset() int64               { return 0 }
func (c *fakeClaim) Messages() <-chan *sarama.ConsumerMessage { return c.msgs }

// runClaim 用给定 ctx 作为 session 上下文，把 msgs 依次交给 ConsumeClaim 处理到结束，
// 返回被 MarkMessage 的消息。
func runClaim(t *testing.T, ctx context.Context, c *Consumer, msgs ...*sarama.ConsumerMessage) []*sarama.ConsumerMessage {
	t.Helper()
	ch := make(chan *sarama.ConsumerMessage, len(msgs))
	for _, m := range msgs {
		ch <- m
	}
	close(ch)
	if c.logger == nil {
		c.logger = &recordingLogger{}
	}
	sess := &fakeSession{ctx: ctx}
	if err := (&groupHandler{c: c}).ConsumeClaim(sess, &fakeClaim{msgs: ch}); err != nil {
		t.Fatalf("ConsumeClaim error: %v", err)
	}
	return sess.marked
}

func TestConsumeClaim_SuccessMarks(t *testing.T) {
	var got Message
	c := &Consumer{CallFunc: func(_ context.Context, m Message) error {
		got = m
		return nil
	}}
	src := &sarama.ConsumerMessage{Topic: "t", Partition: 1, Offset: 10, Value: []byte("v")}

	marked := runClaim(t, context.Background(), c, src)

	if len(marked) != 1 || marked[0] != src {
		t.Fatalf("marked=%v, want [src]", marked)
	}
	if got.Topic != "t" || got.Partition != 1 || got.Offset != 10 || string(got.Value) != "v" {
		t.Errorf("CallFunc 收到的消息错误: %+v", got)
	}
}

func TestConsumeClaim_RetryThenSuccess(t *testing.T) {
	calls := 0
	c := &Consumer{CallFunc: func(context.Context, Message) error {
		calls++
		if calls < 3 {
			return errors.New("boom")
		}
		return nil
	}}

	marked := runClaim(t, context.Background(), c, &sarama.ConsumerMessage{Topic: "t"})

	if calls != 3 {
		t.Errorf("calls=%d, want 3", calls)
	}
	if len(marked) != 1 {
		t.Errorf("len(marked)=%d, want 1", len(marked))
	}
}

func TestConsumeClaim_SkipAfterMaxRetry(t *testing.T) {
	calls := map[int64]int{}
	c := &Consumer{
		MaxRetry: 2,
		OnExceed: OnExceedSkip,
		CallFunc: func(_ context.Context, m Message) error {
			calls[m.Offset]++
			if m.Offset == 1 {
				return errors.New("poison")
			}
			return nil
		},
	}

	marked := runClaim(t, context.Background(), c,
		&sarama.ConsumerMessage{Topic: "t", Offset: 1},
		&sarama.ConsumerMessage{Topic: "t", Offset: 2},
	)

	if calls[1] != 2 || calls[2] != 1 {
		t.Errorf("calls=%v, want map[1:2 2:1]", calls)
	}
	if len(marked) != 2 {
		t.Errorf("len(marked)=%d, want 2（毒消息跳过后不应卡住后续）", len(marked))
	}
}

func TestConsumeClaim_DLQAfterMaxRetry(t *testing.T) {
	sp := mocks.NewSyncProducer(t, nil)
	sp.ExpectSendMessageWithMessageCheckerFunctionAndSucceed(func(msg *sarama.ProducerMessage) error {
		if msg.Topic != "dlq" {
			return fmt.Errorf("Topic=%s, want dlq", msg.Topic)
		}
		for _, h := range msg.Headers {
			if string(h.Key) == "gkafka-dlq-attempt" && string(h.Value) == "2" {
				return nil
			}
		}
		return fmt.Errorf("缺少 gkafka-dlq-attempt=2: %+v", msg.Headers)
	})
	calls := 0
	c := &Consumer{
		MaxRetry: 2,
		OnExceed: OnExceedDLQ,
		DLQTopic: "dlq",
		CallFunc: func(context.Context, Message) error {
			calls++
			return errors.New("boom")
		},
		dlq: sp,
	}
	c.dlqOnce.Do(func() {}) // 跳过懒加载，直接使用注入的 mock

	marked := runClaim(t, context.Background(), c, &sarama.ConsumerMessage{Topic: "t"})

	if calls != 2 {
		t.Errorf("calls=%d, want 2", calls)
	}
	if len(marked) != 1 {
		t.Errorf("len(marked)=%d, want 1", len(marked))
	}
	_ = sp.Close()
}

// 死信投递失败时不提交，持续重试直到 session 取消。
func TestConsumeClaim_DLQFailureKeepsRetrying(t *testing.T) {
	sp := mocks.NewSyncProducer(t, nil)
	for i := 0; i < 3; i++ {
		sp.ExpectSendMessageAndFail(errors.New("dlq down"))
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	c := &Consumer{
		MaxRetry: 1,
		OnExceed: OnExceedDLQ,
		DLQTopic: "dlq",
		CallFunc: func(context.Context, Message) error {
			calls++
			if calls == 3 {
				cancel()
			}
			return errors.New("boom")
		},
		dlq: sp,
	}
	c.dlqOnce.Do(func() {})

	marked := runClaim(t, ctx, c, &sarama.ConsumerMessage{Topic: "t"})

	if calls != 3 {
		t.Errorf("calls=%d, want 3", calls)
	}
	if len(marked) != 0 {
		t.Errorf("len(marked)=%d, want 0（死信失败不应提交）", len(marked))
	}
	_ = sp.Close()
}

// MaxRetry=0（无限重试）时，session 取消即退出且不提交。
func TestConsumeClaim_SessionCanceledStopsRetry(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	calls := 0
	c := &Consumer{CallFunc: func(context.Context, Message) error {
		calls++
		if calls == 2 {
			cancel()
		}
		return errors.New("boom")
	}}

	marked := runClaim(t, ctx, c, &sarama.ConsumerMessage{Topic: "t"})

	if calls != 2 {
		t.Errorf("calls=%d, want 2", calls)
	}
	if len(marked) != 0 {
		t.Errorf("len(marked)=%d, want 0", len(marked))
	}
}

// fakeGroup 模拟 sarama 消费组：Consume 收到取消后还需 settle 时长收尾
// （对应 sarama 等待处理协程退出并提交最后位点），再返回。
type fakeGroup struct {
	sarama.ConsumerGroup
	settle     time.Duration
	mu         sync.Mutex
	events     []string
	closeCalls int32
}

func (g *fakeGroup) record(e string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	g.events = append(g.events, e)
}

func (g *fakeGroup) Consume(ctx context.Context, _ []string, _ sarama.ConsumerGroupHandler) error {
	<-ctx.Done()
	time.Sleep(g.settle)
	g.record("consume-done")
	return nil
}

func (g *fakeGroup) Close() error {
	atomic.AddInt32(&g.closeCalls, 1)
	g.record("group-close")
	return nil
}

// newFakeGroupConsumer 装配一个使用 fakeGroup 的 Consumer（相当于 Connect 之后的状态）。
func newFakeGroupConsumer(g *fakeGroup) *Consumer {
	ctx, cancel := context.WithCancel(context.Background())
	return &Consumer{
		Topics:   []string{"t"},
		CallFunc: func(context.Context, Message) error { return nil },
		logger:   &recordingLogger{},
		group:    g,
		lifeCtx:  ctx,
		cancel:   cancel,
	}
}

// 用过死信（死信 producer 已创建）的消费者被关闭两次时不应崩溃。
func TestConsumerClose_TwiceWithDLQ(t *testing.T) {
	mb := sarama.NewMockBroker(t, 1)
	defer mb.Close()
	mb.SetHandlerByMap(map[string]sarama.MockResponse{
		"ApiVersionsRequest": sarama.NewMockApiVersionsResponse(t),
		"MetadataRequest": sarama.NewMockMetadataResponse(t).
			SetBroker(mb.Addr(), mb.BrokerID()),
	})
	cfg := sarama.NewConfig()
	cfg.Producer.Return.Successes = true
	sp, err := sarama.NewSyncProducer([]string{mb.Addr()}, cfg)
	if err != nil {
		t.Fatalf("NewSyncProducer: %v", err)
	}
	c := &Consumer{dlq: sp}

	if err := c.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	time.Sleep(200 * time.Millisecond) // 留出时间让二次关闭若触发的后台 panic 暴露出来
}

// Close 必须等 RunCommit 收尾（处理协程结束、最后位点提交）后才关闭消费组。
func TestConsumerClose_WaitsRunCommit(t *testing.T) {
	g := &fakeGroup{settle: 100 * time.Millisecond}
	c := newFakeGroupConsumer(g)
	runDone := make(chan error, 1)
	go func() { runDone <- c.RunCommit() }()
	time.Sleep(50 * time.Millisecond) // 确保 RunCommit 已进入 Consume

	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("RunCommit: %v", err)
	}
	g.mu.Lock()
	defer g.mu.Unlock()
	if len(g.events) != 2 || g.events[0] != "consume-done" || g.events[1] != "group-close" {
		t.Fatalf("events=%v, want [consume-done group-close]", g.events)
	}
}

func TestConsumerClose_WithoutRunCommit(t *testing.T) {
	g := &fakeGroup{}
	c := newFakeGroupConsumer(g)

	done := make(chan error, 1)
	go func() { done <- c.Close() }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Close: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("未运行 RunCommit 时 Close 不应等待")
	}
	if g.closeCalls != 1 {
		t.Fatalf("closeCalls=%d, want 1", g.closeCalls)
	}
}

func TestConsumerClose_Concurrent(t *testing.T) {
	g := &fakeGroup{settle: 50 * time.Millisecond}
	c := newFakeGroupConsumer(g)
	runDone := make(chan error, 1)
	go func() { runDone <- c.RunCommit() }()
	time.Sleep(20 * time.Millisecond)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := c.Close(); err != nil {
				t.Errorf("Close: %v", err)
			}
		}()
	}
	wg.Wait()
	<-runDone
	if n := atomic.LoadInt32(&g.closeCalls); n != 1 {
		t.Fatalf("closeCalls=%d, want 1", n)
	}
}

func TestRunCommit_AfterClose(t *testing.T) {
	c := newFakeGroupConsumer(&fakeGroup{})
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := c.RunCommit(); err == nil || !strings.Contains(err.Error(), "closed") {
		t.Fatalf("err=%v, want closed", err)
	}
}

func TestRunCommit_AlreadyRunning(t *testing.T) {
	c := newFakeGroupConsumer(&fakeGroup{})
	runDone := make(chan error, 1)
	go func() { runDone <- c.RunCommit() }()
	time.Sleep(20 * time.Millisecond)

	if err := c.RunCommit(); err == nil || !strings.Contains(err.Error(), "already running") {
		t.Fatalf("err=%v, want already running", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := <-runDone; err != nil {
		t.Fatalf("首个 RunCommit 不应受影响, err=%v", err)
	}
}

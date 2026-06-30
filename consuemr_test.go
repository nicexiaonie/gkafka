package gkafka

import (
	"context"
	"errors"
	"testing"

	"github.com/IBM/sarama"
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

package gkafka

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// resolveTracing 在未配置 TracerProvider 时应返回禁用态，保证"默认关闭、零侵入"。
func TestResolveTracing_DisabledByDefault(t *testing.T) {
	tr := resolveTracing(TracingConf{})
	if tr.enabled {
		t.Fatal("TracerProvider 为 nil 时 tracer 应处于禁用态")
	}
}

// producerCarrier 的 Set 应保证 key 唯一（重复 Set 覆盖而非追加），Get/Keys 正确。
func TestProducerCarrier_SetGetKeysUnique(t *testing.T) {
	msg := &sarama.ProducerMessage{Topic: "t"}
	c := producerCarrier{msg: msg}

	c.Set("traceparent", "v1")
	c.Set("traceparent", "v2") // 覆盖
	c.Set("other", "x")

	if got := c.Get("traceparent"); got != "v2" {
		t.Errorf("Get(traceparent)=%q, want v2", got)
	}
	if got := c.Get("missing"); got != "" {
		t.Errorf("Get(missing)=%q, want empty", got)
	}
	if len(msg.Headers) != 2 {
		t.Errorf("重复 Set 同名 key 后 header 数=%d, want 2", len(msg.Headers))
	}
	keys := c.Keys()
	if len(keys) != 2 {
		t.Errorf("Keys 数=%d, want 2", len(keys))
	}
}

// 端到端核心验证：生产侧把 trace 上下文注入消息 header，
// 消费侧从带相同 header 的消息中提取，trace-id 必须一致——链路才算续上。
func TestTracePropagation_ProducerToConsumer(t *testing.T) {
	prop := propagation.TraceContext{}

	// 构造一个已知 trace-id/span-id 的远端 span 上下文，作为"上游链路"。
	traceID, _ := trace.TraceIDFromHex("0123456789abcdef0123456789abcdef")
	spanID, _ := trace.SpanIDFromHex("0123456789abcdef")
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    traceID,
		SpanID:     spanID,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	// 生产侧：注入到 ProducerMessage.Headers。
	pmsg := &sarama.ProducerMessage{Topic: "t"}
	prop.Inject(ctx, producerCarrier{msg: pmsg})

	if len(pmsg.Headers) == 0 {
		t.Fatal("注入后 ProducerMessage 应携带 traceparent header")
	}

	// 模拟消息落到 broker 再被消费：把生产侧 header 原样搬到 ConsumerMessage。
	cmsg := &sarama.ConsumerMessage{Topic: "t"}
	for i := range pmsg.Headers {
		cmsg.Headers = append(cmsg.Headers, &pmsg.Headers[i])
	}

	// 消费侧：提取父链路。
	got := prop.Extract(context.Background(), consumerCarrier{msg: cmsg})
	gotSC := trace.SpanContextFromContext(got)

	if !gotSC.IsValid() {
		t.Fatal("消费侧提取出的 SpanContext 无效，链路未续上")
	}
	if gotSC.TraceID() != traceID {
		t.Errorf("trace-id 不一致: got=%s want=%s", gotSC.TraceID(), traceID)
	}
	if gotSC.SpanID() != spanID {
		t.Errorf("span-id 不一致: got=%s want=%s", gotSC.SpanID(), spanID)
	}
}

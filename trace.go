package gkafka

import (
	"context"

	"github.com/IBM/sarama"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

// instrumentationName 是本库在 OTel 中的 instrumentation scope 名，便于在后端按来源筛选 span。
const instrumentationName = "github.com/nicexiaonie/gkafka"

// TracingConf 是分布式链路追踪（OpenTelemetry）配置，生产/消费通用。
//
// 设计为"默认关闭、零侵入"：TracerProvider 为 nil 时整套 trace 逻辑完全空转——
// 不起 span、不读写 message header、不影响任何现有行为与性能。只有显式传入
// TracerProvider 才启用。本库只做"把链路上下文打包进 header / 从 header 拆出来"
// 这两个接头，不绑定具体后端（Jaeger/Zipkin/SkyWalking 等经 OTel 均可）。
type TracingConf struct {
	// TracerProvider 提供 Tracer。为 nil 表示关闭 trace（默认）。
	// 通常传入 otel.GetTracerProvider() 或自建的 SDK provider。
	TracerProvider trace.TracerProvider

	// Propagator 负责 trace 上下文在 header 上的注入/提取。
	// 为 nil 且 TracerProvider 非 nil 时，默认使用 W3C TraceContext（traceparent/tracestate）。
	Propagator propagation.TextMapPropagator
}

// tracer 是 TracingConf 解析后的内部可用形态。enabled=false 时所有方法是 no-op。
type tracer struct {
	enabled    bool
	t          trace.Tracer
	propagator propagation.TextMapPropagator
}

// resolveTracing 把对外的 TracingConf 解析为内部 tracer。
// TracerProvider 为 nil 时返回禁用态（enabled=false），调用方据此跳过全部 trace 操作。
func resolveTracing(tc TracingConf) tracer {
	if tc.TracerProvider == nil {
		return tracer{enabled: false}
	}
	prop := tc.Propagator
	if prop == nil {
		prop = propagation.TraceContext{}
	}
	return tracer{
		enabled:    true,
		t:          tc.TracerProvider.Tracer(instrumentationName),
		propagator: prop,
	}
}

// producerCarrier 把 sarama.ProducerMessage.Headers 适配为 OTel TextMapCarrier，
// 供 propagator 注入/读取 trace 上下文。Headers 是值切片（[]RecordHeader）。
type producerCarrier struct{ msg *sarama.ProducerMessage }

// Get 按 key 从 ProducerMessage.Headers 中读取第一个匹配值；不存在返回空字符串。
func (c producerCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set 以幂等方式写入 ProducerMessage.Headers，先删除同名项再追加。
func (c producerCarrier) Set(key, val string) {
	// 保证 key 唯一：先删除同名 header，再追加。
	for i := 0; i < len(c.msg.Headers); i++ {
		if string(c.msg.Headers[i].Key) == key {
			c.msg.Headers = append(c.msg.Headers[:i], c.msg.Headers[i+1:]...)
			i--
		}
	}
	c.msg.Headers = append(c.msg.Headers, sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(val),
	})
}

// Keys 返回 ProducerMessage.Headers 中的所有 key，供 propagator 遍历。
func (c producerCarrier) Keys() []string {
	out := make([]string, len(c.msg.Headers))
	for i, h := range c.msg.Headers {
		out[i] = string(h.Key)
	}
	return out
}

// consumerCarrier 把 sarama.ConsumerMessage.Headers 适配为 OTel TextMapCarrier。
// 与生产侧不同，消费消息的 Headers 是指针切片（[]*RecordHeader），只读提取，不写。
type consumerCarrier struct{ msg *sarama.ConsumerMessage }

// Get 按 key 从 ConsumerMessage.Headers 指针切片中读取第一个匹配值；跳过 nil 项。
func (c consumerCarrier) Get(key string) string {
	for _, h := range c.msg.Headers {
		if h != nil && string(h.Key) == key {
			return string(h.Value)
		}
	}
	return ""
}

// Set 以幂等方式写入 ConsumerMessage.Headers，处理指针切片格式并保证同一 key 唯一。
func (c consumerCarrier) Set(key, val string) {
	for i := 0; i < len(c.msg.Headers); i++ {
		if c.msg.Headers[i] != nil && string(c.msg.Headers[i].Key) == key {
			c.msg.Headers = append(c.msg.Headers[:i], c.msg.Headers[i+1:]...)
			i--
		}
	}
	c.msg.Headers = append(c.msg.Headers, &sarama.RecordHeader{
		Key:   []byte(key),
		Value: []byte(val),
	})
}

// Keys 返回 ConsumerMessage.Headers 中所有非 nil header 的 key，供 propagator 遍历。
func (c consumerCarrier) Keys() []string {
	out := make([]string, len(c.msg.Headers))
	for i, h := range c.msg.Headers {
		if h != nil {
			out[i] = string(h.Key)
		}
	}
	return out
}

// startProduce 起一个 producer span 并把链路上下文注入消息 header。
// 父 span 取自传入的 ctx（生产端已全面携带 ctx），这是与 otelsarama 的关键区别——
// 后者无 ctx，只能从 header 反向提取父 span。enabled=false 时调用方不会进入此函数。
// 返回的 span 由调用方在发送完成后（同步）或在 drainAsync 回收时（异步）结束。
func (tr tracer) startProduce(ctx context.Context, msg *sarama.ProducerMessage) trace.Span {
	if ctx == nil {
		ctx = context.Background()
	}
	carrier := producerCarrier{msg: msg}
	ctx, span := tr.t.Start(ctx, msg.Topic+" publish",
		trace.WithSpanKind(trace.SpanKindProducer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", msg.Topic),
			attribute.String("messaging.operation", "publish"),
		),
	)
	// 注入当前 span 上下文，消费端才能据此续上链路。
	tr.propagator.Inject(ctx, carrier)
	return span
}

// finishProduce 给 producer span 补上分区/offset 属性与状态，并结束。
// 同步发送在 SendMessage 返回后调用；异步发送在 drainAsync 消费成功/失败时调用。
// span 为 nil（trace 关闭或未关联）时直接返回。
func finishProduce(span trace.Span, partition int32, offset int64, err error) {
	if span == nil {
		return
	}
	span.SetAttributes(
		attribute.Int("messaging.kafka.destination.partition", int(partition)),
		attribute.Int64("messaging.kafka.message.offset", offset),
	)
	if err != nil {
		span.SetStatus(codes.Error, err.Error())
	}
	span.End()
}

// spanFromMetadata 从异步消息的 Metadata 中取回先前关联的 producer span。
// 未关联（trace 关闭，或业务自定义了 Metadata）时返回 nil。
func spanFromMetadata(md any) trace.Span {
	if s, ok := md.(trace.Span); ok {
		return s
	}
	return nil
}

// startConsume 从消息 header 提取父链路上下文，起一个 consumer span，
// 返回携带该 span 的 ctx 与 span 本身。span 覆盖"这条消息的整个处理过程"
// （含重试），由调用方在处理结束时结束。enabled=false 时调用方不会进入此函数。
func (tr tracer) startConsume(ctx context.Context, msg *sarama.ConsumerMessage) (context.Context, trace.Span) {
	carrier := consumerCarrier{msg: msg}
	parent := tr.propagator.Extract(ctx, carrier)
	return tr.t.Start(parent, msg.Topic+" process",
		trace.WithSpanKind(trace.SpanKindConsumer),
		trace.WithAttributes(
			attribute.String("messaging.system", "kafka"),
			attribute.String("messaging.destination.name", msg.Topic),
			attribute.String("messaging.operation", "process"),
			attribute.Int("messaging.kafka.destination.partition", int(msg.Partition)),
			attribute.Int64("messaging.kafka.message.offset", msg.Offset),
		),
	)
}

package gkafka

import (
	"context"
	"testing"

	"github.com/IBM/sarama"
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

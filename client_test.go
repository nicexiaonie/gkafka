package gkafka

import (
	"reflect"
	"testing"

	"github.com/IBM/sarama"
)

// fnPtr 返回函数值的代码指针，用于断言两个函数构造器是否同一个。
func fnPtr(v interface{}) uintptr {
	return reflect.ValueOf(v).Pointer()
}

func TestPartitionerFor(t *testing.T) {
	cases := []struct {
		balancer Balancer
		want     sarama.PartitionerConstructor
		wantErr  bool
	}{
		{BalancerHash, sarama.NewHashPartitioner, false},
		{BalancerReferenceHash, sarama.NewReferenceHashPartitioner, false},
		{BalancerRoundRobin, sarama.NewRoundRobinPartitioner, false},
		{BalancerManual, sarama.NewManualPartitioner, false},
		{BalancerRandom, sarama.NewRandomPartitioner, false},
		{"", sarama.NewRandomPartitioner, false}, // 留空默认 random
		{"unknown", nil, true},                   // 未知值报错（#3 修复后）
	}
	for _, c := range cases {
		got, err := partitionerFor(c.balancer)
		if c.wantErr {
			if err == nil {
				t.Errorf("partitionerFor(%q): expected error, got nil", c.balancer)
			}
			continue
		}
		if err != nil {
			t.Errorf("partitionerFor(%q): unexpected error: %v", c.balancer, err)
			continue
		}
		if fnPtr(got) != fnPtr(c.want) {
			t.Errorf("partitionerFor(%q) 映射错误", c.balancer)
		}
	}
}

func TestAcksFor(t *testing.T) {
	cases := []struct {
		acks    Acks
		want    sarama.RequiredAcks
		wantErr bool
	}{
		{AcksNone, sarama.NoResponse, false},
		{AcksAll, sarama.WaitForAll, false},
		{AcksLocal, sarama.WaitForLocal, false},
		{"", sarama.WaitForLocal, false}, // 留空默认 local
		{"unknown", 0, true},             // 未知值报错（#3 修复后）
	}
	for _, c := range cases {
		got, err := acksFor(c.acks)
		if c.wantErr {
			if err == nil {
				t.Errorf("acksFor(%q): expected error, got nil", c.acks)
			}
			continue
		}
		if err != nil {
			t.Errorf("acksFor(%q): unexpected error: %v", c.acks, err)
			continue
		}
		if got != c.want {
			t.Errorf("acksFor(%q) = %v, want %v", c.acks, got, c.want)
		}
	}
}

func TestInitialOffsetFor(t *testing.T) {
	cases := []struct {
		io      InitialOffset
		want    int64
		wantErr bool
	}{
		{OffsetOldest, sarama.OffsetOldest, false},
		{OffsetNewest, sarama.OffsetNewest, false},
		{"", sarama.OffsetOldest, false}, // 留空默认 oldest
		{"bogus", 0, true},               // 非法值报错
	}
	for _, c := range cases {
		got, err := initialOffsetFor(c.io)
		if c.wantErr {
			if err == nil {
				t.Errorf("initialOffsetFor(%q): expected error, got nil", c.io)
			}
			continue
		}
		if err != nil {
			t.Errorf("initialOffsetFor(%q): unexpected error: %v", c.io, err)
			continue
		}
		if got != c.want {
			t.Errorf("initialOffsetFor(%q) = %d, want %d", c.io, got, c.want)
		}
	}
}

// fakeSCRAMClient 是 buildConfig 测试用的 SCRAM 客户端占位实现。
type fakeSCRAMClient struct{}

func (fakeSCRAMClient) Begin(_, _, _ string) error      { return nil }
func (fakeSCRAMClient) Step(_ string) (string, error)   { return "", nil }
func (fakeSCRAMClient) Done() bool                       { return true }

// fakeTokenProvider 是 buildConfig 测试用的 OAuth token 占位实现。
type fakeTokenProvider struct{}

func (fakeTokenProvider) Token() (*sarama.AccessToken, error) { return &sarama.AccessToken{}, nil }

func TestBuildConfig_NoAuthNoTLS(t *testing.T) {
	cfg, err := buildConfig(SASLConf{}, TLSConf{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Net.SASL.Enable {
		t.Error("SASL 不应启用")
	}
	if cfg.Net.TLS.Enable {
		t.Error("TLS 不应启用")
	}
}

func TestBuildConfig_TLS(t *testing.T) {
	cfg, err := buildConfig(SASLConf{}, TLSConf{Enable: true})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.Net.TLS.Enable {
		t.Error("TLS 应启用")
	}
}

func TestBuildConfig_SASLMechanisms(t *testing.T) {
	scramGen := func() sarama.SCRAMClient { return fakeSCRAMClient{} }

	cases := []struct {
		name    string
		sc      SASLConf
		wantErr bool
		wantMec sarama.SASLMechanism
	}{
		{"plain", SASLConf{Enable: true, Mechanism: "plain", User: "u", Password: "p"}, false, sarama.SASLTypePlaintext},
		{"plain-empty-mechanism", SASLConf{Enable: true, User: "u", Password: "p"}, false, sarama.SASLTypePlaintext},
		{"plain-uppercase", SASLConf{Enable: true, Mechanism: "PLAIN", User: "u", Password: "p"}, false, sarama.SASLTypePlaintext},
		{"scram256-ok", SASLConf{Enable: true, Mechanism: "scram-sha-256", SCRAMClientGenerator: scramGen}, false, sarama.SASLTypeSCRAMSHA256},
		{"scram256-missing-gen", SASLConf{Enable: true, Mechanism: "scram-sha-256"}, true, ""},
		{"scram512-ok", SASLConf{Enable: true, Mechanism: "scram-sha-512", SCRAMClientGenerator: scramGen}, false, sarama.SASLTypeSCRAMSHA512},
		{"scram512-missing-gen", SASLConf{Enable: true, Mechanism: "scram-sha-512"}, true, ""},
		{"oauth-ok", SASLConf{Enable: true, Mechanism: "oauthbearer", TokenProvider: fakeTokenProvider{}}, false, sarama.SASLTypeOAuth},
		{"oauth-missing-provider", SASLConf{Enable: true, Mechanism: "oauthbearer"}, true, ""},
		{"gssapi", SASLConf{Enable: true, Mechanism: "gssapi"}, false, sarama.SASLTypeGSSAPI},
		{"unknown", SASLConf{Enable: true, Mechanism: "bogus"}, true, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			cfg, err := buildConfig(c.sc, TLSConf{})
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if !cfg.Net.SASL.Enable {
				t.Error("SASL 应启用")
			}
			if cfg.Net.SASL.Mechanism != c.wantMec {
				t.Errorf("Mechanism = %v, want %v", cfg.Net.SASL.Mechanism, c.wantMec)
			}
		})
	}
}

func TestFromConsumerMessage(t *testing.T) {
	src := &sarama.ConsumerMessage{
		Topic:     "t",
		Partition: 3,
		Offset:    42,
		Key:       []byte("k"),
		Value:     []byte("v"),
		Headers: []*sarama.RecordHeader{
			{Key: []byte("h1"), Value: []byte("v1")},
		},
	}
	m := fromConsumerMessage(src)
	if m.Topic != "t" || m.Partition != 3 || m.Offset != 42 {
		t.Errorf("基础字段映射错误: %+v", m)
	}
	if string(m.Key) != "k" || string(m.Value) != "v" {
		t.Errorf("Key/Value 映射错误: %+v", m)
	}
	if len(m.Headers) != 1 || m.Headers[0].Key != "h1" || string(m.Headers[0].Value) != "v1" {
		t.Errorf("Headers 映射错误: %+v", m.Headers)
	}
}

func TestFromConsumerMessage_NoHeaders(t *testing.T) {
	m := fromConsumerMessage(&sarama.ConsumerMessage{Topic: "t"})
	if m.Headers != nil {
		t.Errorf("无 header 时应为 nil, got %+v", m.Headers)
	}
}

// recordingLogger 是用于断言"原样返回"的独立 logger 类型。
type recordingLogger struct{ n int }

func (*recordingLogger) Printf(string, ...interface{}) {}

func TestResolveLogger(t *testing.T) {
	if _, ok := resolveLogger(nil).(defaultLogger); !ok {
		t.Error("nil 应回退到 defaultLogger")
	}
	custom := &recordingLogger{}
	if got := resolveLogger(custom); got != custom {
		t.Error("非 nil 应原样返回，而非回退")
	}
}

// 预填一个占位实例（不 Connect），让 Register 的 RLock 检查走拒绝分支，
// 避免测试触及真实 broker。
func TestRegisterProduction_RejectDuplicate(t *testing.T) {
	const name = "test_dup_prod"
	mu.Lock()
	kafkaProduction[name] = &Production{}
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		delete(kafkaProduction, name)
		mu.Unlock()
	})

	if err := RegisterProduction(name, &ProductionConf{Address: "127.0.0.1:1"}); err == nil {
		t.Fatal("expected error when registering duplicate name, got nil")
	}
}

func TestRegisterConsumer_RejectDuplicate(t *testing.T) {
	const name = "test_dup_cons"
	mu.Lock()
	kafkaConsumer[name] = &Consumer{}
	mu.Unlock()
	t.Cleanup(func() {
		mu.Lock()
		delete(kafkaConsumer, name)
		mu.Unlock()
	})

	if err := RegisterConsumer(name, &ConsumerConf{Address: "127.0.0.1:1", GroupId: "g", Topics: []string{"t"}}); err == nil {
		t.Fatal("expected error when registering duplicate name, got nil")
	}
}

func TestCloseProductionByName(t *testing.T) {
	const name = "test_close_prod"

	// 未注册时调用：幂等返回 nil
	if err := CloseProductionByName(name); err != nil {
		t.Errorf("未注册时 CloseProductionByName 应返回 nil, got %v", err)
	}

	mu.Lock()
	kafkaProduction[name] = &Production{}
	mu.Unlock()

	if err := CloseProductionByName(name); err != nil {
		t.Errorf("CloseProductionByName failed: %v", err)
	}
	mu.RLock()
	_, exists := kafkaProduction[name]
	mu.RUnlock()
	if exists {
		t.Error("CloseProductionByName 后注册表里应已无此项")
	}

	// 再次调用：仍幂等
	if err := CloseProductionByName(name); err != nil {
		t.Errorf("重复调用应返回 nil, got %v", err)
	}
}

func TestCloseConsumerByName(t *testing.T) {
	const name = "test_close_cons"

	if err := CloseConsumerByName(name); err != nil {
		t.Errorf("未注册时 CloseConsumerByName 应返回 nil, got %v", err)
	}

	mu.Lock()
	kafkaConsumer[name] = &Consumer{}
	mu.Unlock()

	if err := CloseConsumerByName(name); err != nil {
		t.Errorf("CloseConsumerByName failed: %v", err)
	}
	mu.RLock()
	_, exists := kafkaConsumer[name]
	mu.RUnlock()
	if exists {
		t.Error("CloseConsumerByName 后注册表里应已无此项")
	}

	if err := CloseConsumerByName(name); err != nil {
		t.Errorf("重复调用应返回 nil, got %v", err)
	}
}

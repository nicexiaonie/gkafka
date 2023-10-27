package gkafka

import (
	"context"
	"encoding/json"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

var kafkaProduction map[string]*Production

var kafkaConsumer map[string]*Consumer

func init() {
	kafkaProduction = make(map[string]*Production)
	kafkaConsumer = make(map[string]*Consumer)

}

type ProductionConf struct {
	Ctx     context.Context
	NetWork string
	Address string
	Topic   string
	// 是否开启验证
	SASLEnable bool
	//生产分区生成
	Partitioner string
	// writer

	writer *kafka.Writer
	//消息分发(partition)策略
	//
	// &kafka.Hash{}, -> 算法: hasher.Sum32() % len(partitions)
	// &kafka.Hash{}, -> 算法: (int32(hasher.Sum32()) & 0x7fffffff) % len(partitions) => partition
	Balancer kafka.Balancer
	/*

		综上可知，出现发送 1 条消息耗时 1 秒的情况的原因是：在同步地发送 1 条消息后，
		Writer 等待消息凑成一整批（默认 100 条），但是因为后续没有消息写入，因此它等待到超时（默认 1 秒）后，
		将消息写入到 Kafka，然后才返回。解决方法是：在该场景下，可将 BatchSize 设置为 1，
		需要注意的是，这样做会降低写入性能。
	*/
	// Limit on how many messages will be buffered before being sent to a
	// partition.
	//
	// The default is to use a target batch size of 100 messages.
	BatchSize int
	// Limit the maximum size of a request in bytes before being sent to
	// a partition.
	//
	// The default is to use a kafka default value of 1048576.
	BatchBytes int64
	// Time limit on how often incomplete message batches will be flushed to
	// kafka.
	//
	// The default is to flush at least every second.
	BatchTimeout time.Duration
	// Timeout for read operations performed by the Writer.
	//
	// Defaults to 10 seconds.
	ReadTimeout time.Duration
	// Timeout for write operation performed by the Writer.
	//
	// Defaults to 10 seconds.
	WriteTimeout time.Duration
}

func RegisterProduction(name string, pc *ProductionConf) {
	kafkaProduction[name] = &Production{
		Ctx:          context.Background(),
		NetWork:      pc.NetWork,
		Address:      pc.Address,
		Topic:        pc.Topic,
		Balancer:     pc.Balancer,
		BatchSize:    pc.BatchSize,
		BatchBytes:   pc.BatchBytes,
		BatchTimeout: pc.BatchTimeout,
		WriteTimeout: pc.WriteTimeout,
		RequiredAcks: kafka.RequireOne,
	}
	production := GetProduction(name)
	production.Connect()
	log.Println("RegisterProduction: ", GetProduction(name))

	//log.Println("RegisterProduction: ", production)
	return
}

func GetProduction(name string) *Production {
	return kafkaProduction[name]
}

type ConsumerConf struct {
	Ctx     context.Context
	NetWork string
	Address string
	Topic   string
	GroupId string

	// 指定消费分区
	Partition int

	// MinBytes向代理指示使用者可以使用的最小批处理大小 Default: 1
	MinBytes int
	// MaxBytes向代理指示消费者使用的最大批处理大小 Default: 1MB
	MaxBytes int
	// 获取批处理时等待新数据的最大时间 Default: 10s
	MaxWait time.Duration
	// 从kafka消息批处理中获取消息的等待时间。 Default: 10s
	ReadBatchTimeout time.Duration
	/*
		仅当设置了GroupID时使用
	*/
	// CommitInterval提交偏移量的时间间隔代理。如果为0，提交将被同步处理。 Default: 0
	CommitInterval time.Duration

	CallFunc func(message kafka.Message) error
}

func RegisterConsumer(name string, cc *ConsumerConf) error {
	kafkaConsumer[name] = &Consumer{
		Ctx:              context.Background(),
		NetWork:          cc.NetWork,
		Address:          cc.Address,
		Topic:            cc.Topic,
		GroupId:          cc.GroupId,
		CallFunc:         cc.CallFunc,
		ReadBatchTimeout: cc.ReadBatchTimeout,
		MaxWait:          cc.MaxWait,
		CommitInterval:   cc.CommitInterval,
	}
	c := GetConsumer(name)
	err := c.Connect()
	if err != nil {
		return err
	}
	err = c.CreateReader()
	if err != nil {
		return err
	}
	if cc.MaxWait > 0 {
		c.MaxWait = cc.MaxWait
	}
	return nil
}

func GetConsumer(name string) *Consumer {
	return kafkaConsumer[name]
}

func toJson(v interface{}) string {
	jsons, _ := json.Marshal(v)
	r := string(jsons)
	return r
}

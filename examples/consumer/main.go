package main

import (
	"context"
	"github.com/nicexiaonie/gkafka"
	"log"
	"time"
)

func main() {

	err := gkafka.RegisterConsumer("demo", &gkafka.ConsumerConf{
		Brokers: []string{"localhost:9092"},
		Topics:  []string{"demo_topic"},
		GroupId: "demo_group",
		MaxWait: 1 * time.Second,
		CallFunc: func(ctx context.Context, m gkafka.Message) error {
			log.Printf("message at topic:%s, partition:%d, offset:%d, key:%s, value:%s \n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			return nil
		},
	})
	if err != nil {
		log.Fatalln("RegisterConsumer error: ", err)
	}
	consumer := gkafka.GetConsumer("demo")

	// 自动消费 + 自动提交（成功才提交，至少一次语义）
	if err := consumer.RunCommit(); err != nil {
		log.Println("RunCommit error: ", err)
	}

	consumer.Close()
}

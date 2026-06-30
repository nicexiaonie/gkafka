package main

import (
	"context"
	"github.com/nicexiaonie/gkafka"
	"log"
	"time"
)

func main() {

	_ = gkafka.RegisterConsumer("demo", &gkafka.ConsumerConf{
		Brokers: []string{"10.20.0.200:9092"},
		Topics:  []string{"data_sync_canal_to_clickhouse_mjdb_mlchangch_end_rec"},
		GroupId: "flow_sync",
		MaxWait: 1 * time.Second,
		CallFunc: func(ctx context.Context, m gkafka.Message) error {
			log.Printf("message at topic:%s, partition:%d, offset:%d, key:%s, value:%s \n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			return nil
		},
	})
	consumer := gkafka.GetConsumer("demo")

	// 自动消费 + 自动提交（成功才提交，至少一次语义）
	if err := consumer.RunCommit(); err != nil {
		log.Println("RunCommit error: ", err)
	}

	consumer.Close()
}

func c2() {
	// http://kafka.xingdong.co/
	err := gkafka.RegisterConsumer("demo2", &gkafka.ConsumerConf{
		Brokers: []string{"10.20.0.200:9092"},
		Topics:  []string{"data_sync_canal_to_clickhouse"},
		GroupId: "game_room_server",
		MaxWait: 1 * time.Second,
		CallFunc: func(ctx context.Context, m gkafka.Message) error {
			log.Printf("message at topic:%s, partition:%d, offset:%d, key:%s, value:%s \n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			return nil
		},
	})
	if err != nil {
		log.Println("RegisterConsumer error: ", err)
	}
}

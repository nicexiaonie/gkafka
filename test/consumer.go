package main

import (
	"fmt"
	"github.com/nicexiaonie/gkafka"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

func main() {

	_ = gkafka.RegisterConsumer("demo", &gkafka.ConsumerConf{
		NetWork:          "tcp",
		Address:          "10.20.0.200:9092",
		Topic:            "data_sync_canal_to_clickhouse_mjdb_mlchangch_end_rec",
		GroupId:          "flow_sync",
		MaxWait:          1 * time.Second,
		ReadBatchTimeout: 1 * time.Second,
		CallFunc: func(m kafka.Message) error {
			log.Printf("message at topic:%s, partition:%d, offset:%d, key:%s, value:%s \n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			return nil
		},
	})
	consumer := gkafka.GetConsumer("demo")

	list, _ := consumer.FetchMessage(10)

	for _, v := range list {

		fmt.Println(v.Offset)
		consumer.CommitMessage(v)
	}
	consumer.Close()
}

func c2() {
	// http://kafka.xingdong.co/
	err := gkafka.RegisterConsumer("demo2", &gkafka.ConsumerConf{
		NetWork: "tcp",
		Address: "10.20.0.200:9092",
		Topic:   "data_sync_canal_to_clickhouse",
		//GroupId:          "game_room_server",
		MaxWait:          1 * time.Second,
		ReadBatchTimeout: 1 * time.Second,
		CallFunc: func(m kafka.Message) error {
			log.Printf("message at topic:%s, partition:%d, offset:%d, key:%s, value:%s \n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			return nil
		},
	})
	fmt.Println(err)
	//consumer := gkafka.GetConsumer("demo2")

	// 当启用RunCommit方法消费时，根据返回error进行自动commit offset
	//err := consumer.RunCommit()
	//if err != nil {
	//    log.Println("RunCommit error: ", err)
	//    return
	//}
}

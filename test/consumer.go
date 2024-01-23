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
		Topic:            "data_sync_canal_to_clickhouse_mjdb_changch_user_blocks",
		GroupId:          "flow_sync",
		MaxWait:          1 * time.Second,
		ReadBatchTimeout: 1 * time.Second,
		CallFunc: func(m kafka.Message) error {
			log.Printf("message at topic:%s, partition:%d, offset:%d, key:%s, value:%s \n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			return nil
		},
	})
	consumer := gkafka.GetConsumer("demo")
	list := make([]*kafka.Message, 0)
	_ = consumer.FetchMessage(list, 10000000, time.Second*3)

	fmt.Println(len(list))
	//for _, v := range list {
	//
	//	fmt.Println(v.Offset)
	//	fmt.Println(v.Value)
	//}
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

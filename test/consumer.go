package main

import (
	"fmt"
	"github.com/nicexiaonie/gkafka"
	"github.com/segmentio/kafka-go"
	"log"
	"sync"
	"time"
)

func main() {

	_ = gkafka.RegisterConsumer("demo", &gkafka.ConsumerConf{
		NetWork:          "tcp",
		Address:          "10.20.0.200:9092",
		Topic:            "data_sync_canal_to_clickhouse",
		GroupId:          "game_room_server",
		MaxWait:          1 * time.Second,
		ReadBatchTimeout: 1 * time.Second,
		CallFunc: func(m kafka.Message) error {
			log.Printf("message at topic:%s, partition:%d, offset:%d, key:%s, value:%s \n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			return nil
		},
	})
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		consumer := gkafka.GetConsumer("demo")
		err := consumer.RunCommit()
		if err != nil {
			log.Println("RunCommit error: ", err)
			return
		}
	}()
	go c2()

	wg.Wait()

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

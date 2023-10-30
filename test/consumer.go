package main

import (
	"github.com/segmentio/kafka-go"
	"gkafka"
	"log"
	"sync"
	"time"
)

func main() {

	_ = gkafka.RegisterConsumer("demo", &gkafka.ConsumerConf{
		NetWork:          "tcp",
		Address:          "192.168.0.46:9092",
		Topic:            "old_hy_club_card_rec_fuxin",
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

	_ = gkafka.RegisterConsumer("demo2", &gkafka.ConsumerConf{
		NetWork:          "tcp",
		Address:          "192.168.0.46:9092",
		Topic:            "test",
		GroupId:          "game_room_server",
		MaxWait:          1 * time.Second,
		ReadBatchTimeout: 1 * time.Second,
		CallFunc: func(m kafka.Message) error {
			log.Printf("message at topic:%s, partition:%d, offset:%d, key:%s, value:%s \n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
			return nil
		},
	})

	consumer := gkafka.GetConsumer("demo2")

	// 当启用RunCommit方法消费时，根据返回error进行自动commit offset
	err := consumer.RunCommit()
	if err != nil {
		log.Println("RunCommit error: ", err)
		return
	}
}

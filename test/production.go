package main

import (
	"github.com/nicexiaonie/gkafka"
	"github.com/segmentio/kafka-go"
)

func main() {
	gkafka.RegisterProduction("demo", &gkafka.ProductionConf{
		NetWork: "tcp",
		Address: "10.20.0.200:9092",
		// 如果不配置topic
		Topic:     "data_sync_canal_to_clickhouse",
		BatchSize: 1,
		//消息分发(partition)策略
		//
		// &kafka.Hash{}, -> 算法: hasher.Sum32() % len(partitions)
		// &kafka.ReferenceHash{}, -> 算法: (int32(hasher.Sum32()) & 0x7fffffff) % len(partitions) => partition
		// &kafka.RoundRobin{}, -> 算法: 轮询分区
		// &kafka.LeastBytes{}, -> 算法: 指定分区的balancer模式为最小字节分布
		Balancer: &kafka.LeastBytes{},
	})

	production := gkafka.GetProduction("demo")
	//t1 := kafka.TopicConfig{
	//	Topic:             "data_sync_canal_to_clickhouse_test",
	//	NumPartitions:     1,
	//	ReplicationFactor: 1,
	//}
	//
	//err := production.GetConn().CreateTopics(t1)
	//fmt.Println(err)

	// 默认发送配置的topic
	//production.Send("1111", "11111")

	// 发送默认topic 指定分区
	//production.SendPartition(2, "1111", "11111")

	// 可以指定topic发送
	production.SendTopic("data_sync_canal_to_clickhouse", "test", "test")

}

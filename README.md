# 一个基于kafka-go封装的好用工具 有详细注解

Production 生产

```go


gkafka.RegisterProduction("demo", &gkafka.ProductionConf{
	NetWork: "tcp",
	Address: "192.168.0.46:9092",
	// 如果不配置topic
	Topic:     "old_hy_club_card_rec_fuxin",
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

// 默认发送配置的topic
production.Send("1111", "11111")

// 发送默认topic 指定分区
production.SendPartition(2, "1111", "11111")

// 可以指定topic发送
production.SendTopic("test", "test", "test")

```

Consumer 消费

```go

_ = gkafka.RegisterConsumer("demo2", &gkafka.ConsumerConf{
	NetWork:          "tcp",
	Address:          "192.168.0.46:9092",
	Topic:            "test",
	GroupId:          "game_room_server",
	MaxWait:          1 * time.Second,
	ReadBatchTimeout: 1 * time.Second,
	CallFunc: func(m kafka.Message) error {
                // 当启用RunCommit方法消费时，根据返回error进行自动commit offset
                // 返回错误时会自动重试，超过3次后间隔时间1s  超过100次后间隔时间5s
		log.Printf("message at topic:%s, partition:%d, offset:%d, key:%s, value:%s \n", m.Topic, m.Partition, m.Offset, string(m.Key), string(m.Value))
		return nil
	},
})

consumer := gkafka.GetConsumer("demo2")

// 不需要关注kafka消费的分区平衡问题，会自己处理
// 当启用RunCommit方法消费时，根据返回error进行自动commit offset
err := consumer.RunCommit()
if err != nil {
	log.Println("RunCommit error: ", err)
	return
}
```

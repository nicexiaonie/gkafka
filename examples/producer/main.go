package main

import (
	"context"

	"github.com/nicexiaonie/gkafka"
)

func main() {
	err := gkafka.RegisterProduction("demo", &gkafka.ProductionConf{
		Brokers: []string{"localhost:9092"},
		// 如果不配置topic
		Topic:     "demo_topic",
		BatchSize: 1,
		// 分区策略（推荐用 Balancer* 常量，也可写等价字符串）：
		//   BalancerHash / BalancerReferenceHash / BalancerRoundRobin / BalancerRandom / BalancerManual
		Balancer: gkafka.BalancerRoundRobin,
	})
	if err != nil {
		panic(err)
	}

	production := gkafka.GetProduction("demo")

	ctx := context.Background()

	// 默认发送配置的topic
	//production.Send(ctx, "1111", "11111")

	// 发送默认topic 指定分区
	//production.SendPartition(ctx, 2, "1111", "11111")

	// 可以指定topic发送
	production.SendTopic(ctx, "demo_topic_other", "test", "test")

	_ = production.Close()
}

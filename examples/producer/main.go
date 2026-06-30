package main

import (
	"github.com/nicexiaonie/gkafka"
)

func main() {

	/*
	   network: tcp
	   address: 10.20.0.200:9092
	   group_id: flow_sync

	*/
	err := gkafka.RegisterProduction("demo", &gkafka.ProductionConf{
		Brokers: []string{"10.20.0.200:9092"},
		// 如果不配置topic
		Topic:     "data_sync_canal_to_clickhouse_mjdb_mlchangch_end_rec",
		BatchSize: 1,
		// 分区策略（推荐用 Balancer* 常量，也可写等价字符串）：
		//   BalancerHash / BalancerReferenceHash / BalancerRoundRobin / BalancerRandom / BalancerManual
		Balancer: gkafka.BalancerRoundRobin,
	})
	if err != nil {
		panic(err)
	}

	production := gkafka.GetProduction("demo")

	// 默认发送配置的topic
	//production.Send("1111", "11111")

	// 发送默认topic 指定分区
	//production.SendPartition(2, "1111", "11111")

	// 可以指定topic发送
	production.SendTopic("data_sync_canal_to_clickhouse", "test", "test")

	_ = production.Close()
}

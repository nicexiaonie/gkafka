package main

import (
	"gkafka"
)

func main() {
	gkafka.RegisterProduction("demo", &gkafka.ProductionConf{
		NetWork:   "tcp",
		Address:   "192.168.0.46:9092",
		Topic:     "old_hy_club_card_rec_fuxin",
		BatchSize: 1,
	})

	production := gkafka.GetProduction("demo")

	production.Send("11111", "11111")
	production.Send("22222", "22222")
	production.Send("333333", "33333")
	production.Send("444444", "44444")

}

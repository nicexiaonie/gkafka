/*
1、在使用消费者组时会有以下限制：
	(*Reader).SetOffset 当设置了GroupID时会返回错误
	(*Reader).Offset 当设置了GroupID时会永远返回 -1
	(*Reader).Lag 当设置了GroupID时会永远返回 -1
	(*Reader).ReadLag 当设置了GroupID时会返回错误
	(*Reader).Stats 当设置了GroupID时会返回一个-1的分区
*/

package gkafka

import "C"
import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type Consumer struct {
	Ctx       context.Context
	conn      *kafka.Conn
	Reader    *kafka.Reader
	NetWork   string
	Address   string
	Topic     string
	GroupId   string
	Partition int
	MaxBytes  int

	MaxWait          time.Duration
	CommitInterval   time.Duration
	ReadBatchTimeout time.Duration

	CallFunc func(message kafka.Message) error
}

func (c *Consumer) Connect() error {
	conn, err := kafka.Dial(c.NetWork, c.Address)
	if err != nil {
		return err
	}
	c.conn = conn

	return nil
}

func (c *Consumer) CreateReader() error {
	kr := kafka.ReaderConfig{
		Brokers: []string{c.Address},
		Topic:   c.Topic,
	}
	if c.MaxWait > 0 {
		kr.MaxWait = c.MaxWait
	}
	if len(c.GroupId) > 0 {
		log.Println(c.GroupId)
		//kr.GroupID = c.GroupId
	}
	if c.ReadBatchTimeout > 0 {
		kr.ReadBatchTimeout = c.ReadBatchTimeout
	}

	c.Reader = kafka.NewReader(kr)
	return nil
}

func (c *Consumer) RunCommit() error {
	if c.CallFunc == nil {
		return errors.New("call not is null")
	}
	for {
		m, err := c.Reader.FetchMessage(c.Ctx)
		if err != nil {
			break
		}
		i := 0
		for {
			i++
			e := c.CallFunc(m)
			if e != nil {
				if i > 100 {
					time.Sleep(5 * time.Second)
					continue
				}
				if i > 3 {
					time.Sleep(1 * time.Second)
					continue
				}
				continue
			}
			break
		}

		_ = c.Reader.CommitMessages(c.Ctx, m)
	}
	return nil
}

func (c *Consumer) Close() error {
	err := c.Reader.Close()
	if err != nil {
		return err
	}

	err = c.conn.Close()
	if err != nil {
		return err
	}
	return nil

}

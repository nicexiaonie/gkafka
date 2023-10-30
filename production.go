package gkafka

import (
	"context"
	"errors"
	"github.com/segmentio/kafka-go"
	"log"
	"time"
)

type Production struct {
	conn    *kafka.Conn
	Ctx     context.Context
	NetWork string
	Address string
	Topic   string
	// 是否开启验证
	SASLEnable bool
	//生产分区生成
	Partitioner string

	// writer
	writer   *kafka.Writer
	Balancer kafka.Balancer //消息分发(partition)策略

	/*

		综上可知，出现发送 1 条消息耗时 1 秒的情况的原因是：在同步地发送 1 条消息后，
		Writer 等待消息凑成一整批（默认 100 条），但是因为后续没有消息写入，因此它等待到超时（默认 1 秒）后，
		将消息写入到 Kafka，然后才返回。解决方法是：在该场景下，可将 BatchSize 设置为 1，
		需要注意的是，这样做会降低写入性能。
	*/
	BatchSize    int
	BatchBytes   int64
	BatchTimeout time.Duration
	ReadTimeout  time.Duration
	WriteTimeout time.Duration
	RequiredAcks kafka.RequiredAcks
}

func (p *Production) Connect() {
	if len(p.NetWork) <= 0 {
		p.NetWork = "tcp"
	}
	p.writer = &kafka.Writer{
		Addr:         kafka.TCP(p.Address),
		BatchSize:    p.BatchSize,
		BatchTimeout: p.BatchTimeout,
		WriteTimeout: p.WriteTimeout,
	}
	// 支持写入分区平衡器
	if p.Balancer != nil {
		p.writer.Balancer = p.Balancer
	}
	log.Println("Connect: ", p)

}

func (p *Production) Send(key, value string) error {
	if len(p.Topic) <= 0 {
		return errors.New("topic not is nil")
	}
	err := p.sendMessage(kafka.Message{
		Topic: p.Topic,
		Key:   []byte(key),
		Value: []byte(value),
	})
	if err != nil {
		return err
	}
	return nil
}

func (p *Production) SendPartition(part int, key, value string) error {
	if len(p.Topic) <= 0 {
		return errors.New("topic not is nil")
	}
	err := p.sendMessage(kafka.Message{
		Topic:     p.Topic,
		Partition: part,
		Key:       []byte(key),
		Value:     []byte(value),
	})
	if err != nil {
		return err
	}
	return nil
}

func (p *Production) SendTopic(topic, key, value string) error {
	err := p.sendMessage(kafka.Message{
		Topic: topic,
		Key:   []byte(key),
		Value: []byte(value),
	})
	if err != nil {
		return err
	}
	return nil
}

func (p *Production) sendMessage(message kafka.Message) error {
	err := p.writer.WriteMessages(p.Ctx, message)
	if err != nil {
		log.Println("err: ", err)
		return err
	}
	return nil
}

func (p *Production) sendMessageBatch(message ...kafka.Message) error {
	err := p.writer.WriteMessages(p.Ctx, message...)
	if err != nil {
		return err
	}
	return nil
}

func (p *Production) Close(message ...kafka.Message) error {
	if p.writer != nil {
		err := p.writer.Close()
		if err != nil {
			return err
		}
	}
	if p.conn != nil {
		err := p.conn.Close()
		if err != nil {
			return err
		}
	}
	return nil

}

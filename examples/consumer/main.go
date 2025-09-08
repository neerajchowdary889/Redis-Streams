package main

import (
	RSconfig "RedisStreams/Config"
	mq "RedisStreams/QueueModule"
	"context"
	"fmt"
)

func handler(ctx context.Context, topic string, msgID string, fields map[string]interface{}) error {
	fmt.Println("received:", topic, msgID, fields)
	return nil
}

func main() {
	config, err := RSconfig.LoadConfigFromPath("config/order-service.yml")
	if err != nil {
		panic(err)
	}

	client, err := mq.New(config, nil)
	if err != nil {
		panic(err)
	}
	defer client.Close()

	err = client.Subscribe(mq.ConsumerConfig{
		TopicName:    "order.created",
		ConsumerName: "example-consumer-1",
		AutoAck:      true,
	}, handler)
	if err != nil {
		panic(err)
	}

	select {}
}

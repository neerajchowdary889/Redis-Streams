package main

import (
	RSconfig "RedisStreams/Config"
	mq "RedisStreams/QueueModule"
	"fmt"
)

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

	id, err := client.Publish("order.created", map[string]interface{}{"order_id": "123", "amount": 99.99})
	if err != nil {
		panic(err)
	}
	fmt.Println("published id:", id)
}

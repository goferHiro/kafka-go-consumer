package kafka

import (
	"context"
	"fmt"

	"github.com/segmentio/kafka-go"
)

func StartKafkaLocal(topic string, cGID string) {

	broker1 := "localhost:9092"

	conf := kafka.ReaderConfig{
		Brokers:  []string{broker1},
		Topic:    topic,
		GroupID:  cGID,
		MaxBytes: 18,
	}

	reader := kafka.NewReader(conf)
	for {
		m, err := reader.ReadMessage(context.Background())
		if err != nil {
			fmt.Println("Some err ", err)
			continue
		}
		fmt.Println("message is ", string(m.Value))
	}
}

func mainLocal() {
	topic := "kafka-go"
	cGID := "g1"

	fmt.Println("Consumer is being started!")
	defer fmt.Println("Consumer is stopped!")

	StartKafkaLocal(topic, cGID)
}

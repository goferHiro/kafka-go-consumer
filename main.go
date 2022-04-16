package main

import (
	"fmt"
	appKafka "mykafka/kafka"
	"time"
)

func main() {
	topic := "kafka-go"
	cGID := "g1"
	fmt.Println("Consumer is being started!")
	defer fmt.Println("Consumer is stopped!")
	appKafka.StartKafka(topic, cGID)
	runtime := 10 * time.Minute
	time.Sleep(runtime)
}

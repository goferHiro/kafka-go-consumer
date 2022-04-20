package main

import (
	"fmt"
	appKafka "mykafka/kafka"
	webSocket "mykafka/websocket"
	"net/http"
	"time"
)

func handler(w http.ResponseWriter, r *http.Request) {
	greet := `
  <h1> Welcome to KafkaConsumerSocket</h1>
  <p> Websocket @ wss://kafka-consumer.goferhiro.repl.co/v1/ws </p>`
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprint(w, greet)
}

func main() {
	fmt.Println("Starting Websocket")
	http.HandleFunc("/", handler)
	http.HandleFunc("/v1/ws", webSocket.SocketHandler)
	go func() {
		err := http.ListenAndServe(":8080", nil)
		if err != nil {
			panic(err)
		}
	}()

	topic := "kafka-go"
	cGID := "g1"
	fmt.Println("Consumer is being started!")
	defer fmt.Println("Consumer is stopped!")
	appKafka.StartKafka(topic, cGID)
	runtime := 10 * time.Minute
	time.Sleep(runtime)
}

package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
)

var ReadChannel = make(chan []byte,100000) //unbuffereed channel needs a receiver as soon as msg is send

func newReader(url, topic string, dialer *kafka.Dialer) *kafka.Reader {

	return kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{url},
		Topic:   topic,
		Dialer:  dialer,
	})
}

func newDialer(clientID, username, password string) *kafka.Dialer {
	mechanism := plain.Mechanism{
		Username: username,
		Password: password,
	}

	rootCAs, _ := x509.SystemCertPool()
	if rootCAs == nil {
		rootCAs = x509.NewCertPool()
	}

	return &kafka.Dialer{
		Timeout:       10 * time.Second,
		DualStack:     true,
		ClientID:      clientID,
		SASLMechanism: mechanism,
		TLS: &tls.Config{
			InsecureSkipVerify: false,
			RootCAs:            rootCAs,
		},
	}
}

func read(url, topic string, dialer *kafka.Dialer) {

	reader := newReader(url, topic, dialer)
	defer reader.Close()
	for {
		msg, err := reader.ReadMessage(context.Background())
		if try(err, nil) {
			log.Printf("rec%d:\t%s\n", msg.Value, msg.Value)
			ReadChannel <- msg.Value
		}
	}
	// close(ReadChannel)
}

func StartKafka(topic, cGID string) {

	username := "YGNDSDHF6KGOMJQX"
	password := "eWt0ut+fpGlffp+5leVRlHDAco87z10WhwUnq9uUmVgsUDY3MbQnFCQ++2j/reiq"

	dialer := newDialer(cGID, username, password)
	url := "pkc-ymrq7.us-east-2.aws.confluent.cloud:9092"

	read(url, topic, dialer)
}

func try(err error, errorHandler func(s string)) bool {
	if err == nil {
		return true
	}
	if errorHandler == nil {
		panic(err.Error())
	}
	errorHandler(string(err.Error()))
	return false
}

func main() {
	topic := "kafka-go"
	cGID := "g1"
	fmt.Println("Consumer is being started!")
	defer fmt.Println("Consumer is stopped!")
	StartKafka(topic, cGID)
	runtime := 10 * time.Minute
	time.Sleep(runtime)
}

package main

import (
	kafka "github.com/segmentio/kafka-go"
	"context"
	"fmt"
	"time"
	"strconv"
	"math/rand"
)

const (
	broker string = "localhost:9092"
	topic string = "cp-go-1"
)

func main() {
	kafkachan := make(chan string, 1)
	defer close(kafkachan)
	go fout(kafkachan)
	for i := 1; i <= 5; i++ {
		go consume(kafkachan, i)
	}
	produce()
}

func consume(kafkachan chan string, gid int) {
	for {
		rand.Seed(time.Now().UnixNano())
		pt := rand.Intn(5) + 5
		time.Sleep(time.Second * time.Duration(pt))
		message := <-kafkachan
		fmt.Println("Consumer:", gid, "was processing for", pt, "seconds. \tMessage:", message)
	}
}

func fout(kafkachan chan string) {
	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic: topic,
		GroupID: "def-gr",
	})

	for {
		msg, err := consumer.ReadMessage(context.Background())
		if err != nil {
			panic("Consumer can not read messages " + err.Error())
		}
		kafkachan <- string(msg.Value)
	}
}

func produce() {
	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic: topic,
	})

	for {
		err := producer.WriteMessages(context.Background(), kafka.Message{
			Key: []byte("0"),
			Value: []byte("Hello World " + strconv.Itoa(int(time.Now().Unix()))),
		})
	
		if err != nil {
			panic("Can not produce message. " + err.Error())
		}	
	}
}
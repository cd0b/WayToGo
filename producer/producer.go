package main

import (
	kafka "github.com/segmentio/kafka-go"
	"context"
	"strconv"
	"time"
	"os"
	"os/signal"
	"syscall"
	"fmt"
)

const (
	broker string = "localhost:9092"
	topic  string = "cp-go-1"
)

func main() {
	sigchan := createSigChan()
	defer close(sigchan)
	producer := kafka.NewWriter(kafka.WriterConfig{
		Brokers: []string{broker},
		Topic: topic,
	})

	outloop:
	for {
		select {
		case <-sigchan:
			break outloop
		default:
			writeHelloWorld(producer)
		}
	}

	err := producer.Close()
	if err != nil {
		panic("Can not close producer! " + err.Error())
	}
}

func createSigChan() chan os.Signal {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	return sigchan
}

func writeHelloWorld(producer *kafka.Writer) {
	message := "Hello World " + strconv.Itoa(int(time.Now().Unix()))
	err := producer.WriteMessages(context.Background(), kafka.Message{
		Key: []byte("0"),
		Value: []byte(message),
	})

	if err != nil {
		panic("Can not produce message. " + err.Error())
	}

	fmt.Println("Produced message:", message)
}
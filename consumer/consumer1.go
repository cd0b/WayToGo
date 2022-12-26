package main

import (
	kafka "github.com/segmentio/kafka-go"
	"context"
	"fmt"
	"time"
	"math/rand"
	"sync"
	"os"
	"os/signal"
	"syscall"
)

const (
	broker string = "localhost:9092"
	topic string = "cp-go-1"
)

func main() {
	sigchan := createSigChan()
	defer close(sigchan)

	consumer := kafka.NewReader(kafka.ReaderConfig{
		Brokers: []string{broker},
		Topic: topic,
		GroupID: "def-gr",
	})

	wg := sync.WaitGroup{}

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func(gid int) {
			defer wg.Done()

			for {
				select {
				case sig := <-sigchan:
					sigchan <- sig
					return
				default:
					if err := readHelloWorld(gid, consumer); err != nil {
						panic("Consumer can not read messages " + err.Error())
					}
				}
			}
		}(i)
	}

	wg.Wait()
	if err := consumer.Close(); err != nil {
		panic("Can not close consumer! " + err.Error())
	}
}

func createSigChan() chan os.Signal {
	sigchan := make(chan os.Signal, 1)
	signal.Notify(sigchan, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	return sigchan
}

func readHelloWorld(gid int, consumer *kafka.Reader) error {
	msg, err := consumer.ReadMessage(context.Background())
	if err != nil {
		return err
	}
	pt := randInt(5, 10)
	time.Sleep(time.Second * time.Duration(pt))
	fmt.Println("Consumer:", gid, "was processing for", pt, "seconds. \tMessage:", string(msg.Value))
	return nil
}

func randInt(min, max int) int {
	rand.Seed(time.Now().UnixNano())
	pt := rand.Intn(max - min) + min
	return pt
}
package main

import (
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"
)

var (
	brokers = flag.String("brokers",
		"127.0.0.1:9001,127.0.0.1:9101,127.0.0.1:9201",
		"The Kafka brokers to connect to, as a comma separated list",
	)
	topic  = "booking1"
	profit = 0.0
)

func main() {
	flag.Parse()
	config := sarama.NewConfig()
	config.Consumer.Return.Errors = true

	brokerList := strings.Split(*brokers, ",")
	fmt.Printf("Broker list: %s\n", brokerList)
	master, err := sarama.NewConsumer(brokerList, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := master.Close(); err != nil {
			panic(err)
		}
	}()

	consumer, err := master.ConsumePartition(topic, 0, sarama.OffsetOldest)
	if err != nil {
		panic(err)
	}

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	fmt.Println("++++++++++++++++++++++++++++++++++++++++++")
	fmt.Println("------------ PROFIT CALCULATOR -----------")
	fmt.Println("++++++++++++++++++++++++++++++++++++++++++")

	// Get signal for finish
	doneCh := make(chan struct{})
	go func() {
		for {
			select {
			case err := <-consumer.Errors():
				fmt.Println(err)
			case msg := <-consumer.Messages():
				s := strings.Split(string(msg.Value), " ")

				if len(s) == 3 {
					id := s[0]
					status := s[1]
					price, err := strconv.ParseFloat(s[2], 64)

					if err == nil {
						if status == "paid" {
							profit += price
						} else if status == "cancelled" {
							profit -= price
						}
					}
					fmt.Println("Profit ", strconv.FormatFloat(profit, 'f', 2, 64), " from: ", string(id), " - ", string(status), " - ", price)
				} else {
					fmt.Println("Ignoring message: ", s)
				}

			case <-signals:
				fmt.Println("Interrupt is detected")
				doneCh <- struct{}{}
			}
		}
	}()

	<-doneCh
}

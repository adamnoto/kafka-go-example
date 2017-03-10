package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
)

var (
	brokers = flag.String(
		"brokers",
		"127.0.0.1:9001,127.0.0.1:9101,127.0.0.1:9201",
		"The Kafka brokers to connect to, as a comma separated list",
	)
	topic = "booking1"
)

func main() {
	flag.Parse()

	brokerList := strings.Split(*brokers, ",")
	fmt.Printf("Broker list: %s\n", brokerList)

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.ChannelBufferSize = 1
	config.Version = sarama.V0_10_0_1

	producer, err := sarama.NewSyncProducer(brokerList, config)
	if err != nil {
		panic(err)
	}

	defer func() {
		if err := producer.Close(); err != nil {
			panic(err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("----------------------------------------")
	fmt.Println("--------- BOOKING PRODUCER -------------")
	fmt.Println("----------------------------------------")
	fmt.Println("Enter in format of: ID status price\n")

	for {
		fmt.Print("-> ")
		text, _ := reader.ReadString('\n')
		text = strings.Replace(text, "\n", "", -1)
		if text == "end" {
			break
		} else if text == "seed" {
			seed(producer)
		} else {
			sendMessage(producer, text)
		}
	}

	log.Printf("Booking producer terminated")
}

func sendMessage(producer sarama.SyncProducer, text string) {
	msg := &sarama.ProducerMessage{
		Topic: topic,
		Value: sarama.StringEncoder(text),
	}

	partition, offset, err := producer.SendMessage(msg)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Message is stored in topic(%s)/partition(%d)/offset(%d)\n",
		topic, partition, offset)
}

func seed(producer sarama.SyncProducer) {
	statuses := []string{"paid", "reserved"}
	for i := 0; i < 10; i++ {
		price := 1000 * rand.Intn(999)
		id := strconv.FormatInt(time.Now().UnixNano()/int64(time.Millisecond), 10)
		status := statuses[rand.Intn(len(statuses))]

		msg := fmt.Sprintf("%s %s %d", id, status, price)
		sendMessage(producer, msg)
	}
}

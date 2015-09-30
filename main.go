package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"os"
	"os/signal"
)

func main() {
	producerFlag := flag.Bool("producer", false, "Create a Kaffa Producer")
	consumerFlag := flag.Bool("consumer", false, "Create a Kafka Consumer")

	flag.Parse()

	if (*producerFlag && *consumerFlag) || (!*producerFlag && !*consumerFlag) {
		fmt.Println("Please specify either producer or consumer")
		os.Exit(1)
	}

	if *producerFlag {
		startProducer()
	} else if *consumerFlag {
		startConsumer()
	}
}

func startProducer() {
	fmt.Println("Starting...")

	config := sarama.NewConfig()

	ip := os.Getenv("KAFKA")

	producer, err := sarama.NewAsyncProducer([]string{ip}, config)
	if err != nil {
		panic(err)
	}

	fmt.Println("Producer created!")

	defer func() {
		if err := producer.Close(); err != nil {
			fmt.Printf("\nFailed to close producer: %v", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enq, errors int

ProducerLoop:
	for {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: "my_topic", Key: nil, Value: sarama.StringEncoder("This Is Just A Test!")}:
			enq++
		case err := <-producer.Errors():
			fmt.Printf("\nFailed to produce message: %v", err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	fmt.Printf("\nEnq: %d, errors: %d", enq, errors)
}

func startConsumer() {
	fmt.Println("Starting consumer...")

	topic := "my_topic"
	partition := int32(0)
	offset := int64(0)

	config := sarama.NewConfig()

	ip := os.Getenv("KAFKA")
	consumer, err := sarama.NewConsumer([]string{ip}, config)
	if err != nil {
		panic(err)
	}

	fmt.Println("Consumer created!")

	defer func() {
		if err := consumer.Close(); err != nil {
			fmt.Printf("\nFailed to close consumer: %v", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	fmt.Printf("\nConsuming topic: %v  partition: %v  offset:  %v", topic, partition, offset)
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		fmt.Printf("Failed to get partitionConsumer: %v", err)
		return
	}

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			fmt.Printf("\nGOT MESSAGE: %v", msg)
		case err := <-partitionConsumer.Errors():
			fmt.Printf("\n>>>AN ERROR OCCURRED: %v", err)
		case <-signals:
			break ConsumerLoop
		}
	}

	fmt.Println("Done Consuming.")
}

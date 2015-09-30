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
	verboseFlag := flag.Bool("verbose", false, "Print consumer messages")

	flag.Parse()

	if (*producerFlag && *consumerFlag) || (!*producerFlag && !*consumerFlag) {
		fmt.Println("Please specify either producer or consumer")
		os.Exit(1)
	}

	if *producerFlag {
		startProducer()
	} else if *consumerFlag {
		startConsumer(*verboseFlag)
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
			fmt.Println("Failed to close producer: ", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	var enq, errors int

ProducerLoop:
	for {
		select {
		case producer.Input() <- &sarama.ProducerMessage{Topic: "my_topic", Key: nil, Value: sarama.StringEncoder("SECOND TEST")}:
			enq++
		case err := <-producer.Errors():
			fmt.Println("Failed to produce message: ", err)
			errors++
		case <-signals:
			break ProducerLoop
		}
	}

	fmt.Printf("Enq: %d, errors: %d\n", enq, errors)
	fmt.Println("Done producing.")
}

func startConsumer(verbose bool) {
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
			fmt.Println("\nFailed to close consumer: ", err)
		}
	}()

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt)

	fmt.Printf("Consuming  topic: %v  partition: %v  offset:  %v\n", topic, partition, offset)
	partitionConsumer, err := consumer.ConsumePartition(topic, partition, offset)
	if err != nil {
		fmt.Println("Failed to get partitionConsumer: ", err)
		return
	}

	var consumed, errors int

ConsumerLoop:
	for {
		select {
		case msg := <-partitionConsumer.Messages():
			if verbose {
				fmt.Println(">>>GOT MESSAGE")
				fmt.Println("\tTopic: ", msg.Topic)
				fmt.Println("\tPartition: ", msg.Partition)
				fmt.Println("\tOffset: ", msg.Offset)
				fmt.Println("\tKey: ", string(msg.Key))
				fmt.Println("\tValue: ", string(msg.Value))
			}
			consumed++
		case err := <-partitionConsumer.Errors():
			if verbose {
				fmt.Println("<<<AN ERROR OCCURRED: ", err)
			}
			errors++
		case <-signals:
			break ConsumerLoop
		}
	}

	fmt.Printf("Consumed: %d, errors: %d\n", consumed, errors)
	fmt.Println("Done Consuming.")
}

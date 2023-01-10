package cmd

import (
	"log"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Producer(config *kafka.ConfigMap) {
	producer, err := kafka.NewProducer(config)
	if err != nil {
		log.Printf("Failed to create producer: %s\n", err)
		os.Exit(1)
	}

	log.Printf("Created Producer\n")

	go func(drs chan kafka.Event) {
		for ev := range drs {
			msg, ok := ev.(*kafka.Message)
			if !ok {
				continue
			}
			if msg.TopicPartition.Error != nil {
				log.Printf("%% Delivery error: %v\n", msg.TopicPartition)
			} else if Verbosity >= 2 {
				log.Printf("%% Delivered %v\n", msg)
			}
		}
	}(producer.Events())

	run := true

	for run {
		select {
		case sig := <-SignalChannel:
			log.Printf("%% Terminating on signal %v\n", sig)
			run = false

		case value, ok := <-ProducerChannel:
			if !ok {
				run = false
				break
			}

			if err := producer.Produce(&value, nil); err != nil {
				log.Printf("%% Produce error: %v\n", err)
			}
		}
	}

	log.Printf("%% Flushing %d message(s)\n", producer.Len())
	producer.Flush(10000)

	log.Printf("%% Closing\n")
	producer.Close()
}

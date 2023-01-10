package cmd

import (
	"encoding/json"
	"log"
	"os"
	"strings"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

func Consumer(config *kafka.ConfigMap, topics string) {
	consumer, err := kafka.NewConsumer(config)

	if err != nil {
		log.Printf("Failed to create consumer: %s\n", err)
		os.Exit(1)
	}

	log.Printf("Created Consumer %v\n", consumer)

	err = consumer.SubscribeTopics(strings.Split(topics, ","), nil)
	if err != nil {
		log.Panicln(err)
	}

	run := true

	for run {
		select {
		case sig := <-SignalChannel:
			log.Printf("Caught signal %v: terminating\n", sig)
			run = false
		default:
			events := consumer.Poll(100)
			if events == nil {
				continue
			}
			switch event := events.(type) {
			case *kafka.Message:
				log.Printf("%% Message on %s:\n%s\n", event.TopicPartition, string(event.Value))
				if event.Headers != nil {
					log.Printf("%% Headers: %v\n", event.Headers)
				}

				topic := "123"
				tp := kafka.TopicPartition{Topic: &topic}

				ProducerChannel <- kafka.Message{
					TopicPartition: tp,
					Value:          event.Value,
					Key:            event.Key,
				}
			case kafka.Error:
				log.Printf("%% Error: %v: %v\n", event.Code(), event)
				if event.Code() == kafka.ErrAllBrokersDown {
					run = false
				}
			case *kafka.Stats:
				var stats map[string]interface{}
				json.Unmarshal([]byte(event.String()), &stats)
				log.Printf("Stats: %v messages (%v bytes) messages consumed\n", stats["rxmsgs"], stats["rxmsg_bytes"])
			default:
				log.Print(event.String())
				log.Printf("Ignored %v\n", event)
			}
		}
	}

	log.Printf("Closing consumer\n")
	consumer.Close()
}

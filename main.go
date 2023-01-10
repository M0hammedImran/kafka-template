package main

import (
	"grandline-golang/cmd"
	"log"
	"os"
	"os/signal"
	"strconv"
	"sync"
	"syscall"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/joho/godotenv"
)

var (
	consumerBrokers  = ""
	consumerGroupId  = ""
	consumerTopics   = ""
	consumerUsername = ""
	consumerPassword = ""
	consumerUseAuth  = false

	producerBrokers  = ""
	producerUsername = ""
	producerPassword = ""
	producerUseAuth  = false
)

func init() {
	godotenv.Load()

	consumerBrokers = getStringEnvFromOS("KAFKA_CONSUMER_BROKER_URL")
	consumerGroupId = getStringEnvFromOS("KAFKA_CONSUMER_GROUP_ID")
	consumerTopics = getStringEnvFromOS("KAFKA_CONSUMER_TOPICS")
	consumerUsername = getOptionalStringEnvFromOS("KAFKA_CONSUMER_USERNAME")
	consumerPassword = getOptionalStringEnvFromOS("KAFKA_CONSUMER_PASSWORD")
	consumerUseAuth = getBoolEnvFromOS("USE_KAFKA_CONSUMER_TLS")

	producerBrokers = getStringEnvFromOS("KAFKA_PRODUCER_BROKER_URL")
	producerUsername = getOptionalStringEnvFromOS("KAFKA_PRODUCER_USERNAME")
	producerPassword = getOptionalStringEnvFromOS("KAFKA_PRODUCER_PASSWORD")
	producerUseAuth = getBoolEnvFromOS("USE_KAFKA_PRODUCER_TLS")

	cmd.Verbosity = getIntEnvFromOS("VERBOSITY")
}

func getStringEnvFromOS(envname string) string {
	value := os.Getenv(envname)
	if value == "" {
		log.Panicf("%s is not provided\n", envname)
	}
	return value
}

func getBoolEnvFromOS(envname string) bool {
	value := os.Getenv(envname)
	boolValue, err := strconv.ParseBool(value)
	if err != nil {
		log.Panicf("%s is not provided\n", envname)
	}

	return boolValue
}

func getOptionalStringEnvFromOS(envname string) string {
	return os.Getenv(envname)
}

func getIntEnvFromOS(envname string) int {
	value := os.Getenv(envname)
	if value == "" {
		log.Panicf("%s is not provided\n", envname)
	}
	number, err := strconv.Atoi(value)
	if err != nil {
		log.Panicf("%s is not provided\n", err.Error())
	}
	return number
}

func main() {
	cmd.SignalChannel = make(chan os.Signal, 1)
	cmd.ProducerChannel = make(chan kafka.Message)
	wg := &sync.WaitGroup{}

	signal.Notify(cmd.SignalChannel, syscall.SIGINT, syscall.SIGTERM)

	consumerConfig := &kafka.ConfigMap{
		"bootstrap.servers":        consumerBrokers,
		"broker.address.family":    "v4",
		"group.id":                 consumerGroupId,
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": true,
	}

	if consumerUseAuth {
		consumerConfig.SetKey("security.protocol", "SASL_SSL")
		consumerConfig.SetKey("sasl.mechanisms", "SCRAM-SHA-512")
		consumerConfig.SetKey("sasl.username", consumerUsername)
		consumerConfig.SetKey("sasl.password", consumerPassword)
	}

	producerConfig := &kafka.ConfigMap{
		"bootstrap.servers":        producerBrokers,
		"broker.address.family":    "v4",
		"session.timeout.ms":       6000,
		"auto.offset.reset":        "earliest",
		"enable.auto.offset.store": true,
	}

	if producerUseAuth {
		producerConfig.SetKey("security.protocol", "SASL_SSL")
		producerConfig.SetKey("sasl.mechanisms", "SCRAM-SHA-512")
		producerConfig.SetKey("sasl.username", producerUsername)
		producerConfig.SetKey("sasl.password", producerPassword)
	}

	wg.Add(1)
	go func() {
		cmd.Consumer(consumerConfig, consumerTopics)

		wg.Done()
	}()

	wg.Add(1)
	go func() {
		cmd.Producer(producerConfig)

		wg.Done()
	}()
	wg.Wait()
}

package cmd

import (
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

var (
	Verbosity = 0

	SignalChannel   chan os.Signal
	ProducerChannel chan kafka.Message
)

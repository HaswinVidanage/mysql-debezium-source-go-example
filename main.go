package main

import (
	"encoding/json"
	"fmt"
	"os"

	"github.com/confluentinc/confluent-kafka-go/kafka"
)

type Payload struct {
	Before map[string]interface{} `json:"before"`
	After  map[string]interface{} `json:"after"`
}

type simpleMessage struct {
	Payload Payload `json:"payload"`
}

func main() {
	fmt.Println("SERVER STARTING...")

	consumer, err := kafka.NewConsumer(&kafka.ConfigMap{
		"bootstrap.servers":  "localhost:29092",
		"group.id":           "foo",
		"auto.offset.reset":  "smallest",
		"enable.auto.commit": false,
	})
	checkErr(err)

	defer consumer.Close()

	err = consumer.Subscribe("customers", nil)
	checkErr(err)

	messageChan := make(chan *kafka.Message, 100)

	// Start a goroutine for polling Kafka messages
	go func() {
		for {
			ev := consumer.Poll(100)
			switch e := ev.(type) {
			case *kafka.Message:
				if len(e.Value) != 0 {
					messageChan <- e
				}
			case kafka.Error:
				if e.Code() != kafka.ErrUnknownTopicOrPart {
					fmt.Fprintf(os.Stderr, "%% Error: %v\n", e)
					close(messageChan)
					return
				}
			default:
				// Ignored other events
			}
		}
	}()

	// Start a goroutine for processing messages
	go func() {
		for msg := range messageChan {
			handleMessageAndCommit(consumer, msg)
		}
	}()

	fmt.Println("SERVER READY AND LISTENING FOR MESSAGES...")
	select {}
}

func handleMessageAndCommit(consumer *kafka.Consumer, message *kafka.Message) {
	var sm simpleMessage
	err := json.Unmarshal(message.Value, &sm)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%% Unmarshal error: %v\n", err)
		return
	}

	handleMessage(sm.Payload)

	// Manually commit the offset after processing the message
	_, err = consumer.CommitMessage(message)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%% Commit error: %v\n", err)
	}
}

func handleMessage(payload Payload) {
	if payload.Before == nil && payload.After != nil {
		fmt.Println("INSERT detected:", payload.After)
	} else if payload.Before != nil && payload.After == nil {
		fmt.Println("DELETE detected:", payload.Before)
	} else if payload.Before != nil && payload.After != nil {
		fmt.Println("UPDATE detected:")
		fmt.Println("Before:", payload.Before)
		fmt.Println("After:", payload.After)
	} else {
		fmt.Println("Unknown operation")
	}
}

func checkErr(err error) {
	if err != nil {
		panic(err)
	}
}

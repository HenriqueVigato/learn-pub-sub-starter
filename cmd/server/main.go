package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	rabbitURL := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("failed to connect to rabbitmq %v", err)
	}
	defer connection.Close()

	fmt.Println("RabbitMq connection success")

	ch, err := connection.Channel()
	if err != nil {
		log.Fatalf("failed to create a chanel err: %v", err)
	}

	err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
	if err != nil {
		log.Fatalf("Error can't publis the data %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Printf("\nClosing RabbitMQ connection ...\n")
	connection.Close()
	fmt.Println("Closed")
}

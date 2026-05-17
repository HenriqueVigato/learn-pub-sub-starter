package main

import (
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril client...")

	rabbitURL := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("failed to connect to rabbitmq %v", err)
	}
	defer connection.Close()

	username, err := gamelogic.ClientWelcome()
	if err != nil {
		log.Fatalf("Couldn't get user name %v", err)
	}

	chanel, _, err := pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatalf("couldn't make a chanel %v", err)
	}

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Printf("\nClosing connection ...\n")
	connection.Close()
	chanel.Close()
	fmt.Println("Closed")
}

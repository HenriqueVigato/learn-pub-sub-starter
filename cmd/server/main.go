package main

import (
	"fmt"
	"log"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	fmt.Println("Starting Peril server...")

	rabbitURL := "amqp://guest:guest@192.168.1.130:5672/"
	connection, err := amqp.Dial(rabbitURL)
	if err != nil {
		log.Fatalf("failed to connect to rabbitmq %v", err)
	}
	defer connection.Close()

	fmt.Println("RabbitMq connection success")

	ch, _, err := pubsub.DeclareAndBind(connection, routing.ExchangePerilTopic, "game_logs", routing.GameLogSlug, pubsub.Durable)
	if err != nil {
		log.Fatalf("failed to create a chanel err: %v", err)
	}

	err = pubsub.SubscribeGob(connection, routing.ExchangePerilTopic, "game_logs", fmt.Sprintf("%s.*", routing.GameLogSlug), pubsub.Durable, handlerLog(gameState))

	gamelogic.PrintServerHelp()

	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}

		switch userInput[0] {
		case "pause":
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: true})
			if err != nil {
				log.Printf("Error can't publish the data %v", err)
			}
			fmt.Printf("\nPausing the game...\n")
		case "resume":
			err = pubsub.PublishJSON(ch, routing.ExchangePerilDirect, routing.PauseKey, routing.PlayingState{IsPaused: false})
			if err != nil {
				log.Fatalf("Error can't publis the data %v", err)
			}
			fmt.Printf("\nReturning the game...\n")
		case "quit":
			fmt.Printf("\nClosing RabbitMQ connection ...\n")
			return
		default:
			fmt.Println("I don't understand the command")
		}
	}
}

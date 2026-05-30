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
	gameState := gamelogic.NewGameState(username)

	publishCH, err := connection.Channel()
	if err != nil {
		log.Fatalf("could not create channel: %v", err)
	}
	defer publishCH.Close()

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilDirect, fmt.Sprintf("pause.%s", username), routing.PauseKey, pubsub.Transient, handlerPause(gameState))
	if err != nil {
		log.Fatalf("Couldn't Bind the connection with SubscribeJSON, %v", err)
	}

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", username), "army_moves.*", pubsub.Transient, handlerMove(gameState, publishCH))
	if err != nil {
		log.Fatalf("Couldn't subscribe to other users moves: %v", err)
	}

	err = pubsub.SubscribeJSON(connection, routing.ExchangePerilTopic, "war", fmt.Sprintf("%s.*", routing.WarRecognitionsPrefix), pubsub.Durable, handlerWar(gameState))
	if err != nil {
		log.Fatalf("couldn't subscribe to war channel %v", err)
	}

	for {
		userInput := gamelogic.GetInput()
		if len(userInput) == 0 {
			continue
		}

		switch userInput[0] {
		case "spawn":
			err := gameState.CommandSpawn(userInput)
			if err != nil {
				fmt.Printf("\n Erro: %v\n", err)
				continue
			}

		case "move":
			if len(userInput) < 3 {
				fmt.Println("the move command should have 2 options")
				continue
			}
			armyMove, err := gameState.CommandMove(userInput)
			if err != nil {
				fmt.Printf("\n Erro: %v\n", err)
				continue
			}
			err = pubsub.PublishJSON(publishCH, routing.ExchangePerilTopic, fmt.Sprintf("army_moves.%s", username), armyMove)
			if err != nil {
				fmt.Printf("error publishing move: %v\n", err)
				continue
			}
			fmt.Println("Move published successfully")

		case "status":
			gameState.CommandStatus()

		case "help":
			gamelogic.PrintClientHelp()

		case "spam":
			fmt.Println("Spamming not allowed yet!")

		case "quit":
			gamelogic.PrintQuit()
			return

		default:
			fmt.Println("Commando invalide")
		}
	}
}

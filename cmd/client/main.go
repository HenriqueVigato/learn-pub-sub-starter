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

	channel, _, err := pubsub.DeclareAndBind(connection, routing.ExchangePerilDirect, fmt.Sprintf("%s.%s", routing.PauseKey, username), routing.PauseKey, pubsub.Transient)
	if err != nil {
		log.Fatalf("couldn't make a chanel %v", err)
	}
	defer channel.Close()

	gameState := gamelogic.NewGameState(username)
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
			_, err := gameState.CommandMove(userInput)
			if err != nil {
				fmt.Printf("\n Erro: %v\n", err)
				continue
			}
			fmt.Println("Move succes")
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

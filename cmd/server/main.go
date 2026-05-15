package main

import (
	"fmt"
	"os"
	"os/signal"

	amqp "github.com/rabbitmq/amqp091-go"
)

func main() {
	rabbitURL := "amqp://guest:guest@localhost:5672/"
	connection, err := amqp.Dial(rabbitURL)
	if err != nil {
		fmt.Errorf("erro ao estabelecer conecao com o rabbitmq %v", err)
	}
	defer connection.Close()

	fmt.Println("RabbitMq connection success")
	fmt.Println("Starting server...")

	signalChan := make(chan os.Signal, 1)
	signal.Notify(signalChan, os.Interrupt)
	<-signalChan

	fmt.Printf("\nClosing RabbitMQ connection ...\n")
	connection.Close()
	fmt.Println("Closed")
}

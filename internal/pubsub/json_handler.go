package pubsub

import (
	"context"
	"encoding/json"
	"log"

	amqp "github.com/rabbitmq/amqp091-go"
)

type AckType int

const (
	Ack AckType = iota
	NackRequeue
	NackDiscard
)

func PublishJSON[T any](ch *amqp.Channel, exchange, key string, val T) error {
	data, err := json.Marshal(val)
	if err != nil {
		return err
	}

	ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})
	return nil
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	amqpChanel, amqpQueue, err := DeclareAndBind(conn, exchange, queueName, key, queueType)
	if err != nil {
		return err
	}

	msgChannel, err := amqpChanel.Consume(amqpQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgChannel {
			var data T
			err := json.Unmarshal(msg.Body, &data)
			if err != nil {
				log.Printf("couldn't Unmarshal: %v\n> ", err)
				msg.Nack(false, false)
				continue
			}
			response := handler(data)
			switch response {
			case Ack:
				msg.Ack(false)
				log.Printf("Message success\n> ")
			case NackRequeue:
				msg.Nack(false, true)
				log.Print("Message requeue\n> ")
			case NackDiscard:
				msg.Nack(false, false)
				log.Print("Message Discard\n> ")
			}
		}
	}()

	return nil
}

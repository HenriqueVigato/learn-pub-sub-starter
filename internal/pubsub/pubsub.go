// Package pubsub provides fucntions for publishing and subscribing
// to messages using RabbitMQ.

package pubsub

import (
	"bytes"
	"context"
	"encoding/gob"
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

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/json",
		Body:        data,
	})
}

func PublishGob[T any](ch *amqp.Channel, exchange, key string, val T) error {
	var data bytes.Buffer

	enc := gob.NewEncoder(&data)
	err := enc.Encode(val)
	if err != nil {
		return err
	}

	return ch.PublishWithContext(context.Background(), exchange, key, false, false, amqp.Publishing{
		ContentType: "application/gob",
		Body:        data.Bytes(),
	})
}

func SubscribeJSON[T any](conn *amqp.Connection, exchange, queueName, key string, queueType SimpleQueueType, handler func(T) AckType) error {
	JSONUnmarshaller := func(data []byte) (T, error) {
		var dataJSON T
		err := json.Unmarshal(data, &dataJSON)
		if err != nil {
			return *new(T), err
		}
		return dataJSON, nil
	}

	return subscribe(conn, exchange, queueName, key, queueType, handler, JSONUnmarshaller)
}

func SubscribeGob[T any](conn *amqp.Connection, exchange, queueName, key string, simpleQueueType SimpleQueueType, handler func(T) AckType) error {
	gobUnmarshaller := func(data []byte) (T, error) {
		response := bytes.NewBuffer(data)
		decode := gob.NewDecoder(response)

		var dataGob T
		err := decode.Decode(&dataGob)
		if err != nil {
			return *new(T), err
		}
		return dataGob, nil
	}
	return subscribe(conn, exchange, queueName, key, simpleQueueType, handler, gobUnmarshaller)
}

func subscribe[T any](conn *amqp.Connection, exchange, queueName, key string, simpleQueueType SimpleQueueType, handler func(T) AckType, unmarshaller func([]byte) (T, error)) error {
	amqpChannel, amqpQueue, err := DeclareAndBind(conn, exchange, queueName, key, simpleQueueType)
	if err != nil {
		return err
	}

	msgChannel, err := amqpChannel.Consume(amqpQueue.Name, "", false, false, false, false, nil)
	if err != nil {
		return err
	}

	go func() {
		for msg := range msgChannel {
			data, err := unmarshaller(msg.Body)
			if err != nil {
				log.Printf("couldn't Unmarshal the data: %v\n ", err)
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

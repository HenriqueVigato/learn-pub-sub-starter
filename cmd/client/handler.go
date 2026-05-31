package main

import (
	"fmt"
	"log"
	"time"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/gamelogic"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
	amqp "github.com/rabbitmq/amqp091-go"
)

func handlerPause(gs *gamelogic.GameState) func(routing.PlayingState) pubsub.AckType {
	return func(ps routing.PlayingState) pubsub.AckType {
		defer fmt.Print("> ")
		gs.HandlePause(ps)
		return pubsub.Ack
	}
}

func handlerMove(gs *gamelogic.GameState, amqpChannel *amqp.Channel) func(gamelogic.ArmyMove) pubsub.AckType {
	return func(hm gamelogic.ArmyMove) pubsub.AckType {
		defer fmt.Print("> ")
		move := gs.HandleMove(hm)
		switch move {
		case gamelogic.MoveOutComeSafe:
			return pubsub.Ack
		case gamelogic.MoveOutcomeMakeWar:
			err := pubsub.PublishJSON(amqpChannel, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.WarRecognitionsPrefix, gs.GetUsername()), gamelogic.RecognitionOfWar{Attacker: hm.Player, Defender: gs.GetPlayerSnap()})
			if err != nil {
				log.Printf("couldn't Publish war message: %v ", err)
				return pubsub.NackRequeue
			}
			return pubsub.Ack
		case gamelogic.MoveOutcomeSamePlayer:
			return pubsub.NackDiscard
		default:
			return pubsub.NackDiscard
		}
	}
}

func handlerWar(gs *gamelogic.GameState, amqpCh *amqp.Channel) func(gamelogic.RecognitionOfWar) pubsub.AckType {
	return func(warMessage gamelogic.RecognitionOfWar) pubsub.AckType {
		defer fmt.Print("> ")
		warOutcome, winner, loser := gs.HandleWar(warMessage)

		switch warOutcome {
		case gamelogic.WarOutcomeNotInvolved:
			return pubsub.NackRequeue

		case gamelogic.WarOutcomeNoUnits:
			return pubsub.NackDiscard

		case gamelogic.WarOutcomeYouWon:
			logMsg := fmt.Sprintf("{%s} won a war against {%s}", winner, loser)
			err := gameLogPublisher(amqpCh, logMsg, warMessage.Attacker.Username)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.WarOutcomeOpponentWon:
			logMsg := fmt.Sprintf("{%s} won a war against {%s}", winner, loser)
			err := gameLogPublisher(amqpCh, logMsg, warMessage.Attacker.Username)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		case gamelogic.WarOutcomeDraw:
			logMsg := fmt.Sprintf("A war between {%s} and {%s} resulted in a draw", winner, loser)
			err := gameLogPublisher(amqpCh, logMsg, warMessage.Attacker.Username)
			if err != nil {
				return pubsub.NackRequeue
			}
			return pubsub.Ack

		default:
			return pubsub.NackDiscard
		}
	}
}

func gameLogPublisher(amqpCh *amqp.Channel, msg, agressorName string) error {
	return pubsub.PublishGob(amqpCh, routing.ExchangePerilTopic, fmt.Sprintf("%s.%s", routing.GameLogSlug, agressorName), routing.GameLog{
		CurrentTime: time.Now(),
		Message:     msg,
		Username:    agressorName,
	})
}

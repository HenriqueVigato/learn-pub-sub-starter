package main

import (
	"testing"

	"github.com/bootdotdev/learn-pub-sub-starter/internal/pubsub"
	"github.com/bootdotdev/learn-pub-sub-starter/internal/routing"
)

func Test_handlerLog(t *testing.T) {
	tests := []struct {
		name string // description of this test case
		want func(routing.GameLog) pubsub.AckType
	}{
		// TODO: Add test cases.
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := handlerLog()
			// TODO: update the condition below to compare got with tt.want.
			if true {
				t.Errorf("handlerLog() = %v, want %v", got, tt.want)
			}
		})
	}
}

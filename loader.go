package pubsub

import (
	"context"

	"github.com/webcore-go/webcore/infra/config"
	"github.com/webcore-go/webcore/port"
)

type PubSubLoader struct {
	name string
}

func (a *PubSubLoader) SetName(name string) {
	a.name = name
}

func (a *PubSubLoader) Name() string {
	return a.name
}

func (l *PubSubLoader) Init(args ...any) (port.Library, error) {
	context := args[0].(context.Context)
	config := args[1].(config.PubSubConfig)

	pubsub, err := NewPubSub(context, config)
	if err != nil {
		return nil, err
	}

	err = pubsub.Install(args...)
	if err != nil {
		return nil, err
	}

	return pubsub, nil
}

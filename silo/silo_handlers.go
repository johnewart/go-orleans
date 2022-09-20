package silo

import (
	"context"
	"github.com/johnewart/go-orleans/client"
	"zombiezen.com/go/log"
)

type GrainHandle interface {
	Invoke(context.Context, *client.Invocation, chan *client.GrainExecution) error
}

type RemoteGrainHandle struct {
	GrainHandle
	channel chan *client.Invocation
}

func (h RemoteGrainHandle) Invoke(ctx context.Context, invocation *client.Invocation, responseChan chan *client.GrainExecution) error {
	h.channel <- invocation
	return nil
}

type FunctionalGrainHandle struct {
	GrainHandle
	Handler func(context.Context, *client.Invocation) (*client.GrainExecution, error)
}

func (h FunctionalGrainHandle) Invoke(ctx context.Context, invocation *client.Invocation, responseChan chan *client.GrainExecution) error {
	if res, err := h.Handler(ctx, invocation); err != nil {
		log.Infof(ctx, "Error processing request, sending error response to channel for functionalHandler")
		responseChan <- &client.GrainExecution{
			Status: client.ExecutionError,
			Error:  err,
		}
	} else {
		log.Infof(ctx, "Sending response to channel for functionalHandler")
		responseChan <- res
	}

	return nil
}

package silo

import (
	"context"
	"github.com/johnewart/go-orleans/grains"
	"zombiezen.com/go/log"
)

type GrainHandle interface {
	Invoke(context.Context, *grains.Invocation, chan *grains.GrainExecution) error
}

type RemoteGrainHandle struct {
	GrainHandle
	Channel chan *grains.Invocation
}

func (h RemoteGrainHandle) Invoke(ctx context.Context, invocation *grains.Invocation, responseChan chan *grains.GrainExecution) error {
	h.Channel <- invocation
	return nil
}

type FunctionalGrainHandle struct {
	GrainHandle
	Handler func(context.Context, *grains.Invocation) (*grains.GrainExecution, error)
}

func (h FunctionalGrainHandle) Invoke(ctx context.Context, invocation *grains.Invocation, responseChan chan *grains.GrainExecution) error {
	if res, err := h.Handler(ctx, invocation); err != nil {
		log.Infof(ctx, "Error processing request, sending error response to Channel for functionalHandler")
		responseChan <- &grains.GrainExecution{
			Status: grains.ExecutionError,
			Error:  err,
		}
	} else {
		log.Infof(ctx, "Sending response to Channel for functionalHandler")
		responseChan <- res
	}

	return nil
}

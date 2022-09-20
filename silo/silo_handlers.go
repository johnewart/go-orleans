package silo

import (
	"context"
	"fmt"
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

type GrainHandlerFunc func(context.Context, *client.Invocation) (*client.GrainExecution, error)

type GrainRegistry struct {
	handlerMap map[string]GrainHandle
}

func NewSiloGrainRegistry() *GrainRegistry {
	return &GrainRegistry{
		handlerMap: make(map[string]GrainHandle),
	}
}

func (h *GrainRegistry) Register(grainType string, handle GrainHandle) error {
	h.handlerMap[grainType] = handle
	return nil
}

func (h *GrainRegistry) Deregister(grainType string) {
	delete(h.handlerMap, grainType)
}

func (h *GrainRegistry) Handle(ctx context.Context, invocation *client.Invocation) (chan *client.GrainExecution, error) {
	log.Infof(ctx, "Handling grain of type %s@%s", invocation.GrainType, invocation.GrainID)

	if handle, ok := h.handlerMap[invocation.GrainType]; ok {
		ch := make(chan *client.GrainExecution, 1)
		handle.Invoke(ctx, invocation, ch)
		return ch, nil
	} else {
		return nil, fmt.Errorf("no handler registered for grain type %s", invocation.GrainType)
	}

}

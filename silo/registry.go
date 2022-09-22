package silo

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/grains"
	"zombiezen.com/go/log"
)

type GrainHandlerFunc func(context.Context, *grains.Invocation) (*grains.GrainExecution, error)

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

func (h *GrainRegistry) Handle(ctx context.Context, invocation *grains.Invocation) (chan *grains.InvocationResult, error) {
	log.Infof(ctx, "Handling grain of type %s@%s", invocation.GrainType, invocation.GrainID)

	if handle, ok := h.handlerMap[invocation.GrainType]; ok {
		ch := make(chan *grains.InvocationResult, 1000)
		handle.Invoke(ctx, invocation, ch)
		return ch, nil
	} else {
		return nil, fmt.Errorf("no handler registered for grains type %s", invocation.GrainType)
	}

}

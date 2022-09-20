package silo

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/client"
	"zombiezen.com/go/log"
)

type GrainHandle interface {
	Invoke(context.Context, *client.Invocation) (*client.GrainExecution, error)
}

type RemoteGrainHandle struct {
	GrainHandle
	channel chan *client.Invocation
}

func (h RemoteGrainHandle) Invoke(ctx context.Context, invocation *client.Invocation) (*client.GrainExecution, error) {
	h.channel <- invocation
	return nil, nil
}

type FunctionalGrainHandle struct {
	GrainHandle
	Handler func(context.Context, *client.Invocation) (*client.GrainExecution, error)
}

func (h FunctionalGrainHandle) Invoke(ctx context.Context, invocation *client.Invocation) (*client.GrainExecution, error) {
	return h.Handler(ctx, invocation)
}

type GrainHandlerFunc func(context.Context, *client.Invocation) (*client.GrainExecution, error)

type GrainHandler struct {
	handlerMap map[string]GrainHandle
}

func NewSiloGrainHandler() *GrainHandler {
	return &GrainHandler{
		handlerMap: make(map[string]GrainHandle),
	}
}

func (h *GrainHandler) Register(grainType string, handle GrainHandle) error {
	h.handlerMap[grainType] = handle
	return nil
}

func (h *GrainHandler) Deregister(grainType string) {
	delete(h.handlerMap, grainType)
}

func (h *GrainHandler) Handle(ctx context.Context, grainId string, grainType string, data []byte) (*client.GrainExecution, error) {
	log.Infof(ctx, "Handling grain of type %s@%s", grainType, grainId)
	invocation := &client.Invocation{
		GrainID:    grainId,
		GrainType:  grainType,
		Data:       data,
		Context:    ctx,
		MethodName: "hullo",
	}

	if handle, ok := h.handlerMap[grainType]; ok {
		if result, err := handle.Invoke(ctx, invocation); err != nil {
			return nil, err
		} else {
			return result, nil
		}
	} else {
		return nil, fmt.Errorf("no handler registered for grain type %s", grainType)
	}

}

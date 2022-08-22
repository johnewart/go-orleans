package silo

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/client"
	"zombiezen.com/go/log"
)
import "github.com/johnewart/go-orleans/grain"

type GrainHandlerFunc func(context.Context, *grain.Grain) (*client.GrainExecution, error)

type GrainHandler struct {
	handlerMap map[string]GrainHandlerFunc
}

func NewSiloGrainHandler() *GrainHandler {
	return &GrainHandler{
		handlerMap: make(map[string]GrainHandlerFunc),
	}
}

func (h *GrainHandler) Register(grainType string, handler GrainHandlerFunc) error {
	h.handlerMap[grainType] = handler
	return nil
}

func (h *GrainHandler) Deregister(grainType string) {
	delete(h.handlerMap, grainType)
}

func (h *GrainHandler) Handle(ctx context.Context, grainType string, data []byte) (*client.GrainExecution, error) {
	log.Infof(ctx, "Handling grain of type %s", grainType)
	if handler, ok := h.handlerMap[grainType]; ok {
		if result, err := handler(ctx, &grain.Grain{
			Type: grainType,
			Data: data,
		}); err != nil {
			return nil, err
		} else {
			return result, nil
		}
	} else {
		return nil, fmt.Errorf("no handler registered for grain type %s", grainType)
	}

}

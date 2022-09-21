package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/johnewart/go-orleans/client"
	"github.com/johnewart/go-orleans/grains"
	"zombiezen.com/go/log"
)

type OhaiGrain struct {
	client.GrainHandler
}

func (h OhaiGrain) Handle(invocation *grains.Invocation) (grains.GrainExecution, error) {
	log.Infof(invocation.Context, "%s@%s grain being invoked!", invocation.GrainType, invocation.GrainID)
	switch invocation.MethodName {
	case "Greet":

		return grains.GrainExecution{
			GrainID:   invocation.GrainID,
			Result:    []byte(fmt.Sprintf("Ohai, %s!", string(invocation.Data))),
			GrainType: invocation.GrainType,
		}, nil

	default:
		return grains.GrainExecution{
			GrainID:   invocation.GrainID,
			Result:    []byte(fmt.Sprintf("Unknown method: %s", invocation.MethodName)),
			GrainType: invocation.GrainType,
		}, nil
	}
}

func main() {
	ctx := context.Background()

	c := client.NewClientFromEnv(ctx)

	metadata := client.GrainMetadata{Type: "Ohai", Version: "1.0"}

	commandWg := sync.WaitGroup{}

	if commandHandler, err := c.RegisterGrainHandler(metadata, OhaiGrain{}); err != nil {
		log.Warnf(ctx, "Unable to register grains handler: %v", err)
	} else {
		log.Infof(ctx, "Registered grains handler: %v", commandHandler)
		commandWg.Add(1)

		go func() {
			log.Infof(ctx, "Running command handler...")
			defer commandWg.Done()
			commandHandler.Run()
			log.Infof(ctx, "Command handler finished....")
		}()
	}

	log.Infof(ctx, "Waiting for command handler to finish...")

	commandWg.Wait()

}

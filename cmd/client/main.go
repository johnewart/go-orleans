package main

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/client"
	"github.com/johnewart/go-orleans/grains"
	"time"
	"zombiezen.com/go/log"
)

type OhaiGrain struct {
	client.GrainHandler
}

func (h OhaiGrain) Handle(invocation *grains.Invocation) (grains.GrainExecution, error) {
	log.Infof(invocation.Context, "%s@%s grain being invoked!", invocation.GrainType, invocation.GrainID)
	return grains.GrainExecution{
		GrainID:   invocation.GrainID,
		Result:    []byte(fmt.Sprintf("Ohai, %s!", string(invocation.Data))),
		GrainType: invocation.GrainType,
	}, nil
}

func main() {
	ctx := context.Background()

	c := client.NewClientFromEnv(ctx)
	invocationCount := 10000

	start := time.Now()
	for i := 0; i < invocationCount; i++ {
		g := grains.Grain{
			Type: "Ohai",
			ID:   fmt.Sprintf("grain-%d", i),
		}

		i := g.Invocation("Greet", []byte("Samus Aran"))

		log.Infof(ctx, "Scheduling grains: %v", g)
		if res := c.InvokeGrain(i); res.Error != nil {
			log.Warnf(ctx, "Unable to invoke: %v", res.Error)
		} else {
			log.Infof(ctx, "Execution result: %s", res.String())
		}
	}
	n := time.Now()
	elapsed := n.Sub(start)
	log.Infof(ctx, "Elapsed: %s", elapsed)
	log.Infof(ctx, "Invocations per second: %f", float64(invocationCount)/elapsed.Seconds())
}

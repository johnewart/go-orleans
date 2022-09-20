package main

import (
	"context"
	"github.com/johnewart/go-orleans/client"
	"github.com/johnewart/go-orleans/grain"
	"os"
	"strconv"
	"sync"
	"zombiezen.com/go/log"
)

type HelloGrain struct {
	client.GrainHandler
}

func (h HelloGrain) Handle(invocation *client.Invocation) error {
	log.Infof(invocation.Context, "Handling grain of type %s@%s", invocation.GrainType, invocation.GrainID)
	return nil
}

func main() {
	ctx := context.Background()

	clusterHost := os.Getenv("CLUSTER_HOST")
	clusterPort, _ := strconv.Atoi(os.Getenv("CLUSTER_PORT"))

	log.Infof(ctx, "CLUSTER_HOST: %s", clusterHost)
	log.Infof(ctx, "CLUSTER_PORT: %d", clusterPort)

	c := client.NewClient(ctx, clusterHost, clusterPort)

	g := grain.Grain{
		Type: "HelloWorld",
		Data: []byte("Samus Aran"),
	}

	if res := c.ScheduleGrain(&g); res.Error != nil {
		log.Warnf(ctx, "Unable to schedule grain: %v", res.Error)
	} else {
		log.Infof(ctx, "Grain result: %s", res.Result)
	}

	metadata := client.GrainMetadata{"Ohai", "1.0"}

	commandWg := sync.WaitGroup{}

	if commandHandler, err := c.RegisterGrainHandler(metadata, HelloGrain{}); err != nil {
		log.Warnf(ctx, "Unable to register grain handler: %v", err)
	} else {
		log.Infof(ctx, "Registered grain handler: %v", commandHandler)
		commandWg.Add(1)

		go func() {
			log.Infof(ctx, "Running command handler...")
			defer commandWg.Done()
			commandHandler.Run()
		}()
	}

	og := grain.Grain{
		Type: "Ohai",
		ID:   "1",
		Data: []byte("Samus Aran"),
	}

	if res := c.ScheduleGrain(&og); res.Error != nil {
		log.Warnf(ctx, "Unable to schedule grain: %v", res.Error)
	} else {
		log.Infof(ctx, "Execution result: %s", res.String())
	}

	commandWg.Wait()
	/*


		wg := sync.WaitGroup{}
		for i := 0; i <= 5; i++ {
			sleepTime := 20 - i*2
			sleepGrain := grain.Grain{
				ID:   fmt.Sprintf("sleep-%d", i),
				Type: "Sleep",
				Data: []byte(fmt.Sprintf("%d", sleepTime)),
			}
			log.Infof(ctx, "Scheduling grain %d", i)
			wg.Add(1)

			c.ScheduleGrainAsync(&sleepGrain, func(res *client.GrainExecution) {
				id := res.GrainID
				log.Infof(ctx, "Grain %s result: %s", id, res.Result)
				wg.Done()
			})
		}

		wg.Wait()
	*/
}

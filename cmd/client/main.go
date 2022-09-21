package main

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/client"
	"github.com/johnewart/go-orleans/grains"
	"github.com/johnewart/go-orleans/silo"
	"os"
	"strconv"
	"sync"
	"time"
	"zombiezen.com/go/log"
)

type HelloGrain struct {
	client.GrainHandler
}

func (h HelloGrain) Handle(invocation *grains.Invocation) (grains.GrainExecution, error) {
	log.Infof(invocation.Context, "Handling grains of type %s@%s", invocation.GrainType, invocation.GrainID)
	return grains.GrainExecution{
		GrainID:   invocation.GrainID,
		Result:    []byte(fmt.Sprintf("Ohai, %s!", string(invocation.Data))),
		GrainType: invocation.GrainType,
	}, nil
}

func main() {
	ctx := context.Background()

	clusterHost := os.Getenv("CLUSTER_HOST")
	clusterPort, _ := strconv.Atoi(os.Getenv("CLUSTER_PORT"))

	log.Infof(ctx, "CLUSTER_HOST: %s", clusterHost)
	log.Infof(ctx, "CLUSTER_PORT: %d", clusterPort)

	c := client.NewClient(ctx, clusterHost, clusterPort)

	g := grains.Grain{
		Type: "HelloWorld",
		Data: []byte("Samus Aran"),
	}

	if res := c.ScheduleGrain(&g); res.Error != nil {
		log.Warnf(ctx, "Unable to schedule grains: %v", res.Error)
	} else {
		log.Infof(ctx, "Grain result: %s", res.Result)
	}

	if err := c.ScheduleReminder(&silo.Reminder{
		ReminderName: "ReminderFoo!",
		GrainType:    "Ohai",
		GrainId:      "Samus Aran",
		Period:       10 * time.Second,
		DueTime:      time.Now(),
		Data:         []byte("Samus Aran"),
	}); err != nil {
		log.Warnf(ctx, "Unable to schedule reminder: %v", err)
	}

	metadata := client.GrainMetadata{"Ohai", "1.0"}

	commandWg := sync.WaitGroup{}

	if commandHandler, err := c.RegisterGrainHandler(metadata, HelloGrain{}); err != nil {
		log.Warnf(ctx, "Unable to register grains handler: %v", err)
	} else {
		log.Infof(ctx, "Registered grains handler: %v", commandHandler)
		commandWg.Add(1)

		go func() {
			log.Infof(ctx, "Running command handler...")
			defer commandWg.Done()
			commandHandler.Run()
		}()
	}

	og := grains.Grain{
		Type: "Ohai",
		ID:   "1",
		Data: []byte("Samus Aran"),
	}

	log.Infof(ctx, "Scheduling grains: %v", og)
	if res := c.ScheduleGrain(&og); res.Error != nil {
		log.Warnf(ctx, "Unable to schedule grains: %v", res.Error)
	} else {
		log.Infof(ctx, "Execution result: %s", res.String())
	}

	log.Infof(ctx, "Waiting for command handler to finish...")

	commandWg.Wait()
	/*


		wg := sync.WaitGroup{}
		for i := 0; i <= 5; i++ {
			sleepTime := 20 - i*2
			sleepGrain := grains.Grain{
				ID:   fmt.Sprintf("sleep-%d", i),
				Type: "Sleep",
				Data: []byte(fmt.Sprintf("%d", sleepTime)),
			}
			log.Infof(ctx, "Scheduling grains %d", i)
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

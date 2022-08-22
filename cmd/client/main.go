package main

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/client"
	"github.com/johnewart/go-orleans/grain"
	"os"
	"strconv"
	"sync"
	"zombiezen.com/go/log"
)

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
}

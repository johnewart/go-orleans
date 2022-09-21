package main

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/johnewart/go-orleans/client"
	"github.com/johnewart/go-orleans/silo"
	"zombiezen.com/go/log"
)

func main() {
	ctx := context.Background()

	clusterHost := os.Getenv("CLUSTER_HOST")
	clusterPort, _ := strconv.Atoi(os.Getenv("CLUSTER_PORT"))

	log.Infof(ctx, "CLUSTER_HOST: %s", clusterHost)
	log.Infof(ctx, "CLUSTER_PORT: %d", clusterPort)

	c := client.NewClient(ctx, clusterHost, clusterPort)

	for i := 0; i < 1000; i++ {
		if err := c.ScheduleReminder(&silo.Reminder{
			ReminderName: fmt.Sprintf("ReminderFoo-%d", i),
			GrainType:    "Ohai",
			GrainId:      fmt.Sprintf("%d", i),
			Period:       10 * time.Second,
			FireAt:       time.Now(),
			Data:         []byte("Samus Aran"),
		}); err != nil {
			log.Warnf(ctx, "Unable to schedule reminder: %v", err)
		}
	}

}

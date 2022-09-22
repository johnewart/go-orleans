package main

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/reminders/data"
	"os"
	"strconv"
	"time"

	"github.com/johnewart/go-orleans/client"
	"zombiezen.com/go/log"
)

func main() {
	ctx := context.Background()

	clusterHost := os.Getenv("CLUSTER_HOST")
	clusterPort, _ := strconv.Atoi(os.Getenv("CLUSTER_PORT"))

	log.Infof(ctx, "CLUSTER_HOST: %s", clusterHost)
	log.Infof(ctx, "CLUSTER_PORT: %d", clusterPort)

	c := client.NewClient(ctx, clusterHost, clusterPort)

	start := time.Now()
	reminderCount := 10000
	log.Infof(ctx, "Creating %d reminders", reminderCount)
	for i := 0; i < reminderCount; i++ {
		if err := c.ScheduleReminder(&data.Reminder{
			ReminderName: fmt.Sprintf("RemindMeToGreet-%d", i),
			GrainType:    "Ohai",
			GrainId:      fmt.Sprintf("%d", i),
			Period:       10 * time.Second,
			FireAt:       time.Now(),
			Method:       "Greet",
			Data:         []byte("Mikey O'CouldntRecall"),
		}); err != nil {
			log.Warnf(ctx, "Unable to schedule reminder: %v", err)
		}
	}
	rate := float64(reminderCount) / time.Since(start).Seconds()
	log.Infof(ctx, "Scheduled %d reminders in %s", reminderCount, time.Since(start))
	log.Infof(ctx, "Rate: %f reminders/second", rate)

}

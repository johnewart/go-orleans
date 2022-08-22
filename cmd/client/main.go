package main

import (
	"context"
	"github.com/johnewart/go-orleans/client"
	"github.com/johnewart/go-orleans/grain"
	"os"
	"strconv"
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

}

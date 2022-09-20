package main

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/client"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-orleans/silo"
	"github.com/johnewart/go-orleans/util"
	"google.golang.org/grpc"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
	"zombiezen.com/go/log"
)

func main() {
	ctx := context.Background()
	port := os.Getenv("PORT")
	metricsPort := os.Getenv("METRICS_PORT")
	dsn := os.Getenv("DATABASE_URL")
	redisHostPort := os.Getenv("REDIS_HOST_PORT")
	grainType := os.Getenv("GRAIN_TYPE")

	log.Infof(ctx, "silo starting up...")
	log.Infof(ctx, "PORT: %s", port)
	log.Infof(ctx, "DATABASE_URL: %s", dsn)
	log.Infof(ctx, "METRICS_PORT: %d", metricsPort)
	log.Infof(ctx, "REDIS_HOST_PORT: %s", redisHostPort)
	log.Infof(ctx, "GRAIN_TYPE: %s", grainType)

	if ip, err := util.GetIP(); err != nil {
		log.Warnf(ctx, "failed to get ip: %v", err)
	} else {
		log.Infof(ctx, "ip: %s", ip)
		p, _ := strconv.Atoi(port)
		mp, _ := strconv.Atoi(metricsPort)
		heartbeatInterval := 5 * time.Second

		siloConfig := silo.SiloConfig{
			MetricsPort:      mp,
			ServicePort:      p,
			TableStoreDSN:    dsn,
			RedisHostPort:    redisHostPort,
			HearbeatInterval: heartbeatInterval,
			RoutableIP:       ip,
			ReminderInterval: 5 * time.Second,
		}
		if siloNode, err := silo.NewSilo(ctx, siloConfig); err != nil {

		} else {
			svcConfig := silo.ServiceConfig{
				Silo: siloNode,
			}

			siloService, svcErr := silo.NewSiloService(ctx, svcConfig)
			if svcErr != nil {
				log.Errorf(ctx, "failed to create silo service: %v", svcErr)
				os.Exit(-1)
			}

			helloGrain := silo.FunctionalGrainHandle{
				Handler: func(ctx context.Context, invocation *client.Invocation) (*client.GrainExecution, error) {
					log.Infof(ctx, "Handling grain of type %s@%s", invocation.GrainType, invocation.GrainID)
					data := invocation.Data
					message := fmt.Sprintf("Hello %s", string(data))
					log.Infof(ctx, "HelloWorld: %s", data)
					return &client.GrainExecution{
						Status: client.ExecutionSuccess,
						Result: []byte(message),
					}, nil
				},
			}

			sleepGrain := silo.FunctionalGrainHandle{
				Handler: func(ctx context.Context, invocation *client.Invocation) (*client.GrainExecution, error) {
					data := invocation.Data
					sleepTime, _ := strconv.Atoi(string(data))
					log.Infof(ctx, "Sleep grain will sleep for %d seconds...", sleepTime)

					time.Sleep(time.Duration(sleepTime) * time.Second)
					sleepZzs := make([]string, 0)
					for i := 0; i < sleepTime; i++ {
						sleepZzs = append(sleepZzs, "z")
					}
					response := fmt.Sprintf("%d Z%s...", sleepTime, strings.Join(sleepZzs, ""))
					return &client.GrainExecution{
						GrainID: invocation.GrainID,
						Status:  client.ExecutionSuccess,
						Result:  []byte(response),
					}, nil
				},
			}

			siloNode.RegisterHandler("HelloWorld", helloGrain)
			siloNode.RegisterHandler("Sleep", sleepGrain)

			go func() {
				log.Infof(ctx, "starting monitor process...")
				if sErr := siloNode.Start(); sErr != nil {
					log.Warnf(ctx, "failed to start monitor process: %v", sErr)
				}
			}()

			lis, listenErr := net.Listen("tcp", fmt.Sprintf(":%d", p))
			if listenErr != nil {
				log.Errorf(ctx, "failed to listen: %v", listenErr)
			}
			s := grpc.NewServer()

			log.Infof(ctx, "server listening at %v", lis.Addr())
			pb.RegisterSiloServiceServer(s, siloService)
			if sErr := s.Serve(lis); err != nil {
				log.Errorf(ctx, "unable to serve: %v", sErr)
				os.Exit(-1)
			}
		}
	}
}

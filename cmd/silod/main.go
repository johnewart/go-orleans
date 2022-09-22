package main

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/metrics"
	"github.com/johnewart/go-orleans/reminders"

	"github.com/johnewart/go-orleans/grains"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-orleans/services"
	"github.com/johnewart/go-orleans/silo"
	"github.com/johnewart/go-orleans/util"
	"github.com/joho/godotenv"

	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"google.golang.org/grpc"
	"zombiezen.com/go/log"
)

func main() {
	ctx := context.Background()

	err := godotenv.Load()
	if err != nil {
		log.Warnf(ctx, "Error loading .env file: %v", err)
	}

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

		routableHostPort := fmt.Sprintf("%s:%d", ip, p)

		metricsRegistry := metrics.NewMetricRegistry(mp)

		siloConfig := silo.SiloConfig{
			ServicePort:      p,
			TableStoreDSN:    dsn,
			RedisHostPort:    redisHostPort,
			HearbeatInterval: heartbeatInterval,
			RoutableIP:       ip,
			ReminderInterval: 5 * time.Second,
			Metrics:          metricsRegistry,
		}

		if siloNode, err := silo.NewSilo(ctx, siloConfig); err != nil {
			log.Errorf(ctx, "failed to create silo: %v", err)
		} else {
			svcConfig := services.ServiceConfig{
				Silo:    siloNode,
				Metrics: metricsRegistry,
			}

			siloService, svcErr := services.NewSiloService(ctx, svcConfig)
			if svcErr != nil {
				log.Errorf(ctx, "failed to create silo service: %v", svcErr)
				os.Exit(-1)
			}

			helloGrain := silo.FunctionalGrainHandle{
				Handler: func(ctx context.Context, invocation *grains.Invocation) (*grains.InvocationResult, error) {
					log.Infof(ctx, "FunctionalHandler.hello handling %s@%s", invocation.GrainType, invocation.GrainID)
					log.Infof(ctx, "FunctionalHandler.hello data: %v", invocation.Data)

					data := invocation.Data
					message := fmt.Sprintf("Hello %s", string(data))
					log.Infof(ctx, "HelloWorld: %s", message)
					return &grains.InvocationResult{
						Status:       grains.InvocationSuccess,
						Data:         []byte(message),
						InvocationId: invocation.InvocationId,
					}, nil
				},
			}

			sleepGrain := silo.FunctionalGrainHandle{
				Handler: func(ctx context.Context, invocation *grains.Invocation) (*grains.InvocationResult, error) {
					data := invocation.Data
					sleepTime, _ := strconv.Atoi(string(data))
					log.Infof(ctx, "Sleep grain will sleep for %d seconds...", sleepTime)

					time.Sleep(time.Duration(sleepTime) * time.Second)
					sleepZzs := make([]string, 0)
					for i := 0; i < sleepTime; i++ {
						sleepZzs = append(sleepZzs, "z")
					}
					response := fmt.Sprintf("%d Z%s...", sleepTime, strings.Join(sleepZzs, ""))
					return &grains.InvocationResult{
						Status:       grains.InvocationSuccess,
						Data:         []byte(response),
						InvocationId: invocation.InvocationId,
					}, nil
				},
			}

			siloNode.RegisterHandler("Ohai", helloGrain)
			siloNode.RegisterHandler("Sleep", sleepGrain)

			go func() {
				log.Infof(ctx, "starting monitor process...")
				if sErr := siloNode.Start(); sErr != nil {
					log.Warnf(ctx, "failed to start monitor process: %v", sErr)
				}
			}()

			lis, listenErr := net.Listen("tcp", fmt.Sprintf("0.0.0.0:%d", p))
			if listenErr != nil {
				log.Errorf(ctx, "failed to listen: %v", listenErr)
			}

			s := grpc.NewServer(
				grpc.UnaryInterceptor(siloService.ServerInterceptor),
			)

			pb.RegisterSiloServiceServer(s, siloService)

			// Internal pipe service
			pipe := ListenPipe()

			go func() {
				log.Infof(ctx, "starting internal silo service...")
				if err := s.Serve(pipe); err != nil {
					log.Errorf(ctx, "failed to serve: %v", err)
				}
			}()

			pipeConn, err := grpc.Dial(`pipe`,
				grpc.WithInsecure(),
				grpc.WithContextDialer(func(c context.Context, s string) (net.Conn, error) {
					return pipe.DialContext(c, `pipe`, s)
				}),
			)
			if err != nil {
				log.Errorf(ctx, "did not connect: %v", err)
			}

			c := pb.NewSiloServiceClient(pipeConn)

			reminderConfig := reminders.ReminderConfig{
				ReminderStoreDSN: dsn,
				TickInterval:     5 * time.Second,
				MetricsRegistry:  metricsRegistry,
				SiloClient:       c,
				SiloHostPort:     routableHostPort,
			}

			if reminderRegistry, err := reminders.NewReminderRegistry(ctx, reminderConfig); err != nil {
				log.Errorf(ctx, "failed to create reminder registry: %v", err)
			} else {
				reminderRegistry.StartReminderProcess()
				siloService.AddReminderRegistry(reminderRegistry)
			}

			// Start external gRPC service
			log.Infof(ctx, "server listening at %v", lis.Addr())
			if sErr := s.Serve(lis); err != nil {
				log.Errorf(ctx, "unable to serve: %v", sErr)
				os.Exit(-1)
			}
		}
	}

}

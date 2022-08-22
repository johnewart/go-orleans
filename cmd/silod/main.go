package main

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/client"
	"github.com/johnewart/go-orleans/grain"
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

	log.Infof(ctx, "silo starting up...")
	log.Infof(ctx, "PORT: %s", port)
	log.Infof(ctx, "DATABASE_URL: %s", dsn)
	log.Infof(ctx, "METRICS_PORT: %d", metricsPort)

	if ip, err := util.GetIP(); err != nil {
		log.Warnf(ctx, "failed to get ip: %v", err)
	} else {
		log.Infof(ctx, "ip: %s", ip)
		p, _ := strconv.Atoi(port)
		mp, _ := strconv.Atoi(metricsPort)
		heartbeatInterval := 5 * time.Second

		siloService, svcErr := silo.NewSiloService(ctx, mp, p, ip, heartbeatInterval, dsn)
		if svcErr != nil {
			log.Errorf(ctx, "failed to create silo service: %v", svcErr)
			os.Exit(-1)
		}

		if err = siloService.RegisterHandler("HelloWorld", func(ctx context.Context, grain *grain.Grain) (*client.GrainExecution, error) {
			data := grain.Data
			message := fmt.Sprintf("Hello %s", string(data))
			log.Infof(ctx, "HelloWorld: %s", data)
			return &client.GrainExecution{
				Status: client.ExecutionSuccess,
				Result: []byte(message),
			}, nil
		}); err != nil {
			log.Errorf(ctx, "failed to register HelloWorld handler: %v", err)
		}

		if err = siloService.RegisterHandler("Sleep", func(ctx context.Context, grain *grain.Grain) (*client.GrainExecution, error) {
			data := grain.Data
			sleepTime, _ := strconv.Atoi(string(data))
			log.Infof(ctx, "Sleep grain will sleep for %d seconds...", sleepTime)

			time.Sleep(time.Duration(sleepTime) * time.Second)
			sleepZzs := make([]string, 0)
			for i := 0; i < sleepTime; i++ {
				sleepZzs = append(sleepZzs, "z")
			}
			response := fmt.Sprintf("%d Z%s...", sleepTime, strings.Join(sleepZzs, ""))
			return &client.GrainExecution{
				GrainID: grain.ID,
				Status:  client.ExecutionSuccess,
				Result:  []byte(response),
			}, nil
		}); err != nil {
			log.Errorf(ctx, "failed to register Sleep handler: %v", err)
		}

		go func() {
			log.Infof(ctx, "starting monitor process...")
			if sErr := siloService.Start(); sErr != nil {
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

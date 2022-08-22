package main

import (
	"context"
	"fmt"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-orleans/silo"
	"github.com/johnewart/go-orleans/util"
	"google.golang.org/grpc"
	"net"
	"os"
	"strconv"
	"zombiezen.com/go/log"
)

func main() {
	ctx := context.Background()
	port := os.Getenv("PORT")
	dsn := os.Getenv("DATABASE_URL")

	log.Infof(ctx, "silo starting up...")
	log.Infof(ctx, "PORT: %s", port)
	log.Infof(ctx, "DATABASE_URL: %s", dsn)

	if ip, err := util.GetIP(); err != nil {
		log.Warnf(ctx, "failed to get ip: %v", err)
	} else {
		log.Infof(ctx, "ip: %s", ip)
		p, _ := strconv.Atoi(port)
		siloService, err := silo.NewSiloService(ctx, p, ip, 30, dsn)
		if err != nil {
			log.Errorf(ctx, "failed to create silo service: %v", err)
			os.Exit(-1)
		}

		go func() {
			log.Infof(ctx, "starting monitor process...")
			if err := siloService.Start(); err != nil {
				log.Warnf(ctx, "failed to start monitor process: %v", err)
			}
		}()

		lis, err := net.Listen("tcp", fmt.Sprintf(":%d", p))
		if err != nil {
			log.Errorf(ctx, "failed to listen: %v", err)
		}
		s := grpc.NewServer()

		log.Infof(ctx, "server listening at %v", lis.Addr())
		pb.RegisterSiloServiceServer(s, siloService)
		if err := s.Serve(lis); err != nil {
			log.Errorf(ctx, "unable to serve: %v", err)
			os.Exit(-1)
		}
	}

}

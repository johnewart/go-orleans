package silo

import (
	"context"
	"github.com/johnewart/go-orleans/membership"
	"github.com/johnewart/go-orleans/membership/storage"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"strconv"
	"sync"
	"time"
	"zombiezen.com/go/log"
)

type Service struct {
	pb.UnimplementedSiloServiceServer
	membershipStore            storage.MemberStore
	routableIP                 string
	servicePort                int
	startEpoch                 int64
	ctx                        context.Context
	heartbeatIntervalInSeconds int
}

func NewSiloService(ctx context.Context, servicePort int, routableIP string, heartbeatIntervalInSeconds int, dsn string) (*Service, error) {

	if store, err := storage.NewPostgresqlMemberStore(dsn); err != nil {
		return nil, err
	} else {
		return &Service{
			ctx:                        ctx,
			membershipStore:            store,
			routableIP:                 routableIP,
			servicePort:                servicePort,
			startEpoch:                 time.Now().UnixMicro(),
			heartbeatIntervalInSeconds: heartbeatIntervalInSeconds,
		}, nil
	}
}

func (s *Service) StartMonitorProcess() error {
	for {
		membershipTable, err := s.membershipStore.GetLatestTable()
		if err != nil {
			log.Warnf(s.ctx, "Unable to get membershipTable: %v", err)
		} else {
			log.Infof(s.ctx, "Pinging %d other membershipTable", len(membershipTable.Members))
			for _, m := range membershipTable.Members {
				if conn, err := grpc.Dial(m.IP+":"+strconv.Itoa(int(m.Port)), grpc.WithInsecure()); err != nil {
					log.Warnf(s.ctx, "Unable to dial %s:%d: %v", m.IP, m.Port, err)
				} else {
					if resp, err := pb.NewSiloServiceClient(conn).Ping(s.ctx, &emptypb.Empty{}); err != nil {
						log.Warnf(s.ctx, "Unable to ping %s:%d: %v", m.IP, m.Port, err)
						if err := s.membershipStore.Suspect(&m); err != nil {
							log.Warnf(s.ctx, "Unable to suspect %s:%d: %v", m.IP, m.Port, err)
						}
					} else {
						log.Infof(s.ctx, "Pinged %s:%d, epoch: %d", m.IP, m.Port, resp.Epoch)
					}
				}
			}
		}
		time.Sleep(time.Second * time.Duration(s.heartbeatIntervalInSeconds))
	}
}

func (s *Service) StartMembershipUpdateProcess() error {
	for {

		member := membership.Member{
			IP:    s.routableIP,
			Port:  s.servicePort,
			Epoch: s.startEpoch,
		}

		if err := s.membershipStore.Announce(&member); err != nil {
			log.Warnf(s.ctx, "Unable to announce ourselves: %v", err)
		}

		log.Infof(s.ctx, "Announced ourselves as %v -- will hearbeat again in %d seconds", member, s.heartbeatIntervalInSeconds)
		time.Sleep(time.Second * time.Duration(s.heartbeatIntervalInSeconds))
	}
}

func (s *Service) Ping(ctx context.Context, req *emptypb.Empty) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Epoch: s.startEpoch,
	}, nil
}

func (s *Service) Start() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		log.Infof(s.ctx, "Starting announcement process")
		if err := s.StartMembershipUpdateProcess(); err != nil {
			log.Warnf(s.ctx, "Unable to start announcement process: %v", err)
		}
	}()

	go func() {
		log.Infof(s.ctx, "Starting monitor process")
		if err := s.StartMonitorProcess(); err != nil {
			log.Warnf(s.ctx, "Unable to start monitor process: %v", err)
		}
	}()

	wg.Wait()
	return nil
}

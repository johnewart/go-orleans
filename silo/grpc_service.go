package silo

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/cluster"
	"github.com/johnewart/go-orleans/cluster/storage"
	"github.com/johnewart/go-orleans/cluster/table"
	"github.com/johnewart/go-orleans/grain"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"strconv"
	"strings"
	"sync"
	"time"
	"zombiezen.com/go/log"
)

type Service struct {
	pb.UnimplementedSiloServiceServer
	membershipTable   *table.MembershipTable
	routableIP        string
	servicePort       int
	startEpoch        int64
	ctx               context.Context
	heartbeatInterval time.Duration
	metrics           *MetricsRegistry
	silo              cluster.Member
	grainHandler      *GrainHandler
}

func NewSiloService(ctx context.Context, metricsPort int, servicePort int, routableIP string, heartbeatInterval time.Duration, dsn string) (*Service, error) {
	tableConfig := table.Config{
		SuspicionWindow: 60 * time.Second,
		SuspicionQuorum: 2,
	}

	if store, err := storage.NewPostgresqlMemberStore(dsn); err != nil {
		return nil, err
	} else {
		startEpoch := time.Now().UnixMicro()
		membershipTable := table.NewTable(ctx, store, tableConfig)
		member := cluster.Member{
			IP:    routableIP,
			Port:  servicePort,
			Epoch: startEpoch,
		}

		return &Service{
			ctx:               ctx,
			membershipTable:   membershipTable,
			routableIP:        routableIP,
			servicePort:       servicePort,
			startEpoch:        startEpoch,
			heartbeatInterval: heartbeatInterval,
			metrics:           NewMetricRegistry(metricsPort),
			silo:              member,
			grainHandler:      NewSiloGrainHandler(),
		}, nil
	}
}

func (s *Service) Ping(_ context.Context, _ *emptypb.Empty) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Epoch: s.startEpoch,
	}, nil
}

func (s *Service) ExecuteGrain(ctx context.Context, req *pb.ExecuteGrainRequest) (*pb.ExecuteGrainResponse, error) {
	log.Infof(ctx, "Can we handle this grain? (%v)", req.GrainType)
	if s.silo.CanHandle(req.GrainType) {
		log.Infof(ctx, "Executing grain %v", req.GrainType)
		if r, err := s.grainHandler.Handle(ctx, req.GrainType, req.Data); err != nil {
			return &pb.ExecuteGrainResponse{
				Status: pb.ExecutionStatus_EXECUTION_ERROR,
				Result: []byte(err.Error()),
			}, nil
		} else {
			return &pb.ExecuteGrainResponse{
				Status: pb.ExecutionStatus_EXECUTION_OK,
				Result: r.Result,
			}, nil
		}
	} else {
		log.Infof(ctx, "Grain %v is not compatible with silo %v", req.GrainType, s.silo)
		return &pb.ExecuteGrainResponse{
			Status: pb.ExecutionStatus_EXECUTION_NO_LONGER_ABLE,
		}, nil
	}

}

func (s *Service) PlaceGrain(ctx context.Context, req *pb.PlaceGrainRequest) (*pb.PlaceGrainResponse, error) {
	g := grain.Grain{
		ID:   -1,
		Type: req.GrainType,
		Data: []byte{},
	}

	target := s.membershipTable.GetSiloForGrain(g)

	if target == nil {
		return &pb.PlaceGrainResponse{
			Status: pb.PlacementStatus_PLACEMENT_NO_COMPATIBLE_SILO,
		}, nil
	} else {
		// Place grain
		return &pb.PlaceGrainResponse{
			Status: pb.PlacementStatus_PLACEMENT_OK,
		}, nil
	}

}

func (s *Service) StartMonitorProcess() error {
	for {
		s.metrics.UpdateTableSyncCount()
		suspects := make([]cluster.Member, 0)

		err := s.metrics.TimeTableSync(func() error {
			if err := s.membershipTable.Update(); err != nil {
				return fmt.Errorf("unable to update cluster table: %v", err)
			}

			log.Infof(s.ctx, "Pinging %d other members in table", s.membershipTable.Size())
			err := s.membershipTable.WithMembers(func(m *cluster.Member) error {
				if conn, err := grpc.Dial(m.IP+":"+strconv.Itoa(int(m.Port)), grpc.WithInsecure()); err != nil {
					log.Warnf(s.ctx, "Unable to dial %s:%d: %v", m.IP, m.Port, err)
					log.Infof(s.ctx, "Suspect that %v is dead", m)
					suspects = append(suspects, *m)
				} else {
					if resp, err := pb.NewSiloServiceClient(conn).Ping(s.ctx, &emptypb.Empty{}); err != nil {
						log.Warnf(s.ctx, "Unable to ping %s:%d: %v", m.IP, m.Port, err)
						log.Infof(s.ctx, "Suspect that %v is dead", m)
						suspects = append(suspects, *m)
					} else {
						log.Infof(s.ctx, "Pinged %s:%d, epoch: %d", m.IP, m.Port, resp.Epoch)
					}
				}

				return nil
			})

			if err != nil {
				log.Warnf(s.ctx, "Unable to ping all members: %v", err)
			}

			return nil
		})

		if err != nil {
			log.Warnf(s.ctx, "Error during table sync: %v", err)
		}

		for _, suspect := range suspects {
			log.Infof(s.ctx, "Suspect that %v is dead", suspect)
			if err := s.membershipTable.Suspect(&s.silo, &suspect); err != nil {
				log.Warnf(s.ctx, "Unable to suspect %v: %v", suspect, err)
			}
		}

		time.Sleep(s.heartbeatInterval)
	}
}

func (s *Service) StartMembershipUpdateProcess() error {
	for {

		if err := s.membershipTable.Announce(&s.silo); err != nil {
			log.Warnf(s.ctx, "Unable to announce ourselves: %v", err)
		}

		log.Infof(s.ctx, "Announced ourselves as %v -- will hearbeat again in %0.2f seconds", s.silo, s.heartbeatInterval.Seconds())
		time.Sleep(s.heartbeatInterval)
	}
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

	go func() {
		log.Infof(s.ctx, "Starting metrics service")
		if err := s.metrics.Serve(); err != nil {
			log.Warnf(s.ctx, "Unable to start metrics service: %v", err)
		}
	}()

	wg.Wait()
	return nil
}

func (s *Service) RegisterHandler(grainType string, f GrainHandlerFunc) error {
	if err := s.grainHandler.Register(grainType, f); err != nil {
		return err
	} else {
		for _, g := range s.silo.Grains {
			if g == grainType {
				return fmt.Errorf("grain type %s already registered", grainType)
			}
		}

		s.silo.Grains = append(s.silo.Grains, grainType)
		log.Infof(s.ctx, "Registered grain type %s", grainType)
		log.Infof(s.ctx, "Silo now has %d grain types: [%s]", len(s.silo.Grains), strings.Join(s.silo.Grains, ", "))
		log.Infof(s.ctx, "Re-announcing ourselves!")

		if err = s.membershipTable.Announce(&s.silo); err != nil {
			log.Warnf(s.ctx, "Unable to announce ourselves: %v", err)
			return fmt.Errorf("unable to announce new supported grain types: %v", err)
		} else {
			return nil
		}

	}
}

package silo

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/johnewart/go-orleans/client"
	"github.com/johnewart/go-orleans/cluster"
	"github.com/johnewart/go-orleans/cluster/storage"
	"github.com/johnewart/go-orleans/cluster/table"
	"github.com/johnewart/go-orleans/grain"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-orleans/silo/locator"
	"github.com/johnewart/go-orleans/silo/state/store"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
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
	grainHandler      *GrainRegistry
	grainLocator      locator.GrainLocator
	grainResponseMap  map[string]chan *client.GrainExecution
}

type ServiceConfig struct {
	RedisHostPort    string
	TableStoreDSN    string
	HearbeatInterval time.Duration
	RoutableIP       string
	MetricsPort      int
	ServicePort      int
}

func NewSiloService(ctx context.Context, config ServiceConfig) (*Service, error) {

	tableConfig := table.Config{
		SuspicionWindow: 60 * time.Second,
		SuspicionQuorum: 2,
	}

	locationStore := locator.NewRedisLocator(config.RedisHostPort)
	if !locationStore.Healthy() {
		return nil, fmt.Errorf("unable to connect to redis at %v", config.RedisHostPort)
	}

	stateStore := store.NewRedisGrainStateStore(config.RedisHostPort)
	if !stateStore.Healthy() {
		return nil, fmt.Errorf("unable to connect to redis at %v", config.RedisHostPort)
	}

	tableStore, err := storage.NewPostgresqlMemberStore(config.TableStoreDSN)
	if err != nil {
		return nil, err
	}

	startEpoch := time.Now().UnixMicro()
	membershipTable := table.NewTable(ctx, tableStore, tableConfig)
	member := cluster.Member{
		IP:    config.RoutableIP,
		Port:  config.ServicePort,
		Epoch: startEpoch,
	}

	return &Service{
		ctx:               ctx,
		membershipTable:   membershipTable,
		routableIP:        config.RoutableIP,
		servicePort:       config.ServicePort,
		startEpoch:        startEpoch,
		heartbeatInterval: config.HearbeatInterval,
		metrics:           NewMetricRegistry(config.MetricsPort),
		silo:              member,
		grainHandler:      NewSiloGrainRegistry(),
		grainLocator:      locationStore,
		grainResponseMap:  make(map[string]chan *client.GrainExecution),
	}, nil

}

func (s *Service) Ping(_ context.Context, _ *emptypb.Empty) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Epoch: s.startEpoch,
	}, nil
}

func (s *Service) ResultStream(stream pb.SiloService_ResultStreamServer) error {
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		log.Infof(s.ctx, "Received: %v", in)

		if ch, ok := s.grainResponseMap[in.RequestId]; ok {
			if in.GetResult() != nil {
				ch <- &client.GrainExecution{
					GrainID:   "grain-id",
					GrainType: "grain-type",
					Result:    in.GetResult(),
					Error:     nil,
					Status:    client.ExecutionSuccess,
				}
			}
		}
	}
}

func (s *Service) BounceExecutionRequest(ctx context.Context, targetHostPort string, req *pb.ExecuteGrainRequest) (*pb.ExecuteGrainResponse, error) {
	if conn, err := grpc.Dial(targetHostPort, grpc.WithInsecure()); err != nil {
		return nil, fmt.Errorf("unable to connect to %v: %v", targetHostPort, err)
	} else {
		client := pb.NewSiloServiceClient(conn)

		if res, bounceErr := client.ExecuteGrain(ctx, req); bounceErr != nil {
			return nil, fmt.Errorf("unable to bounce grain: %v", bounceErr)
		} else {
			return res, nil
		}
	}
}

func (s *Service) RegisterGrainHandler(req *pb.RegisterGrainHandlerRequest, stream pb.SiloService_RegisterGrainHandlerServer) error {
	log.Infof(s.ctx, "Registering remote grain handler for %v", req.GrainType)

	c := make(chan *client.Invocation, 100)
	handle := RemoteGrainHandle{
		channel: c,
	}

	s.RegisterHandler(req.GrainType, &handle)

	for {
		select {
		case invocation := <-c:
			log.Infof(s.ctx, "Execute %v", req.GrainType)
			if _, ok := s.grainResponseMap[invocation.InvocationId]; !ok {
				s.grainResponseMap[invocation.InvocationId] = make(chan *client.GrainExecution, 100)
			}

			stream.Send(&pb.GrainExecutionRequest{
				GrainType:  req.GrainType,
				Data:       []byte("hello"),
				GrainId:    "123",
				MethodName: "Hello",
				RequestId:  invocation.InvocationId,
			})
		}
	}
	//if err := s.grainHandler.Register(req.GrainType, func(ctx context.Context, g *grain.Grain) (*client.GrainExecution, error) {
	//	log.Infof(s.ctx, "Executing remote grain handler for %v with id %s", req.GrainType, g.ID)
	//	stream.Send(&pb.GrainExecutionRequest{
	//		RequestId:  uuid.New().String(),
	//		GrainType:  req.GrainType,
	//		GrainId:    g.ID,
	//		MethodName: "Hello",
	//		Data:       g.Data,
	//	})
	//	log.Infof(s.ctx, "Sent remote grain handler for %v with id %s", req.GrainType, g.ID)
	//	time.Sleep(10 * time.Second)
	//	return &client.GrainExecution{
	//		GrainID:   g.ID,
	//		Status:    client.ExecutionSuccess,
	//		Result:    []byte("Narf"),
	//		Error:     nil,
	//		GrainType: g.Type,
	//	}, nil
	//}); err != nil {
	//	log.Errorf(s.ctx, "unable to register grain handler: %v", err)
	//	return err
	//} else {
	//	s.Announce(req.GrainType)
	//	return nil
	//}
}

func (s *Service) ExecuteGrain(ctx context.Context, req *pb.ExecuteGrainRequest) (*pb.ExecuteGrainResponse, error) {
	existing, err := s.grainLocator.GetSilo(req.GrainType, req.GrainId)
	if err != nil {
		return nil, fmt.Errorf("unable to locate grain: %v", err)
	}

	shouldHandle := false

	if existing != nil {
		if (existing.IP == s.routableIP) && (existing.Port == s.servicePort) {
			log.Infof(s.ctx, "grain %v already located on this silo", req.GrainId)
			shouldHandle = true
		} else {
			log.Infof(ctx, "Grain %s/%s already exists at %v", req.GrainType, req.GrainId, existing)
			shouldHandle = false
			return s.BounceExecutionRequest(ctx, fmt.Sprintf("%v:%v", existing.IP, existing.Port), req)
		}
	} else {
		log.Infof(ctx, "Grain %s/%s does not exist", req.GrainType, req.GrainId)
		log.Infof(ctx, "Can we handle this grain? (%v)", req.GrainType)
		if s.silo.CanHandle(req.GrainType) {
			log.Infof(ctx, "Yes, we can handle this grain")

			if locErr := s.grainLocator.PutSilo(req.GrainType, req.GrainId, s.silo); locErr != nil {
				return nil, fmt.Errorf("unable to record grain existence in this silo: %v", locErr)
			}
			shouldHandle = true
		}
	}

	if shouldHandle {

		log.Infof(ctx, "Executing grain %v", req.GrainType)
		invocationId := uuid.New().String()
		invocation := &client.Invocation{
			GrainID:      req.GrainId,
			GrainType:    req.GrainType,
			MethodName:   "Invoke",
			Data:         req.Data,
			InvocationId: invocationId,
		}

		if resultChan, err := s.grainHandler.Handle(ctx, invocation); err != nil {
			return &pb.ExecuteGrainResponse{
				Status: pb.ExecutionStatus_EXECUTION_ERROR,
				Result: []byte(err.Error()),
			}, nil
		} else {

			if resultChan != nil {
				// Store channel for later remote callback
				s.grainResponseMap[invocationId] = resultChan
				log.Infof(ctx, "Waiting for result of grain execution...")
				select {
				case r := <-resultChan:
					log.Infof(ctx, "Got result of grain execution: %v", r)
					return &pb.ExecuteGrainResponse{
						Status: pb.ExecutionStatus_EXECUTION_OK,
						Result: r.Result,
					}, nil
					// TODO: timeout / cancel
				}
			} else {
				log.Infof(ctx, "No result channel returned")
				return &pb.ExecuteGrainResponse{
					Status: pb.ExecutionStatus_EXECUTION_OK,
					Result: []byte(""),
				}, nil
			}
		}
	} else {
		log.Infof(ctx, "Grain %v is not compatible with silo %v", req.GrainType, s.silo)

		log.Infof(ctx, "Locating compatible silo for %v", req.GrainType)
		for _, member := range s.membershipTable.Members {
			if member.CanHandle(req.GrainType) {
				log.Infof(ctx, "Bouncing grain %v to %v", req.GrainType, member)
				return s.BounceExecutionRequest(ctx, fmt.Sprintf("%v:%v", member.IP, member.Port), req)
			}
		}

		log.Infof(ctx, "No compatible silo found for %v", req.GrainType)
		return &pb.ExecuteGrainResponse{
			Status: pb.ExecutionStatus_EXECUTION_NO_LONGER_ABLE,
		}, nil
	}
}

func (s *Service) PlaceGrain(ctx context.Context, req *pb.PlaceGrainRequest) (*pb.PlaceGrainResponse, error) {
	g := grain.Grain{
		ID:   req.GrainId,
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

func (s *Service) RegisterHandler(grainType string, handle GrainHandle) {
	s.grainHandler.Register(grainType, handle)
	s.Announce(grainType)
}
func (s *Service) Announce(grainType string) error {
	for _, g := range s.silo.Grains {
		if g == grainType {
			return fmt.Errorf("grain type %s already registered", grainType)
		}
	}

	s.silo.Grains = append(s.silo.Grains, grainType)
	log.Infof(s.ctx, "Registered grain type %s", grainType)
	log.Infof(s.ctx, "Silo now has %d grain types: [%s]", len(s.silo.Grains), strings.Join(s.silo.Grains, ", "))
	log.Infof(s.ctx, "Re-announcing ourselves!")

	if err := s.membershipTable.Announce(&s.silo); err != nil {
		log.Warnf(s.ctx, "Unable to announce ourselves: %v", err)
		return fmt.Errorf("unable to announce new supported grain types: %v", err)
	} else {
		return nil
	}

}

package services

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/johnewart/go-orleans/grains"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-orleans/silo"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"zombiezen.com/go/log"
)

type GrainResponseMap struct {
	sync.Map
}

func (g *GrainResponseMap) LoadChannel(key string) (chan *grains.GrainExecution, bool) {
	if item, ok := g.Load(key); !ok {
		log.Infof(context.Background(), "Unable to find channel for %s", key)
		return nil, false
	} else {
		log.Infof(context.Background(), "Found channel for request %s", key)
		return item.(chan *grains.GrainExecution), true
	}
}

func (g *GrainResponseMap) StoreChannel(requestId string, c chan *grains.GrainExecution) error {
	log.Infof(context.Background(), "Storing channel for request id %s", requestId)
	g.Store(requestId, c)
	return nil
}

type Service struct {
	pb.UnimplementedSiloServiceServer

	ctx              context.Context
	silo             *silo.Silo
	grainResponseMap *GrainResponseMap
}

type ServiceConfig struct {
	Silo *silo.Silo
}

func NewSiloService(ctx context.Context, config ServiceConfig) (*Service, error) {
	return &Service{
		ctx:              ctx,
		silo:             config.Silo,
		grainResponseMap: &GrainResponseMap{}, // //make(map[string]chan *grains.GrainExecution),
	}, nil
}

func (s *Service) Ping(_ context.Context, _ *emptypb.Empty) (*pb.PingResponse, error) {
	return &pb.PingResponse{
		Epoch: s.silo.Ping(),
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

		if ch, ok := s.grainResponseMap.LoadChannel(in.RequestId); ok {
			log.Infof(s.ctx, "Found channel for grain response!")
			if in.GetResult() != nil {
				ch <- &grains.GrainExecution{
					GrainID:   "grains-id",
					GrainType: "grains-type",
					Result:    in.GetResult(),
					Error:     nil,
					Status:    grains.ExecutionSuccess,
				}
			}
		} else {
			log.Infof(s.ctx, "Unable to find channel for request: %s", in.RequestId)
		}
	}
}

func (s *Service) BounceExecutionRequest(ctx context.Context, targetHostPort string, req *pb.ExecuteGrainRequest) (*pb.ExecuteGrainResponse, error) {
	if conn, err := grpc.Dial(targetHostPort, grpc.WithInsecure()); err != nil {
		return nil, fmt.Errorf("unable to connect to %v: %v", targetHostPort, err)
	} else {
		client := pb.NewSiloServiceClient(conn)

		if res, bounceErr := client.ExecuteGrain(ctx, req); bounceErr != nil {
			return nil, fmt.Errorf("unable to bounce grains: %v", bounceErr)
		} else {
			return res, nil
		}
	}
}

func (s *Service) RegisterGrainHandler(req *pb.RegisterGrainHandlerRequest, stream pb.SiloService_RegisterGrainHandlerServer) error {
	log.Infof(s.ctx, "Registering remote grains handler for %v", req.GrainType)

	c := make(chan *grains.Invocation, 100)
	handle := silo.RemoteGrainHandle{
		Channel: c,
	}

	s.silo.RegisterHandler(req.GrainType, &handle)

	for {
		select {
		case invocation := <-c:
			log.Infof(s.ctx, "Execute %v", req.GrainType)

			if _, ok := s.grainResponseMap.LoadChannel(invocation.InvocationId); !ok {
				s.grainResponseMap.StoreChannel(invocation.InvocationId, make(chan *grains.GrainExecution, 100))
			}

			stream.Send(&pb.GrainExecutionRequest{
				GrainType:  req.GrainType,
				Data:       invocation.Data,
				GrainId:    invocation.GrainID,
				MethodName: invocation.MethodName,
				RequestId:  invocation.InvocationId,
			})
		}
	}
}

func (s *Service) ExecuteGrain(ctx context.Context, req *pb.ExecuteGrainRequest) (*pb.ExecuteGrainResponse, error) {
	existing, err := s.silo.GetSilo(req.GrainType, req.GrainId)
	if err != nil {
		return nil, fmt.Errorf("unable to locate grains: %v", err)
	}

	shouldHandle := false

	if existing != nil {
		if s.silo.IsLocal(existing) {
			log.Infof(s.ctx, "grains %v already located on this silo", req.GrainId)
			shouldHandle = true
		} else {
			log.Infof(ctx, "Grain %s/%s already exists at %v", req.GrainType, req.GrainId, existing)
			shouldHandle = false
			return s.BounceExecutionRequest(ctx, fmt.Sprintf("%v:%v", existing.IP, existing.Port), req)
		}
	} else {
		log.Infof(ctx, "Grain %s/%s does not exist", req.GrainType, req.GrainId)
		log.Infof(ctx, "Can we handle this grains? (%v)", req.GrainType)
		if s.silo.CanHandle(req.GrainType) {
			log.Infof(ctx, "Yes, we can handle this grains")

			if locErr := s.silo.RegisterGrain(req.GrainType, req.GrainId); locErr != nil {
				return nil, fmt.Errorf("unable to record grains existence in this silo: %v", locErr)
			}
			shouldHandle = true
		}
	}

	if shouldHandle {

		log.Infof(ctx, "Executing grains %v", req.GrainType)
		invocationId := uuid.New().String()
		invocation := &grains.Invocation{
			GrainID:      req.GrainId,
			GrainType:    req.GrainType,
			MethodName:   "Invoke",
			Data:         req.Data,
			InvocationId: invocationId,
		}

		if resultChan, err := s.silo.Handle(ctx, invocation); err != nil {
			return &pb.ExecuteGrainResponse{
				Status: pb.ExecutionStatus_EXECUTION_ERROR,
				Result: []byte(err.Error()),
			}, nil
		} else {

			if resultChan != nil {
				// Store channel for later remote callback
				s.grainResponseMap.StoreChannel(invocationId, resultChan)
				log.Infof(ctx, "Waiting for result of grains execution...")
				select {
				case r := <-resultChan:
					log.Infof(ctx, "Got result of grains execution: %v", r)
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
		compatibleSilo := s.silo.Locate(req.GrainType)
		if compatibleSilo != nil {
			log.Infof(ctx, "Bouncing grains %v to %v", req.GrainType, compatibleSilo)
			return s.BounceExecutionRequest(ctx, fmt.Sprintf("%v:%v", compatibleSilo.IP, compatibleSilo.Port), req)
		}

		log.Infof(ctx, "No compatible silo found for %v", req.GrainType)
		return &pb.ExecuteGrainResponse{
			Status: pb.ExecutionStatus_EXECUTION_NO_LONGER_ABLE,
		}, nil
	}
}

func (s *Service) RegisterReminder(ctx context.Context, req *pb.RegisterReminderRequest) (*pb.RegisterReminderResponse, error) {
	dueTime := time.UnixMilli(int64(req.DueTime))
	period := time.Duration(req.Period) * time.Second

	if err := s.silo.RegisterReminder(req.ReminderName, req.GrainType, req.GrainId, dueTime, period); err != nil {
		return nil, fmt.Errorf("unable to register reminder: %v", err)
	} else {
		return &pb.RegisterReminderResponse{
			ReminderId: req.ReminderName,
		}, nil
	}
}

func (s *Service) PlaceGrain(ctx context.Context, req *pb.PlaceGrainRequest) (*pb.PlaceGrainResponse, error) {
	g := grains.Grain{
		ID:   req.GrainId,
		Type: req.GrainType,
		Data: []byte{},
	}

	target := s.silo.GetSiloForGrain(g)

	if target == nil {
		return &pb.PlaceGrainResponse{
			Status: pb.PlacementStatus_PLACEMENT_NO_COMPATIBLE_SILO,
		}, nil
	} else {
		// Place grains
		return &pb.PlaceGrainResponse{
			Status: pb.PlacementStatus_PLACEMENT_OK,
		}, nil
	}

}

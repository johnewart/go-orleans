package services

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/metrics"
	"github.com/johnewart/go-orleans/reminders"
	"google.golang.org/grpc"
	"io"
	"time"

	"github.com/johnewart/go-orleans/grains"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-orleans/silo"
	"google.golang.org/protobuf/types/known/emptypb"
	"zombiezen.com/go/log"
)

type Service struct {
	pb.UnimplementedSiloServiceServer

	ctx              context.Context
	silo             *silo.Silo
	metrics          *metrics.MetricsRegistry
	reminderRegistry *reminders.ReminderRegistry
}

type ServiceConfig struct {
	Silo    *silo.Silo
	Metrics *metrics.MetricsRegistry
}

func NewSiloService(ctx context.Context, config ServiceConfig) (*Service, error) {
	return &Service{
		ctx:     ctx,
		silo:    config.Silo,
		metrics: config.Metrics,
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
		log.Infof(s.ctx, "Result stream read: %v", in)

		var result *grains.InvocationResult
		if in.GetResult() != nil {
			result = &grains.InvocationResult{
				Data:         in.GetResult(),
				InvocationId: in.RequestId,
				Status:       grains.InvocationSuccess,
			}
		} else {
			result = &grains.InvocationResult{
				Data:         []byte(in.GetError()),
				InvocationId: in.RequestId,
				Status:       grains.InvocationFailure,
			}
		}

		s.silo.HandleInvocationResult(result)
	}
}

func (s *Service) RegisterGrainHandler(req *pb.RegisterGrainHandlerRequest, stream pb.SiloService_RegisterGrainHandlerServer) error {
	log.Infof(s.ctx, "Registering remote grains handler for %v", req.GrainType)

	c := make(chan *grains.Invocation, 1)
	handle := silo.RemoteGrainHandle{
		Channel: c,
	}

	s.silo.RegisterHandler(req.GrainType, &handle)

	for {
		select {
		case invocation := <-c:
			log.Infof(s.ctx, "Execute %v", req.GrainType)

			stream.Send(&pb.GrainInvocationRequest{
				GrainType:  req.GrainType,
				Data:       invocation.Data,
				GrainId:    invocation.GrainID,
				MethodName: invocation.MethodName,
				RequestId:  invocation.InvocationId,
			})
		}
	}
}

func (s *Service) InvokeGrain(ctx context.Context, req *pb.GrainInvocationRequest) (*pb.GrainInvocationResponse, error) {
	invocation := &grains.Invocation{
		GrainID:      req.GrainId,
		GrainType:    req.GrainType,
		MethodName:   req.MethodName,
		Data:         req.Data,
		InvocationId: req.RequestId,
	}

	if result, err := s.silo.HandleInvocation(ctx, invocation); err != nil {
		//return nil, err
		return &pb.GrainInvocationResponse{
			Status: pb.ExecutionStatus_EXECUTION_ERROR,
			Result: []byte(err.Error()),
		}, nil
	} else {
		return &pb.GrainInvocationResponse{
			Result:  result.Data,
			GrainId: invocation.GrainID,
			Status:  pb.ExecutionStatus_EXECUTION_OK,
		}, nil
	}

}

func (s *Service) RegisterReminder(ctx context.Context, req *pb.RegisterReminderRequest) (*pb.RegisterReminderResponse, error) {
	dueTime := time.UnixMilli(int64(req.DueTime))
	period := time.Duration(req.Period) * time.Second
	if s.reminderRegistry != nil {
		if err := s.reminderRegistry.Register(req.ReminderName, req.GrainType, req.GrainId, dueTime, period); err != nil {
			return nil, fmt.Errorf("unable to register reminder: %v", err)
		} else {
			return &pb.RegisterReminderResponse{
				ReminderId: req.ReminderName,
			}, nil
		}
	} else {
		return nil, fmt.Errorf("no reminder registry available")
	}
}

func (s *Service) PlaceGrain(ctx context.Context, req *pb.PlaceGrainRequest) (*pb.PlaceGrainResponse, error) {
	g := grains.Grain{
		ID:   req.GrainId,
		Type: req.GrainType,
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

func (s *Service) ServerInterceptor(ctx context.Context,
	req interface{},
	info *grpc.UnaryServerInfo,
	handler grpc.UnaryHandler) (interface{}, error) {

	return s.metrics.TimeGRPCEndpoint(info.FullMethod, func() (interface{}, error) {
		// Calls the handler
		h, err := handler(ctx, req)

		return h, err
	})
}

func (s *Service) AddReminderRegistry(registry *reminders.ReminderRegistry) {
	s.reminderRegistry = registry
}

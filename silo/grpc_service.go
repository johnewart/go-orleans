package silo

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/johnewart/go-orleans/client"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"io"
	"zombiezen.com/go/log"
)

type Service struct {
	pb.UnimplementedSiloServiceServer

	ctx              context.Context
	silo             *Silo
	grainResponseMap map[string]chan *client.GrainExecution
}

type ServiceConfig struct {
	Silo *Silo
}

func NewSiloService(ctx context.Context, config ServiceConfig) (*Service, error) {
	return &Service{
		ctx:              ctx,
		silo:             config.Silo,
		grainResponseMap: make(map[string]chan *client.GrainExecution),
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

	s.silo.RegisterHandler(req.GrainType, &handle)

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
}

func (s *Service) ExecuteGrain(ctx context.Context, req *pb.ExecuteGrainRequest) (*pb.ExecuteGrainResponse, error) {
	existing, err := s.silo.GetSilo(req.GrainType, req.GrainId)
	if err != nil {
		return nil, fmt.Errorf("unable to locate grain: %v", err)
	}

	shouldHandle := false

	if existing != nil {
		if s.silo.IsLocal(existing) {
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

			if locErr := s.silo.RegisterGrain(req.GrainType, req.GrainId); locErr != nil {
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

		if resultChan, err := s.silo.Handle(ctx, invocation); err != nil {
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
		compatibleSilo := s.silo.Locate(req.GrainType)
		if compatibleSilo != nil {
			log.Infof(ctx, "Bouncing grain %v to %v", req.GrainType, compatibleSilo)
			return s.BounceExecutionRequest(ctx, fmt.Sprintf("%v:%v", compatibleSilo.IP, compatibleSilo.Port), req)
		}

		log.Infof(ctx, "No compatible silo found for %v", req.GrainType)
		return &pb.ExecuteGrainResponse{
			Status: pb.ExecutionStatus_EXECUTION_NO_LONGER_ABLE,
		}, nil
	}
}

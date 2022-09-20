package client

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/grain"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"google.golang.org/grpc"
	"time"
	"zombiezen.com/go/log"
)

type Invocation struct {
	GrainID    string
	GrainType  string
	MethodName string
	Data       []byte
	Context    context.Context
}

type GrainHandler interface {
	Handle(*Invocation) error
	//Run(chan *Invocation) error
}

type GrainMetadata struct {
	Type    string
	Version string
}

type Client struct {
	clusterHost   string
	clusterPort   int
	connection    *grpc.ClientConn
	pbClient      pb.SiloServiceClient
	ctx           context.Context
	grainHandlers map[GrainMetadata]GrainHandler
}

type ExecutionStatus int

const (
	ExecutionError ExecutionStatus = iota
	ExecutionSuccess
	ExecutionNoLongerAbleToRun
)

type ScheduleStatus int

const (
	ScheduleError ScheduleStatus = iota
	ScheduleSuccess
)

type GrainCommandStreamHandler struct {
	stream  pb.SiloService_RegisterGrainHandlerClient
	ctx     context.Context
	handler GrainHandler
}

func (h *GrainCommandStreamHandler) Run() {
	log.Infof(h.ctx, "Starting command stream handler")
	for {
		if command, err := h.stream.Recv(); err != nil {
			log.Warnf(h.ctx, "Unable to receive command: %v", err)
			time.Sleep(2 * time.Second)
		} else {
			log.Infof(h.ctx, "Received command: %s", command)
			invocation := &Invocation{
				GrainID:    command.GrainId,
				GrainType:  command.GrainType,
				MethodName: command.MethodName,
				Data:       command.Data,
				Context:    h.ctx,
			}
			h.handler.Handle(invocation)
		}
	}
}

type GrainExecution struct {
	GrainID   string
	Status    ExecutionStatus
	Result    []byte
	Error     error
	GrainType string
}

func (ge *GrainExecution) IsSuccessful() bool {
	return ge.Status == ExecutionSuccess
}

func (ge *GrainExecution) String() string {
	return fmt.Sprintf("GrainExecution{GrainID: %s, Status: %d, Result: %s, Error: %v}", ge.GrainID, ge.Status, ge.Result, ge.Error)
}

func NewClient(ctx context.Context, clusterHost string, clusterPort int) *Client {
	connAddr := fmt.Sprintf("%s:%d", clusterHost, clusterPort)
	if conn, err := grpc.Dial(connAddr, grpc.WithInsecure()); err != nil {
		log.Warnf(ctx, "Unable to dial %s: %v", connAddr, err)
		return nil
	} else {

		return &Client{
			clusterHost:   clusterHost,
			clusterPort:   clusterPort,
			ctx:           ctx,
			grainHandlers: make(map[GrainMetadata]GrainHandler),
			pbClient:      pb.NewSiloServiceClient(conn),
		}
	}
}

func (c *Client) RegisterGrainHandler(grainMetadata GrainMetadata, handler GrainHandler) (*GrainCommandStreamHandler, error) {

	req := &pb.RegisterGrainHandlerRequest{
		GrainType:    grainMetadata.Type,
		GrainVersion: grainMetadata.Version,
	}
	if commandStream, err := c.pbClient.RegisterGrainHandler(c.ctx, req); err != nil {
		return nil, fmt.Errorf("unable to register grain handler %s/%s: %v", grainMetadata.Type, grainMetadata.Version, err)
	} else {
		c.grainHandlers[grainMetadata] = handler

		return &GrainCommandStreamHandler{
			stream:  commandStream,
			ctx:     c.ctx,
			handler: handler,
		}, nil

	}

}

func (c *Client) ScheduleGrain(grain *grain.Grain) GrainExecution {
	req := &pb.ExecuteGrainRequest{
		GrainId:   grain.ID,
		GrainType: grain.Type,
		Data:      grain.Data,
	}
	if result, err := c.pbClient.ExecuteGrain(c.ctx, req); err != nil {
		return GrainExecution{
			GrainID:   grain.ID,
			GrainType: grain.Type,
			Status:    ExecutionError,
			Error:     fmt.Errorf("unable to schedule grain: %v", err),
		}
	} else {
		if result.Status == pb.ExecutionStatus_EXECUTION_NO_LONGER_ABLE {
			return GrainExecution{
				GrainID:   grain.ID,
				GrainType: grain.Type,
				Status:    ExecutionNoLongerAbleToRun,
				Error:     fmt.Errorf("unable to schedule grain %s@%s, silo is no longer able to run it", grain.Type, grain.ID),
			}
		}
		if result.Status == pb.ExecutionStatus_EXECUTION_OK {
			return GrainExecution{
				GrainID:   grain.ID,
				GrainType: grain.Type,
				Status:    ExecutionSuccess,
				Result:    result.Result,
			}
		} else {
			return GrainExecution{
				GrainID:   grain.ID,
				GrainType: grain.Type,
				Status:    ExecutionError,
				Error:     fmt.Errorf("unable to schedule grain: %s", result.Result),
			}
		}
	}
}

func (c *Client) ScheduleGrainAsync(grain *grain.Grain, callback func(*GrainExecution)) context.Context {
	asyncContext := context.Background()

	go func(ctx context.Context) {
		// TODO: handle cancellation
		log.Infof(ctx, "Scheduling grain asynchronously")
		result := c.ScheduleGrain(grain)
		log.Infof(ctx, "Grain result: %s", result.Result)
		callback(&result)
	}(asyncContext)

	return asyncContext
}

package client

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/grains"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-orleans/silo"
	"google.golang.org/grpc"
	"time"
	"zombiezen.com/go/log"
)

type GrainHandler interface {
	Handle(*grains.Invocation) (grains.GrainExecution, error)
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

type GrainCommandStreamHandler struct {
	stream       pb.SiloService_RegisterGrainHandlerClient
	ctx          context.Context
	handler      GrainHandler
	resultStream pb.SiloService_ResultStreamClient
}

func (h *GrainCommandStreamHandler) Run() {
	log.Infof(h.ctx, "Starting command stream handler")
	for {
		if command, err := h.stream.Recv(); err != nil {
			log.Warnf(h.ctx, "Unable to receive command: %v", err)
			time.Sleep(2 * time.Second)
		} else {
			log.Infof(h.ctx, "Received command: %s", command)
			invocation := &grains.Invocation{
				GrainID:      command.GrainId,
				GrainType:    command.GrainType,
				MethodName:   command.MethodName,
				Data:         command.Data,
				InvocationId: command.RequestId,
				Context:      h.ctx,
			}
			if result, err := h.handler.Handle(invocation); err != nil {
				h.resultStream.Send(&pb.GrainExecutionResult{
					RequestId: command.RequestId,
					Status:    pb.ExecutionStatus_EXECUTION_ERROR,
					TestResult: &pb.GrainExecutionResult_Error{
						Error: err.Error(),
					},
				})
			} else {
				h.resultStream.Send(&pb.GrainExecutionResult{
					RequestId: invocation.InvocationId,
					Status:    pb.ExecutionStatus_EXECUTION_OK,
					TestResult: &pb.GrainExecutionResult_Result{
						Result: result.Result,
					},
				})
			}

		}
	}
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

	if resultStream, err := c.pbClient.ResultStream(c.ctx); err != nil {
		return nil, fmt.Errorf("unable to open result stream for %s/%s: %v", grainMetadata.Type, grainMetadata.Version, err)
	} else {
		if commandStream, err := c.pbClient.RegisterGrainHandler(c.ctx, req); err != nil {
			return nil, fmt.Errorf("unable to register grains handler %s/%s: %v", grainMetadata.Type, grainMetadata.Version, err)
		} else {
			c.grainHandlers[grainMetadata] = handler

			return &GrainCommandStreamHandler{
				stream:       commandStream,
				resultStream: resultStream,
				ctx:          c.ctx,
				handler:      handler,
			}, nil

		}
	}

}

func (c *Client) ScheduleGrain(g *grains.Grain) grains.GrainExecution {
	req := &pb.ExecuteGrainRequest{
		GrainId:   g.ID,
		GrainType: g.Type,
		Data:      g.Data,
	}
	if result, err := c.pbClient.ExecuteGrain(c.ctx, req); err != nil {
		return grains.GrainExecution{
			GrainID:   g.ID,
			GrainType: g.Type,
			Status:    grains.ExecutionError,
			Error:     fmt.Errorf("unable to schedule grains: %v", err),
		}
	} else {
		if result.Status == pb.ExecutionStatus_EXECUTION_NO_LONGER_ABLE {
			return grains.GrainExecution{
				GrainID:   g.ID,
				GrainType: g.Type,
				Status:    grains.ExecutionNoLongerAbleToRun,
				Error:     fmt.Errorf("unable to schedule grains %s@%s, silo is no longer able to run it", g.Type, g.ID),
			}
		}
		if result.Status == pb.ExecutionStatus_EXECUTION_OK {
			return grains.GrainExecution{
				GrainID:   g.ID,
				GrainType: g.Type,
				Status:    grains.ExecutionSuccess,
				Result:    result.Result,
			}
		} else {
			return grains.GrainExecution{
				GrainID:   g.ID,
				GrainType: g.Type,
				Status:    grains.ExecutionError,
				Error:     fmt.Errorf("unable to schedule grains: %s", result.Result),
			}
		}
	}
}

func (c *Client) ScheduleGrainAsync(grain *grains.Grain, callback func(*grains.GrainExecution)) context.Context {
	asyncContext := context.Background()

	go func(ctx context.Context) {
		// TODO: handle cancellation
		log.Infof(ctx, "Scheduling grains asynchronously")
		result := c.ScheduleGrain(grain)
		log.Infof(ctx, "Grain result: %s", result.Result)
		callback(&result)
	}(asyncContext)

	return asyncContext
}

func (c *Client) ScheduleReminder(reminder *silo.Reminder) error {
	req := &pb.RegisterReminderRequest{
		GrainId:      reminder.GrainId,
		GrainType:    reminder.GrainType,
		ReminderName: reminder.ReminderName,
		Period:       int64(reminder.Period.Seconds()),
		DueTime:      reminder.FireAt.UnixMilli(),
		Data:         reminder.Data,
	}

	if _, err := c.pbClient.RegisterReminder(c.ctx, req); err != nil {
		return fmt.Errorf("unable to schedule reminder: %v", err)
	} else {
		return nil
	}
}

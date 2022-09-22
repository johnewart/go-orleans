package client

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/grains"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-orleans/reminders/data"
	"google.golang.org/grpc"
	"os"
	"strconv"
	"zombiezen.com/go/log"
)

type clusterClient struct {
	clusterHost   string
	clusterPort   int
	connection    *grpc.ClientConn
	pbClient      pb.SiloServiceClient
	ctx           context.Context
	grainHandlers map[GrainMetadata]GrainHandler
}

func NewClient(ctx context.Context, clusterHost string, clusterPort int) *clusterClient {
	connAddr := fmt.Sprintf("%s:%d", clusterHost, clusterPort)
	if conn, err := grpc.Dial(connAddr, grpc.WithInsecure()); err != nil {
		log.Warnf(ctx, "Unable to dial %s: %v", connAddr, err)
		return nil
	} else {

		return &clusterClient{
			clusterHost:   clusterHost,
			clusterPort:   clusterPort,
			ctx:           ctx,
			grainHandlers: make(map[GrainMetadata]GrainHandler),
			pbClient:      pb.NewSiloServiceClient(conn),
		}
	}
}

func NewClientFromEnv(ctx context.Context) *clusterClient {
	clusterHost := os.Getenv("CLUSTER_HOST")
	clusterPort, _ := strconv.Atoi(os.Getenv("CLUSTER_PORT"))

	log.Infof(ctx, "CLUSTER_HOST: %s", clusterHost)
	log.Infof(ctx, "CLUSTER_PORT: %d", clusterPort)

	return NewClient(ctx, clusterHost, clusterPort)
}

func (c *clusterClient) RegisterGrainHandler(grainMetadata GrainMetadata, handler GrainHandler) (*GrainCommandStreamHandler, error) {

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

func (c *clusterClient) InvokeGrain(invocation *grains.Invocation) grains.GrainExecution {
	req := &pb.GrainInvocationRequest{
		GrainId:    invocation.GrainID,
		GrainType:  invocation.GrainType,
		Data:       invocation.Data,
		MethodName: invocation.MethodName,
		RequestId:  invocation.InvocationId,
	}

	if result, err := c.pbClient.InvokeGrain(c.ctx, req); err != nil {
		return grains.GrainExecution{
			GrainID:   invocation.GrainID,
			GrainType: invocation.GrainType,
			Status:    grains.ExecutionError,
			Error:     fmt.Errorf("unable to schedule grain: %v", err),
		}
	} else {
		if result.Status == pb.ExecutionStatus_EXECUTION_NO_LONGER_ABLE {
			return grains.GrainExecution{
				GrainID:   invocation.GrainID,
				GrainType: invocation.GrainType,
				Status:    grains.ExecutionNoLongerAbleToRun,
				Error:     fmt.Errorf("unable to schedule grain %s@%s, silo is no longer able to run it", invocation.GrainType, invocation.GrainID),
			}
		}
		if result.Status == pb.ExecutionStatus_EXECUTION_OK {
			return grains.GrainExecution{
				GrainID:   invocation.GrainID,
				GrainType: invocation.GrainType,
				Status:    grains.ExecutionSuccess,
				Result:    result.Result,
			}
		} else {
			return grains.GrainExecution{
				GrainID:   invocation.GrainID,
				GrainType: invocation.GrainType,
				Status:    grains.ExecutionError,
				Error:     fmt.Errorf("unable to schedule grain: %s", result.Result),
			}
		}
	}
}

func (c *clusterClient) ScheduleGrainAsync(invocation *grains.Invocation, callback func(*grains.GrainExecution)) context.Context {
	asyncContext := context.Background()

	go func(ctx context.Context) {
		// TODO: handle cancellation
		log.Infof(ctx, "Scheduling grains asynchronously")
		result := c.InvokeGrain(invocation)
		log.Infof(ctx, "Grain result: %s", result.Result)
		callback(&result)
	}(asyncContext)

	return asyncContext
}

func (c *clusterClient) ScheduleReminder(reminder *data.Reminder) error {
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

package client

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/grain"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"google.golang.org/grpc"
	"zombiezen.com/go/log"
)

type Client struct {
	clusterHost string
	clusterPort int
	ctx         context.Context
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

type GrainExecution struct {
	GrainID   string
	Status    ExecutionStatus
	Result    []byte
	Error     error
	GrainType string
}

func NewClient(ctx context.Context, clusterHost string, clusterPort int) *Client {
	return &Client{
		clusterHost: clusterHost,
		clusterPort: clusterPort,
		ctx:         ctx,
	}
}

func (c *Client) WithClient(f func(context.Context, pb.SiloServiceClient) GrainExecution) GrainExecution {
	connAddr := fmt.Sprintf("%s:%d", c.clusterHost, c.clusterPort)
	if conn, err := grpc.Dial(connAddr, grpc.WithInsecure()); err != nil {
		log.Warnf(c.ctx, "Unable to dial %s: %v", connAddr, err)
		return GrainExecution{
			Status: ExecutionError,
			Error:  fmt.Errorf("unable to dial %s: %v", connAddr, err),
		}
	} else {
		client := pb.NewSiloServiceClient(conn)
		return f(c.ctx, client)
	}
}

func (c *Client) ScheduleGrain(grain *grain.Grain) GrainExecution {
	return c.WithClient(func(ctx context.Context, client pb.SiloServiceClient) GrainExecution {
		req := &pb.ExecuteGrainRequest{
			GrainId:   grain.ID,
			GrainType: grain.Type,
			Data:      grain.Data,
		}
		if result, err := client.ExecuteGrain(ctx, req); err != nil {
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
					Error:     fmt.Errorf("unable to schedule grain, silo is no longer able to run it"),
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
	})
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

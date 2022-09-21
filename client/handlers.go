package client

import (
	"context"
	"github.com/johnewart/go-orleans/grains"
	pb "github.com/johnewart/go-orleans/proto/silo"
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
				h.resultStream.Send(&pb.GrainInvocationResult{
					RequestId: command.RequestId,
					Status:    pb.ExecutionStatus_EXECUTION_ERROR,
					TestResult: &pb.GrainInvocationResult_Error{
						Error: err.Error(),
					},
				})
			} else {
				h.resultStream.Send(&pb.GrainInvocationResult{
					RequestId: invocation.InvocationId,
					Status:    pb.ExecutionStatus_EXECUTION_OK,
					TestResult: &pb.GrainInvocationResult_Result{
						Result: result.Result,
					},
				})
			}

		}
	}
}

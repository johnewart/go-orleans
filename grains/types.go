package grains

import (
	"context"
	"fmt"
	"github.com/google/uuid"
)

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

type Invocation struct {
	InvocationId string
	GrainID      string
	GrainType    string
	MethodName   string
	Data         []byte
	Context      context.Context
}

type InvocationResult struct {
	InvocationId string
	Data         []byte
	Status       InvocationStatus
}

type Grain struct {
	ID   string
	Type string
}

func (g Grain) Invocation(method string, data []byte) *Invocation {
	return &Invocation{
		GrainID:      g.ID,
		GrainType:    g.Type,
		MethodName:   method,
		Data:         data,
		InvocationId: NewInvocationID(),
	}
}

func NewInvocationID() string {
	return uuid.New().String()
}

type InvocationStatus int

const (
	InvocationSuccess InvocationStatus = iota
	InvocationFailure
)

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

package grains

import (
	"context"
	"fmt"
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

type Grain struct {
	ID   string
	Type string
	Data []byte
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

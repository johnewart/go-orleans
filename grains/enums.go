package grains

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

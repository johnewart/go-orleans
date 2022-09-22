package reminders

import (
	"time"

	"github.com/johnewart/go-orleans/grains"
	"github.com/johnewart/go-timescheduler/schedule"
)

type Reminder struct {
	schedule.Schedulable
	ReminderName string
	GrainId      string
	GrainType    string
	FireAt       time.Time
	Period       time.Duration
	Method       string
	Data         []byte
}

func (r Reminder) ShouldFire() bool {
	return r.FireAt.Before(time.Now())
}

func (r Reminder) Id() string {
	return r.ReminderName
}

func (r Reminder) DueTime() time.Time {
	return r.FireAt
}

func (r Reminder) Grain() grains.Grain {
	return grains.Grain{
		ID:   r.GrainId,
		Type: r.GrainType,
	}
}

func (r Reminder) Invocation() *grains.Invocation {
	return r.Grain().Invocation(r.Method, r.Data)
}

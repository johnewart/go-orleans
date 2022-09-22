package silo

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/util"
	"time"

	"github.com/google/uuid"
	"github.com/johnewart/go-orleans/grains"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-timescheduler/schedule"
	"zombiezen.com/go/log"
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

type ReminderRegistry struct {
	ctx            context.Context
	reminderTicker *time.Ticker
	schedule       *schedule.Scheduler[Reminder]
	connectionPool *util.ConnectionPool
}

type ReminderCallback func(*grains.Invocation) (*grains.GrainExecution, error)

func NewReminderRegistry(ctx context.Context) *ReminderRegistry {
	return &ReminderRegistry{
		ctx:            ctx,
		schedule:       schedule.NewScheduler[Reminder](ctx, 30*time.Second, 3),
		reminderTicker: time.NewTicker(20 * time.Second),
		connectionPool: &util.ConnectionPool{},
	}
}

func (r *ReminderRegistry) Register(name string, grainType string, grainId string, dueTime time.Time, period time.Duration) error {
	log.Infof(r.ctx, "Registering reminder %s for grain %s/%s", name, grainType, grainId)
	r.schedule.AddReminder(Reminder{
		ReminderName: name,
		GrainId:      grainId,
		GrainType:    grainType,
		FireAt:       dueTime,
		Period:       period,
	})
	return nil
}

func (r *ReminderRegistry) Tick(s *Silo) {

	updatedReminders := make([]Reminder, 0)

	for _, reminder := range r.schedule.Due() {
		log.Infof(r.ctx, "Firing reminder %s", reminder.ReminderName)
		invocation := grains.Invocation{
			GrainID:      reminder.GrainId,
			GrainType:    reminder.GrainType,
			Data:         reminder.Data,
			Context:      r.ctx,
			InvocationId: fmt.Sprintf("reminder-%s-%s", reminder.ReminderName, uuid.New().String()),
			MethodName:   reminder.Method,
		}

		if s.CanHandle(invocation.GrainType) {
			log.Infof(r.ctx, "Firing reminder %s for grain %s/%s", reminder.ReminderName, reminder.GrainType, reminder.GrainId)
			if ch, err := s.Handle(r.ctx, &invocation); err != nil {
				log.Errorf(r.ctx, "Error handling reminder: %s", err)
			} else {
				select {
				case result := <-ch:
					log.Infof(r.ctx, "Reminder result: %s", result)
				case <-time.After(1 * time.Second):
					log.Errorf(r.ctx, "Reminder timed out")
				}
			}
		} else {
			log.Infof(r.ctx, "Grain %v is not compatible with silo %v", invocation.GrainType, s.silo)

			log.Infof(r.ctx, "Locating compatible silo for %v", invocation.GrainType)
			m := s.Locate(invocation.GrainType)
			if m != nil {
				if conn, err := r.connectionPool.GetConnection(m.HostPort()); err != nil {
					log.Errorf(r.ctx, "Error connecting to silo: %s", err)
				} else {
					log.Infof(r.ctx, "Forwarding reminder %s to silo %s", reminder.ReminderName, m.HostPort())
					client := pb.NewSiloServiceClient(conn)
					if _, err := client.InvokeGrain(s.ctx, &pb.GrainInvocationRequest{
						GrainId:    invocation.GrainID,
						GrainType:  invocation.GrainType,
						Data:       invocation.Data,
						MethodName: invocation.MethodName,
						RequestId:  invocation.InvocationId,
					}); err != nil {
						log.Warnf(s.ctx, "Unable to submit job to %s: %v", m.HostPort(), err)
					} else {
						log.Infof(s.ctx, "Executing grain for %s on %s", reminder.ReminderName, m.HostPort())
					}
				}
			} else {
				log.Warnf(s.ctx, "Unable to locate silo for %s", invocation.GrainType)
			}
		}

		reminder.FireAt = reminder.FireAt.Add(reminder.Period)

		updatedReminders = append(updatedReminders, reminder)
	}

	log.Infof(r.ctx, "Updated reminders: %d", len(updatedReminders))
	for _, reminder := range updatedReminders {
		log.Infof(r.ctx, " * %s", reminder.ReminderName)
		r.schedule.AddReminder(reminder)
	}

}

func (r *ReminderRegistry) StartReminderProcess(s *Silo) error {

	for {
		select {
		case <-r.ctx.Done():
			log.Infof(r.ctx, "Context is done, stopping reminder process...")
		case t := <-r.reminderTicker.C:
			log.Infof(r.ctx, "Reminder tick at %v", t)
			r.Tick(s)
		}
	}

}

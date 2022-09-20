package reminders

import (
	"context"
	"github.com/johnewart/go-orleans/silo"
	"time"
	"zombiezen.com/go/log"
)

type Reminder struct {
	ReminderName string
	GrainId      string
	GrainType    string
	DueTime      time.Time
	Period       time.Duration
	Data         []byte
}

func (r *Reminder) ShouldFire() bool {
	return r.DueTime.Before(time.Now())
}

type ReminderRegistry struct {
	ctx         context.Context
	reminderMap map[string]Reminder
	silo        silo.Silo
}

func NewReminderRegistry(node silo.Silo) *ReminderRegistry {
	return &ReminderRegistry{
		reminderMap: make(map[string]Reminder),
		silo:        node,
	}
}

func (r ReminderRegistry) Register(name string, id string, dueTime time.Time, period time.Duration) {
	r.reminderMap[name] = Reminder{
		ReminderName: name,
		GrainId:      id,
		DueTime:      dueTime,
		Period:       period,
	}
}

func (r ReminderRegistry) Tick() {
	updatedReminders := make([]Reminder, 0)

	for _, reminder := range r.reminderMap {
		if reminder.ShouldFire() {
			log.Infof(context.Background(), "Firing reminder %s", reminder.ReminderName)

			reminder.DueTime = reminder.DueTime.Add(reminder.Period)
			updatedReminders = append(updatedReminders, reminder)
		}
	}

	for _, reminder := range updatedReminders {
		r.reminderMap[reminder.ReminderName] = reminder
	}
}

package reminders

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/johnewart/go-orleans/grains"
	"github.com/johnewart/go-orleans/metrics"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-orleans/reminders/data"
	"github.com/johnewart/go-orleans/reminders/storage"
	"github.com/johnewart/go-timescheduler/schedule"
	"time"
	"zombiezen.com/go/log"
)

type ReminderConfig struct {
	ReminderStoreDSN string
	TickInterval     time.Duration
	SiloClient       pb.SiloServiceClient
	MetricsRegistry  *metrics.MetricsRegistry
}

func (r *ReminderConfig) ReminderStore() (storage.ReminderStore, error) {
	return storage.NewPostgresqlReminderStore(r.ReminderStoreDSN)
}

type ReminderRegistry struct {
	ctx             context.Context
	reminderTicker  *time.Ticker
	schedule        *schedule.Scheduler[*data.Reminder]
	siloClient      pb.SiloServiceClient
	metricsRegistry *metrics.MetricsRegistry
	store           storage.ReminderStore
}

type ReminderCallback func(*grains.Invocation) (*grains.GrainExecution, error)

func NewReminderRegistry(ctx context.Context, config ReminderConfig) (*ReminderRegistry, error) {

	if store, err := config.ReminderStore(); err != nil {
		return nil, fmt.Errorf("Unable to create reminder store: %v", err)
	} else {
		return &ReminderRegistry{
			ctx:             ctx,
			schedule:        schedule.NewScheduler[*data.Reminder](ctx, 30*time.Second, 3),
			reminderTicker:  time.NewTicker(config.TickInterval),
			siloClient:      config.SiloClient,
			metricsRegistry: config.MetricsRegistry,
			store:           store,
		}, nil
	}
}

func (r *ReminderRegistry) Register(name string, grainType string, grainId string, payload []byte, dueTime time.Time, period time.Duration) error {
	log.Infof(r.ctx, "Registering reminder %s for grain %s/%s", name, grainType, grainId)
	reminder := &data.Reminder{
		ReminderName: name,
		GrainId:      grainId,
		GrainType:    grainType,
		FireAt:       dueTime,
		Period:       period,
		Data:         payload,
	}

	if err := r.store.StoreReminder(reminder); err != nil {
		return err
	} else {
		r.schedule.AddReminder(reminder)
		return nil
	}
}

func (r *ReminderRegistry) Tick() error {

	updatedReminders := make([]*data.Reminder, 0)

	for _, reminder := range r.schedule.Due() {
		r.metricsRegistry.CountReminderInvocation(reminder.ReminderName)
		invocation := grains.Invocation{
			GrainID:      reminder.GrainId,
			GrainType:    reminder.GrainType,
			Data:         reminder.Data,
			Context:      r.ctx,
			InvocationId: fmt.Sprintf("reminder-%s-%s", reminder.ReminderName, uuid.New().String()),
			MethodName:   reminder.Method,
		}

		log.Infof(r.ctx, "Firing reminder %s for grain %s/%s", reminder.ReminderName, reminder.GrainType, reminder.GrainId)
		if _, err := r.siloClient.InvokeGrain(r.ctx, &pb.GrainInvocationRequest{
			GrainId:    invocation.GrainID,
			GrainType:  invocation.GrainType,
			Data:       invocation.Data,
			MethodName: invocation.MethodName,
			RequestId:  invocation.InvocationId,
		}); err != nil {
			log.Warnf(r.ctx, "Unable to invoke grain %s/%s: %v", invocation.GrainType, invocation.GrainID, err)
		} else {
			log.Infof(r.ctx, "Invoked grain %s/%s", invocation.GrainType, invocation.GrainID)
		}

		if reminder.Period.Nanoseconds() > 0 {
			r.metricsRegistry.CountRemindersRescheduled(reminder.ReminderName)
			reminder.FireAt = reminder.FireAt.Add(reminder.Period)
			updatedReminders = append(updatedReminders, reminder)
		}
	}

	log.Infof(r.ctx, "Updating reminders: %d", len(updatedReminders))
	for _, reminder := range updatedReminders {
		r.schedule.AddReminder(reminder)
	}

	return nil
}

func (r *ReminderRegistry) StartReminderProcess() {
	log.Infof(r.ctx, "Starting reminder process")

	log.Infof(r.ctx, "Loading reminders from store")
	if reminders, err := r.store.GetReminders(); err != nil {
		log.Errorf(r.ctx, "Unable to load reminders: %v", err)
	} else {
		log.Infof(r.ctx, "Loaded %d reminders", len(reminders))
		for _, reminder := range reminders {
			r.schedule.AddReminder(reminder)
		}
	}

	go func() {
		for {
			select {
			case <-r.ctx.Done():
				log.Infof(r.ctx, "Context is done, stopping reminder process...")
			case t := <-r.reminderTicker.C:
				log.Infof(r.ctx, "Reminder tick at %v", t)
				if err := r.metricsRegistry.TimeReminderRegistryTick(r.Tick); err != nil {
					log.Warnf(r.ctx, "Unable to time reminder tick: %v", err)
				}
			}
		}
	}()
}

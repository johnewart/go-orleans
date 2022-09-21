package silo

import (
	"context"
	"fmt"
	"github.com/google/uuid"
	"github.com/johnewart/go-orleans/grains"
	"sync"
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

type TimespanBucket struct {
	startTime time.Time
	endTime   time.Time
	reminders []Reminder
	lock      *sync.Mutex
}

func NewTimespanBucket(startTime time.Time, endTime time.Time) TimespanBucket {
	return TimespanBucket{
		startTime: startTime,
		endTime:   endTime,
		reminders: make([]Reminder, 0),
		lock:      &sync.Mutex{},
	}
}

func (t *TimespanBucket) Contains(in time.Time) bool {
	return t.startTime.Before(in) && t.endTime.After(in)
}

func (t *TimespanBucket) AddReminder(reminder Reminder) {
	t.lock.Lock()
	defer t.lock.Unlock()
	log.Infof(context.Background(), "Adding reminder %s to bucket %s", reminder.ReminderName, t.String())
	t.reminders = append(t.reminders, reminder)
}

func (t *TimespanBucket) Past() bool {
	return t.endTime.Before(time.Now())
}

func (t *TimespanBucket) String() string {
	return fmt.Sprintf("TimespanBucket: %s -> %s", t.startTime, t.endTime)
}

func (t *TimespanBucket) Size() int {
	t.lock.Lock()
	defer t.lock.Unlock()
	return len(t.reminders)
}

func (t *TimespanBucket) IsAfter(dueTime time.Time) bool {
	return t.startTime.After(dueTime)
}

func (t *TimespanBucket) IsBefore(dueTime time.Time) bool {
	return t.endTime.Before(dueTime)
}

type Scheduler struct {
	buckets   []TimespanBucket
	blockSize time.Duration
	numBlocks int
	ctx       context.Context
}

func NewScheduler(ctx context.Context, blockSize time.Duration, numBlocks int) *Scheduler {
	buckets := make([]TimespanBucket, 0)

	for i := 0; i < numBlocks; i++ {
		startTime := time.Now().Add(time.Duration(i) * blockSize)
		endTime := startTime.Add(blockSize)
		buckets = append(buckets, NewTimespanBucket(startTime, endTime))
	}

	return &Scheduler{
		ctx:       ctx,
		buckets:   buckets,
		blockSize: blockSize,
		numBlocks: numBlocks,
	}
}

func (s *Scheduler) Update() {
	if s.buckets[0].Size() > 0 {
		log.Infof(s.ctx, "First bucket contains reminders, not moving yet!")
	} else {
		s.buckets = s.buckets[1:]
		s.buckets = append(s.buckets, TimespanBucket{
			startTime: s.buckets[len(s.buckets)-1].endTime,
			endTime:   s.buckets[len(s.buckets)-1].endTime.Add(s.blockSize),
		})
	}
}

func (s *Scheduler) NeedsUpdate() bool {
	// Is the first bucket in the past?
	// If so, we need to update
	return s.buckets[0].Past()
}

func (s *Scheduler) AddReminder(reminder Reminder) {

	if s.buckets[0].IsAfter(reminder.DueTime) {
		// Overdue? Put it at the head of the queue
		s.buckets[0].AddReminder(reminder)
		return
	}

	if s.buckets[len(s.buckets)-1].IsBefore(reminder.DueTime) {
		// Too far out? Shove it into the last bucket
		s.buckets[len(s.buckets)-1].AddReminder(reminder)
		return
	}

	for _, bucket := range s.buckets {
		if bucket.Contains(reminder.DueTime) {
			log.Infof(s.ctx, "Adding reminder %s to bucket %s", reminder.ReminderName, bucket.String())
			bucket.AddReminder(reminder)
			return
		}
	}

	/*	if s.NeedsUpdate() {
		s.Update()
	}*/
}

func (s *Scheduler) Due() []Reminder {
	dueReminders := make([]Reminder, 0)
	bucket := s.buckets[0]

	removeIdxs := make([]int, 0)
	for i, reminder := range bucket.reminders {
		if reminder.ShouldFire() {
			log.Infof(s.ctx, "Reminder %s is due!", reminder.ReminderName)
			dueReminders = append(dueReminders, reminder)
			removeIdxs = append(removeIdxs, i)
		}
	}

	for _, idx := range removeIdxs {
		// Do the truffle shuffle!
		bucket.reminders = append(bucket.reminders[:idx], bucket.reminders[idx+1:]...)
	}

	return dueReminders
}

func (s *Scheduler) Dump() {
	for _, bucket := range s.buckets {
		log.Infof(s.ctx, "%s", bucket.String())
		for _, reminder := range bucket.reminders {
			log.Infof(s.ctx, " * %s @ %s", reminder.ReminderName, reminder.DueTime)
		}
	}
}

type ReminderRegistry struct {
	ctx            context.Context
	silo           *Silo
	reminderTicker *time.Ticker
	schedule       *Scheduler
}

type ReminderCallback func(*grains.Invocation) (*grains.GrainExecution, error)

func NewReminderRegistry(ctx context.Context) *ReminderRegistry {
	return &ReminderRegistry{
		ctx:            ctx,
		schedule:       NewScheduler(ctx, 30*time.Second, 3),
		reminderTicker: time.NewTicker(5 * time.Second),
	}
}

func (r ReminderRegistry) Register(name string, grainType string, grainId string, dueTime time.Time, period time.Duration) error {
	log.Infof(r.ctx, "Registering reminder %s for grain %s/%s", name, grainType, grainId)
	r.schedule.AddReminder(Reminder{
		ReminderName: name,
		GrainId:      grainId,
		GrainType:    grainType,
		DueTime:      dueTime,
		Period:       period,
	})
	r.schedule.Dump()
	return nil
}

func (r ReminderRegistry) Tick(s *Silo) {
	r.schedule.Dump()

	updatedReminders := make([]Reminder, 0)

	for _, reminder := range r.schedule.Due() {
		log.Infof(r.ctx, "Firing reminder %s", reminder.ReminderName)
		invocation := grains.Invocation{
			GrainID:      reminder.GrainId,
			GrainType:    reminder.GrainType,
			Data:         reminder.Data,
			Context:      r.ctx,
			InvocationId: fmt.Sprintf("reminder-%s-%s", reminder.ReminderName, uuid.New().String()),
			MethodName:   "Invoke",
		}

		if ch, err := s.Handle(r.ctx, &invocation); err != nil {
			log.Errorf(r.ctx, "Error handling reminder: %s", err)
		} else {
			result := <-ch
			log.Infof(r.ctx, "Reminder result: %s", result)
		}

		reminder.DueTime = reminder.DueTime.Add(reminder.Period)

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
			return nil
		case t := <-r.reminderTicker.C:
			log.Infof(r.ctx, "Reminder tick at %v", t)
			r.Tick(s)
		}
	}

}

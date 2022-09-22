package storage

import (
	"github.com/johnewart/go-orleans/reminders/data"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

type PgReminder struct {
	Name      string `gorm:"primaryKey"`
	GrainId   string `gorm:"primaryKey"`
	GrainType string `gorm:"primaryKey"`
	Data      []byte
	Period    int64
	FireAt    int64
	Method    string
}

func (p PgReminder) TableName() string {
	return "reminders"
}

type PostgresqlReminderStore struct {
	ReminderStore
	db *gorm.DB
}

func NewPostgresqlReminderStore(dsn string) (*PostgresqlReminderStore, error) {
	if db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{}); err != nil {
		return nil, err
	} else {
		return &PostgresqlReminderStore{db: db}, nil
	}
}

func (s *PostgresqlReminderStore) GetReminders() ([]*data.Reminder, error) {
	reminders := make([]PgReminder, 0)
	if result := s.db.Find(&reminders); result.Error != nil {
		return nil, result.Error
	} else {
		records := make([]*data.Reminder, 0)
		for _, reminder := range reminders {
			p := time.Duration(time.Duration(reminder.Period) * time.Millisecond)
			f := time.Unix(0, reminder.FireAt)

			records = append(records, &data.Reminder{
				ReminderName: reminder.Name,
				GrainId:      reminder.GrainId,
				GrainType:    reminder.GrainType,
				Data:         reminder.Data,
				FireAt:       f,
				Period:       p,
				Method:       reminder.Method,
			})
		}

		return records, nil
	}
}

func (s *PostgresqlReminderStore) StoreReminder(reminder *data.Reminder) error {
	pgReminder := PgReminder{
		Name:      reminder.ReminderName,
		GrainId:   reminder.GrainId,
		GrainType: reminder.GrainType,
		Data:      reminder.Data,
		Period:    int64(reminder.Period.Milliseconds()),
		FireAt:    reminder.FireAt.UnixNano(),
		Method:    reminder.Method,
	}

	if result := s.db.Clauses(clause.OnConflict{
		UpdateAll: true,
	}).Create(&pgReminder); result.Error != nil {
		return result.Error
	} else {
		return nil
	}

}

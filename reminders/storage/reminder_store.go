package storage

import "github.com/johnewart/go-orleans/reminders/data"

type ReminderStore interface {
	GetReminders() ([]*data.Reminder, error)
	StoreReminder(reminder *data.Reminder) error
}

package locator

type ReminderLocator interface {
	LocateReminder(reminderName string) (string, error)
	StoreReminderLocation(reminderName string, hostPort string) error
}

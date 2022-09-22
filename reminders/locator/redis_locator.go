package locator

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
)

type RedisLocator struct {
	ReminderLocator
	redisClient *redis.Client
	ctx         context.Context
}

func NewRedisLocator(redisHostPort string) *RedisLocator {
	redisClient := redis.NewClient(&redis.Options{
		Addr: redisHostPort,
	})

	return &RedisLocator{
		redisClient: redisClient,
		ctx:         context.Background(),
	}
}

func (r *RedisLocator) locationKeyForReminder(reminderName string) string {
	return fmt.Sprintf("loc://reminder:%s", reminderName)
}

func (r *RedisLocator) Healthy() bool {
	if _, err := r.redisClient.Ping(r.ctx).Result(); err != nil {
		return false
	} else {
		return true
	}
}

func (r *RedisLocator) StoreReminderLocation(reminderName string, hostPort string) error {
	if _, err := r.redisClient.Set(r.ctx, r.locationKeyForReminder(reminderName), hostPort, 0).Result(); err != nil {
		return fmt.Errorf("unable to set grains silo: %v", err)
	} else {
		return nil
	}
}

func (r *RedisLocator) LocateReminder(reminderName string) (string, error) {
	if res, err := r.redisClient.Get(r.ctx, r.locationKeyForReminder(reminderName)).Result(); err != nil {
		if err == redis.Nil {
			return "", nil
		} else {
			return "", fmt.Errorf("unable to get grains silo: %v", err)
		}
	} else {
		return res, nil
	}
}

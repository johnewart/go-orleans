package locator

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/johnewart/go-orleans/cluster"
)

type RedisLocator struct {
	GrainLocator
	redisClient *redis.Client
	ctx         context.Context
}

type RedisSilo struct {
	IP   string `json:"ip"`
	Port int    `json:"port"`
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

func (r *RedisLocator) locationKeyForGrain(grainType string, grainId string) string {
	return "loc://" + grainType + ":" + grainId
}

func (r *RedisLocator) Healthy() bool {
	if _, err := r.redisClient.Ping(r.ctx).Result(); err != nil {
		return false
	} else {
		return true
	}
}

func (r *RedisLocator) PutSilo(grainType string, grainId string, silo cluster.Member) error {
	redisSilo := RedisSilo{
		IP:   silo.IP,
		Port: silo.Port,
	}
	redisSiloJson, err := json.Marshal(redisSilo)
	if err != nil {
		return fmt.Errorf("unable to marshal silo: %v", err)
	}

	if _, err := r.redisClient.Set(r.ctx, r.locationKeyForGrain(grainType, grainId), redisSiloJson, 0).Result(); err != nil {
		return fmt.Errorf("unable to set grains silo: %v", err)
	} else {
		return nil
	}
}

func (r *RedisLocator) GetSilo(grainType string, grainId string) (*cluster.Member, error) {
	if res, err := r.redisClient.Get(r.ctx, r.locationKeyForGrain(grainType, grainId)).Result(); err != nil {
		if err == redis.Nil {
			return nil, nil
		} else {
			return nil, fmt.Errorf("unable to get grains silo: %v", err)
		}
	} else {
		var redisSilo RedisSilo
		decodeErr := json.Unmarshal([]byte(res), &redisSilo)
		if decodeErr != nil {
			return nil, fmt.Errorf("unable to decode silo: %v", decodeErr)
		}

		return &cluster.Member{
			IP:   redisSilo.IP,
			Port: redisSilo.Port,
		}, nil
	}
}

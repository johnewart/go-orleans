package store

import (
	"context"
	"fmt"
	"github.com/go-redis/redis/v9"
	"github.com/johnewart/go-orleans/silo/state"
	"github.com/johnewart/go-orleans/silo/state/types"
)

type RedisGrainStateStore struct {
	state.GrainStateStore
	client *redis.Client
	ctx    context.Context
}

func NewRedisGrainStateStore(redisHostPort string) *RedisGrainStateStore {
	client := redis.NewClient(&redis.Options{
		Addr: redisHostPort,
	})

	return &RedisGrainStateStore{
		client: client,
		ctx:    context.Background(),
	}
}

func (r *RedisGrainStateStore) Get(grainType string, grainId string) (*types.GrainState, error) {
	if stateData, err := r.client.Get(r.ctx, grainType+":"+grainId).Result(); err != nil {
		return nil, fmt.Errorf("unable to get grain state: %v", err)
	} else {
		return &types.GrainState{
			GrainType: grainType,
			GrainId:   grainId,
			StateData: []byte(stateData),
		}, nil
	}
}

func (r *RedisGrainStateStore) Put(grainType string, grainId string, stateData []byte) error {
	return r.client.Set(r.ctx, grainType+":"+grainId, stateData, 0).Err()
}

func (r *RedisGrainStateStore) Delete(grainType string, grainId string) error {
	return r.client.Del(r.ctx, grainType+":"+grainId).Err()
}

func (r *RedisGrainStateStore) Healthy() bool {
	if _, err := r.client.Ping(r.ctx).Result(); err != nil {
		return false
	} else {
		return true
	}
}

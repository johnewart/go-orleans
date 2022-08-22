package state

import "github.com/johnewart/go-orleans/silo/state/types"

type GrainStateStore interface {
	Get(grainType string, grainId string) (*types.GrainState, error)
	Put(grainType string, grainId string, stateData []byte) error
	Delete(grainType string, grainId string) error
}

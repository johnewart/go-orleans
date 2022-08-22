package locator

import "github.com/johnewart/go-orleans/cluster"

type GrainLocator interface {
	GetSilo(grainType string, grainId string) (*cluster.Member, error)
	PutSilo(grainType string, grainId string, silo cluster.Member) error
}

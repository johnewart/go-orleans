package cluster

import (
	"context"
	"zombiezen.com/go/log"
)

type Suspicion struct {
	Suspect   Member
	Accuser   Member
	Timestamp int64
}

type Member struct {
	IP     string
	Port   int
	Epoch  int64
	Grains []string
}

func (s *Member) CanHandle(grainType string) bool {
	ctx := context.TODO()
	for _, g := range s.Grains {
		log.Infof(ctx, "Ability '%s' == requested ('%s')? %v", g, grainType, g == grainType)
		if g == grainType {
			return true
		}
	}

	return false
}

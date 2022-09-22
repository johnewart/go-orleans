package table

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/cluster"
	"github.com/johnewart/go-orleans/cluster/storage"
	"github.com/johnewart/go-orleans/grains"
	"math"
	"time"
	"zombiezen.com/go/log"
)

type Config struct {
	SuspicionWindow time.Duration
	SuspicionQuorum int
	PlacementMethod PlacementMethod
}

type PlacementMethod int

const (
	RandomPlacement PlacementMethod = iota
	LeastLoad
	RoundRobin
)

type MembershipTable struct {
	Members []*cluster.Member
	store   storage.MemberStore
	ctx     context.Context
	config  Config
}

func NewTable(ctx context.Context, store storage.MemberStore, config Config) *MembershipTable {
	return &MembershipTable{
		Members: make([]*cluster.Member, 0),
		store:   store,
		ctx:     ctx,
		config:  config,
	}
}

func (t *MembershipTable) Size() int {
	return len(t.Members)
}

func (t *MembershipTable) Suspect(accuser, suspect *cluster.Member) error {
	if err := t.store.DeclareSuspect(accuser, suspect); err != nil {
		return fmt.Errorf("unable to suspect member: %v", err)
	}

	if suspicions, err := t.store.GetSuspicions(suspect); err != nil {
		return fmt.Errorf("error getting suspicions: %v", err)
	} else {
		if len(suspicions) > 0 {
			earliestTimestamp := int64(math.MaxInt64)
			latestTimestamp := int64(0)

			for _, s := range suspicions {
				log.Infof(t.ctx, "suspicion: %v", s)
				if s.Timestamp < earliestTimestamp {
					earliestTimestamp = s.Timestamp
				}
				if s.Timestamp > latestTimestamp {
					latestTimestamp = s.Timestamp
				}
			}

			if (earliestTimestamp != int64(math.MaxInt64)) && (latestTimestamp != int64(0)) {
				now := time.Now().UnixMicro()
				timeSinceEarliest := time.Duration(time.Duration(now-earliestTimestamp) * time.Microsecond)

				if timeSinceEarliest.Seconds() > t.config.SuspicionWindow.Seconds() {
					log.Infof(t.ctx, "suspicion window expired (%0.2f > %0.2f), removing %v from suspects", timeSinceEarliest.Seconds(), t.config.SuspicionWindow.Seconds(), suspect)
					return t.store.RemoveSuspicions(suspect)
				} else {
					log.Infof(t.ctx, "suspicion window not expired")
					if len(suspicions) > t.config.SuspicionQuorum {
						log.Infof(t.ctx, "suspicion quorum met for %v", suspect)
						if latestSuspicion, lErr := t.store.LatestSuspicion(suspect); lErr != nil {
							return fmt.Errorf("error getting latest suspicion: %v", lErr)
						} else {
							if latestSuspicion != nil {
								log.Infof(t.ctx, "latest suspicion: %v", latestSuspicion)
								if latestSuspicion.Timestamp == latestTimestamp {
									log.Infof(t.ctx, "latest suspicion is latest, declaring %v dead", suspect)
									return t.store.DeclareDead(suspect)
								}
							}
						}

					} else {
						log.Infof(t.ctx, "suspicion quorum not met")
						return nil
					}
				}
			}
		}
	}

	return nil
}

func (t *MembershipTable) Lock() {
}

func (t *MembershipTable) Unlock() {

}

func (t *MembershipTable) GetSiloForGrain(g grains.Grain) *cluster.Member {
	switch t.config.PlacementMethod {
	case RandomPlacement:
	case RoundRobin:
	case LeastLoad:
		// All the same for now
		for _, m := range t.Members {
			for _, sg := range m.Grains {
				if sg == g.Type {
					return m
				}
			}
		}
	}

	return nil
}

func (t *MembershipTable) WithMembers(f func(*cluster.Member) error) error {
	t.Lock()
	defer t.Unlock()

	for _, m := range t.Members {
		if err := f(m); err != nil {
			return err
		}
	}

	return nil
}

func (t *MembershipTable) Update() error {
	t.Lock()
	defer t.Unlock()

	if members, err := t.store.GetMembers(); err != nil {
		return err
	} else {
		t.Members = members
		return nil
	}
}

func (t *MembershipTable) Announce(m *cluster.Member) error {
	return t.store.Announce(m)
}

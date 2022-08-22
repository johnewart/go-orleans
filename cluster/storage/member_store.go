package storage

import (
	"github.com/johnewart/go-orleans/cluster"
)

type MemberStore interface {
	GetMembers() ([]cluster.Member, error)
	Announce(member *cluster.Member) error
	GetSuspicions(member *cluster.Member) ([]cluster.Suspicion, error)
	RemoveSuspicions(member *cluster.Member) error
	LatestSuspicion(member *cluster.Member) (*cluster.Suspicion, error)
	DeclareDead(member *cluster.Member) error
	DeclareSuspect(accuser, suspect *cluster.Member) error
}

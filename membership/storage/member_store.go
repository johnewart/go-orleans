package storage

import (
	"github.com/johnewart/go-orleans/membership"
)

type MemberStore interface {
	GetLatestTable() (*membership.MembershipTable, error)
	Announce(member *membership.Member) error
	Suspect(member *membership.Member) error
}

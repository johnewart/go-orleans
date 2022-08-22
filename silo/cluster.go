package silo

import "time"

type ClusterConfig struct {
	SuspicionWindow    time.Duration
	SuspicionThreshold int
	UpdateTimeout      time.Duration
}

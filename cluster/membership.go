package cluster

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"zombiezen.com/go/log"
)

type Suspicion struct {
	Suspect   Member
	Accuser   Member
	Timestamp int64
}

type Member struct {
	IP         string
	Port       int
	Epoch      int64
	Grains     []string
	connection *grpc.ClientConn
}

func NewClusterMember(ip string, port int, epoch int64, grains []string) *Member {
	return &Member{
		IP:     ip,
		Port:   port,
		Epoch:  epoch,
		Grains: grains,
	}
}

func (s *Member) HostPort() string {
	return fmt.Sprintf("%s:%d", s.IP, s.Port)
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

package cluster

import (
	"context"
	"fmt"
	pb "github.com/johnewart/go-orleans/proto/silo"
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

func (s *Member) HostPort() string {
	return fmt.Sprintf("%s:%d", s.IP, s.Port)
}

func (s *Member) SiloClient() (pb.SiloServiceClient, error) {
	conn, err := s.Connection()
	if err != nil {
		return nil, err
	}

	client := pb.NewSiloServiceClient(conn)
	return client, nil
}

func (s *Member) Connection() (*grpc.ClientConn, error) {
	if s.connection == nil {
		hostPort := s.HostPort()
		conn, err := grpc.Dial(hostPort, grpc.WithInsecure())
		if err != nil {
			return nil, fmt.Errorf("error connecting to member %s: %v", hostPort, err)
		}
		s.connection = conn
	}

	return s.connection, nil
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

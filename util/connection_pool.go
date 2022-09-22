package util

import (
	"google.golang.org/grpc"
	"sync"
)

type ConnectionPool struct {
	sync.Map
}

func (p *ConnectionPool) GetConnection(address string) (*grpc.ClientConn, error) {

	// TODO: Health check
	if conn, ok := p.getConnection(address); ok {
		return conn, nil
	}

	conn, err := grpc.Dial(address, grpc.WithInsecure())
	if err != nil {
		return nil, err
	}

	p.storeConnection(address, conn)
	return conn, nil
}

func (p *ConnectionPool) getConnection(address string) (*grpc.ClientConn, bool) {
	if item, ok := p.Load(address); !ok {
		return nil, false
	} else {
		return item.(*grpc.ClientConn), true
	}
}

func (p *ConnectionPool) storeConnection(address string, c *grpc.ClientConn) error {
	p.Store(address, c)
	return nil
}

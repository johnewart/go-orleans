package silo

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/cluster"
	"github.com/johnewart/go-orleans/cluster/storage"
	"github.com/johnewart/go-orleans/cluster/table"
	"github.com/johnewart/go-orleans/grains"
	"github.com/johnewart/go-orleans/metrics"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-orleans/silo/locator"
	"github.com/johnewart/go-orleans/silo/state/store"
	"github.com/johnewart/go-orleans/util"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"strings"
	"sync"
	"time"
	"zombiezen.com/go/log"
)

type GrainResponseMap struct {
	sync.Map
}

func (g *GrainResponseMap) LoadChannel(key string) (chan *grains.InvocationResult, bool) {
	if item, ok := g.Load(key); !ok {
		log.Infof(context.Background(), "Unable to find channel for %s", key)
		return nil, false
	} else {
		log.Infof(context.Background(), "Found channel for request %s", key)
		return item.(chan *grains.InvocationResult), true
	}
}

func (g *GrainResponseMap) StoreChannel(requestId string, c chan *grains.InvocationResult) error {
	log.Infof(context.Background(), "Storing channel for request id %s", requestId)
	g.Store(requestId, c)
	return nil
}

type SiloConfig struct {
	RedisHostPort    string
	TableStoreDSN    string
	HearbeatInterval time.Duration
	RoutableIP       string
	ServicePort      int
	ReminderInterval time.Duration
	Metrics          *metrics.MetricsRegistry
}

type Silo struct {
	membershipTable   *table.MembershipTable
	routableIP        string
	servicePort       int
	startEpoch        int64
	ctx               context.Context
	heartbeatInterval time.Duration
	metrics           *metrics.MetricsRegistry
	silo              cluster.Member
	grainHandler      *GrainRegistry
	grainLocator      locator.GrainLocator
	grainResponseMap  GrainResponseMap
	reminderTicker    *time.Ticker
	connectionPool    *util.ConnectionPool
}

func NewSilo(ctx context.Context, config SiloConfig) (*Silo, error) {

	tableConfig := table.Config{
		SuspicionWindow: 60 * time.Second,
		SuspicionQuorum: 2,
	}

	locationStore := locator.NewRedisLocator(config.RedisHostPort)
	if !locationStore.Healthy() {
		return nil, fmt.Errorf("unable to connect to redis at %v", config.RedisHostPort)
	}

	stateStore := store.NewRedisGrainStateStore(config.RedisHostPort)
	if !stateStore.Healthy() {
		return nil, fmt.Errorf("unable to connect to redis at %v", config.RedisHostPort)
	}

	tableStore, err := storage.NewPostgresqlMemberStore(config.TableStoreDSN)
	if err != nil {
		return nil, err
	}

	startEpoch := time.Now().UnixMicro()
	membershipTable := table.NewTable(ctx, tableStore, tableConfig)
	member := cluster.Member{
		IP:    config.RoutableIP,
		Port:  config.ServicePort,
		Epoch: startEpoch,
	}

	return &Silo{
		ctx:               ctx,
		membershipTable:   membershipTable,
		routableIP:        config.RoutableIP,
		servicePort:       config.ServicePort,
		startEpoch:        startEpoch,
		heartbeatInterval: config.HearbeatInterval,
		metrics:           config.Metrics,
		silo:              member,
		grainHandler:      NewSiloGrainRegistry(),
		grainLocator:      locationStore,
		grainResponseMap:  GrainResponseMap{},
		connectionPool:    &util.ConnectionPool{},
	}, nil
}

func (s *Silo) Ping() int64 {
	return s.startEpoch
}

func (s *Silo) Handle(ctx context.Context, invocation *grains.Invocation) (chan *grains.InvocationResult, error) {
	return s.grainHandler.Handle(ctx, invocation)
}

func (s *Silo) CanHandle(grainType string) bool {
	return s.silo.CanHandle(grainType)
}

func (s *Silo) Locate(grainType string) *cluster.Member {
	for _, member := range s.membershipTable.Members {
		if member.CanHandle(grainType) {
			return member
		}
	}

	return nil
}

func (s *Silo) GetSilo(grainType, grainId string) (*cluster.Member, error) {
	return s.grainLocator.GetSilo(grainType, grainId)
}

func (s *Silo) RegisterGrain(grainType, grainId string) error {
	return s.grainLocator.PutSilo(grainType, grainId, s.silo)
}

func (s *Silo) IsLocal(member *cluster.Member) bool {
	return member.IP == s.routableIP && member.Port == s.servicePort
}
func (s *Silo) GetSiloForGrain(g grains.Grain) *cluster.Member {
	return s.membershipTable.GetSiloForGrain(g)
}

func (s *Silo) StartMonitorProcess() error {
	for {
		s.metrics.UpdateTableSyncCount()
		suspects := make([]cluster.Member, 0)

		err := s.metrics.TimeTableSync(func() error {
			if err := s.membershipTable.Update(); err != nil {
				return fmt.Errorf("unable to update cluster table: %v", err)
			}

			log.Infof(s.ctx, "Pinging %d other members in table", s.membershipTable.Size())
			err := s.membershipTable.WithMembers(func(m *cluster.Member) error {
				if conn, err := s.Connection(m.HostPort()); err != nil {
					log.Warnf(s.ctx, "Unable to dial %s:%d: %v", m.IP, m.Port, err)
					log.Infof(s.ctx, "Suspect that %v is dead", m)
					suspects = append(suspects, *m)
				} else {
					if resp, err := pb.NewSiloServiceClient(conn).Ping(s.ctx, &emptypb.Empty{}); err != nil {
						log.Warnf(s.ctx, "Unable to ping %s:%d: %v", m.IP, m.Port, err)
						log.Infof(s.ctx, "Suspect that %v is dead", m)
						suspects = append(suspects, *m)
					} else {
						log.Infof(s.ctx, "Pinged %s:%d, epoch: %d", m.IP, m.Port, resp.Epoch)
					}
				}

				return nil
			})

			if err != nil {
				log.Warnf(s.ctx, "Unable to ping all members: %v", err)
			}

			return nil
		})

		if err != nil {
			log.Warnf(s.ctx, "Error during table sync: %v", err)
		}

		for _, suspect := range suspects {
			log.Infof(s.ctx, "Suspect that %v is dead", suspect)
			if err := s.membershipTable.Suspect(&s.silo, &suspect); err != nil {
				log.Warnf(s.ctx, "Unable to suspect %v: %v", suspect, err)
			}
		}

		time.Sleep(s.heartbeatInterval)
	}
}

func (s *Silo) StartMembershipUpdateProcess() error {
	for {

		if err := s.membershipTable.Announce(&s.silo); err != nil {
			log.Warnf(s.ctx, "Unable to announce ourselves: %v", err)
		}

		log.Infof(s.ctx, "Announced ourselves as %v -- will hearbeat again in %0.2f seconds", s.silo, s.heartbeatInterval.Seconds())
		time.Sleep(s.heartbeatInterval)
	}
}

func (s *Silo) Start() error {
	wg := &sync.WaitGroup{}
	wg.Add(1)

	go func() {
		log.Infof(s.ctx, "Starting announcement process")
		if err := s.StartMembershipUpdateProcess(); err != nil {
			log.Warnf(s.ctx, "Unable to start announcement process: %v", err)
		}
	}()

	go func() {
		log.Infof(s.ctx, "Starting monitor process")
		if err := s.StartMonitorProcess(); err != nil {
			log.Warnf(s.ctx, "Unable to start monitor process: %v", err)
		}
	}()

	go func() {
		log.Infof(s.ctx, "Starting metrics service")
		if err := s.metrics.Serve(); err != nil {
			log.Warnf(s.ctx, "Unable to start metrics service: %v", err)
		}
	}()

	wg.Wait()
	return nil
}

func (s *Silo) RegisterHandler(grainType string, handle GrainHandle) {
	s.grainHandler.Register(grainType, handle)
	s.Announce(grainType)
}

func (s *Silo) bounceInvocation(ctx context.Context, target *cluster.Member, invocation *grains.Invocation) (*grains.InvocationResult, error) {
	if conn, err := s.Connection(target.HostPort()); err != nil {
		return nil, fmt.Errorf("unable to connect to %v: %v", target.HostPort(), err)
	} else {
		client := pb.NewSiloServiceClient(conn)
		req := &pb.GrainInvocationRequest{
			GrainId:    invocation.GrainID,
			GrainType:  invocation.GrainType,
			MethodName: invocation.MethodName,
			Data:       invocation.Data,
			RequestId:  invocation.InvocationId,
		}
		if res, bounceErr := client.InvokeGrain(ctx, req); bounceErr != nil {
			return nil, fmt.Errorf("unable to bounce grains: %v", bounceErr)
		} else {
			return &grains.InvocationResult{
				InvocationId: invocation.InvocationId,
				Data:         res.Result,
				Status:       grains.InvocationSuccess,
			}, nil

		}
	}
}

func (s *Silo) HandleInvocation(ctx context.Context, invocation *grains.Invocation) (*grains.InvocationResult, error) {

	existing, err := s.GetSilo(invocation.GrainType, invocation.GrainID)
	if err != nil {
		return nil, fmt.Errorf("unable to locate grain: %v", err)
	}

	if existing != nil {
		if s.IsLocal(existing) {
			log.Infof(s.ctx, "Grain %v located on this silo", invocation.GrainID)
			return s.InvokeGrain(invocation)
		} else {
			log.Infof(ctx, "Grain %s/%s exists at %s; redirecting request there", invocation.GrainType, invocation.GrainID, existing.HostPort())
			return s.bounceInvocation(ctx, existing, invocation)
		}
	} else {
		log.Infof(ctx, "Grain %s does not exist", invocation.GrainInfo())
		log.Infof(ctx, "Can we handle this grain? (%v)", invocation.GrainType)
		if s.CanHandle(invocation.GrainType) {
			log.Infof(ctx, "Yes, we can handle this grain")

			if locErr := s.RegisterGrain(invocation.GrainType, invocation.GrainID); locErr != nil {
				return nil, fmt.Errorf("unable to record grain existence in this silo: %v", locErr)
			} else {
				// Now that we're registered, we can handle the invocation
				return s.HandleInvocation(ctx, invocation)
			}
		}
	}

	// We couldn't handle it, and neither could anyone else
	return nil, IncompatibleGrainError{}
}

func (s *Silo) HandleInvocationResult(result *grains.InvocationResult) error {
	log.Infof(s.ctx, "Received invocation result: %v", result)

	if ch, ok := s.grainResponseMap.LoadChannel(result.InvocationId); ok {
		log.Infof(s.ctx, "Found channel for grain response!")
		ch <- result
		return nil
	} else {
		return fmt.Errorf("unable to find channel for invocation %s", result.InvocationId)
	}
}

func (s *Silo) InvokeGrain(invocation *grains.Invocation) (*grains.InvocationResult, error) {
	log.Infof(s.ctx, "Executing grain %s/%s", invocation.GrainType, invocation.GrainID)

	if resultChan, err := s.Handle(s.ctx, invocation); err != nil {
		return nil, err
	} else {
		if resultChan != nil {
			// Store channel for later remote callback
			if err := s.grainResponseMap.StoreChannel(invocation.InvocationId, resultChan); err != nil {
				return nil, fmt.Errorf("unable to store grain response channel: %v", err)
			} else {
				log.Infof(s.ctx, "Waiting for result of grains execution...")
				select {
				case r := <-resultChan:
					log.Infof(s.ctx, "Got result of invocation %s: %v", invocation.InvocationId, r)
					return r, nil
				// TODO: timeout / cancel
				case <-time.After(2 * time.Second):
					return nil, fmt.Errorf("timeout waiting for result of invocation %s", invocation.InvocationId)
				}
			}
		} else {
			return nil, fmt.Errorf("unable to execute grain - no result channel available")
		}
	}
}

func (s *Silo) Announce(grainType string) error {
	for _, g := range s.silo.Grains {
		if g == grainType {
			return fmt.Errorf("grains type %s already registered", grainType)
		}
	}

	s.silo.Grains = append(s.silo.Grains, grainType)
	log.Infof(s.ctx, "Registered grains type %s", grainType)
	log.Infof(s.ctx, "Silo now has %d grains types: [%s]", len(s.silo.Grains), strings.Join(s.silo.Grains, ", "))
	log.Infof(s.ctx, "Re-announcing ourselves!")

	if err := s.membershipTable.Announce(&s.silo); err != nil {
		log.Warnf(s.ctx, "Unable to announce ourselves: %v", err)
		return fmt.Errorf("unable to announce new supported grains types: %v", err)
	} else {
		return nil
	}

}

func (s *Silo) Connection(hostPort string) (*grpc.ClientConn, error) {
	return s.connectionPool.GetConnection(hostPort)
}

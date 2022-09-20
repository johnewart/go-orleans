package silo

import (
	"context"
	"fmt"
	"github.com/johnewart/go-orleans/client"
	"github.com/johnewart/go-orleans/cluster"
	"github.com/johnewart/go-orleans/cluster/storage"
	"github.com/johnewart/go-orleans/cluster/table"
	"github.com/johnewart/go-orleans/grain"
	pb "github.com/johnewart/go-orleans/proto/silo"
	"github.com/johnewart/go-orleans/reminders"
	"github.com/johnewart/go-orleans/silo/locator"
	"github.com/johnewart/go-orleans/silo/state/store"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/emptypb"
	"strconv"
	"strings"
	"sync"
	"time"
	"zombiezen.com/go/log"
)

type SiloConfig struct {
	RedisHostPort    string
	TableStoreDSN    string
	HearbeatInterval time.Duration
	RoutableIP       string
	MetricsPort      int
	ServicePort      int
	ReminderInterval time.Duration
}

type Silo struct {
	membershipTable   *table.MembershipTable
	routableIP        string
	servicePort       int
	startEpoch        int64
	ctx               context.Context
	heartbeatInterval time.Duration
	metrics           *MetricsRegistry
	silo              cluster.Member
	grainHandler      *GrainRegistry
	grainLocator      locator.GrainLocator
	grainResponseMap  map[string]chan *client.GrainExecution
	reminderRegistry  *reminders.ReminderRegistry
	reminderTicker    *time.Ticker
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
		reminderTicker:    time.NewTicker(config.ReminderInterval),
		metrics:           NewMetricRegistry(config.MetricsPort),
		silo:              member,
		grainHandler:      NewSiloGrainRegistry(),
		grainLocator:      locationStore,
		reminderRegistry:  reminders.NewReminderRegistry(),
		grainResponseMap:  map[string]chan *client.GrainExecution{},
	}, nil
}

func (s *Silo) Ping() int64 {
	return s.startEpoch
}

func (s *Silo) Handle(ctx context.Context, invocation *client.Invocation) (chan *client.GrainExecution, error) {
	return s.grainHandler.Handle(ctx, invocation)
}
func (s *Silo) CanHandle(grainType string) bool {
	return s.silo.CanHandle(grainType)
}

func (s *Silo) Locate(grainType string) *cluster.Member {
	for _, member := range s.membershipTable.Members {
		if member.CanHandle(grainType) {
			return &member
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

func (s *Silo) RegisterReminder(ctx context.Context, req *pb.RegisterReminderRequest) (*pb.RegisterReminderResponse, error) {
	dueTime := time.UnixMilli(int64(req.DueTime))
	period := time.Duration(req.Period) * time.Second

	s.reminderRegistry.Register(req.ReminderName, req.GrainId, dueTime, period)
	return &pb.RegisterReminderResponse{
		ReminderId: req.ReminderName,
	}, nil
}

func (s *Silo) PlaceGrain(ctx context.Context, req *pb.PlaceGrainRequest) (*pb.PlaceGrainResponse, error) {
	g := grain.Grain{
		ID:   req.GrainId,
		Type: req.GrainType,
		Data: []byte{},
	}

	target := s.membershipTable.GetSiloForGrain(g)

	if target == nil {
		return &pb.PlaceGrainResponse{
			Status: pb.PlacementStatus_PLACEMENT_NO_COMPATIBLE_SILO,
		}, nil
	} else {
		// Place grain
		return &pb.PlaceGrainResponse{
			Status: pb.PlacementStatus_PLACEMENT_OK,
		}, nil
	}

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
				if conn, err := grpc.Dial(m.IP+":"+strconv.Itoa(int(m.Port)), grpc.WithInsecure()); err != nil {
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

func (s *Silo) StartReminderProcess(ctx context.Context) error {

	for {
		select {
		case <-ctx.Done():
			return nil
		case t := <-s.reminderTicker.C:
			log.Infof(s.ctx, "Reminder tick at %v", t)
			s.reminderRegistry.Tick()
		}
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

	go func() {
		log.Infof(s.ctx, "Starting reminder process")
		if err := s.StartReminderProcess(s.ctx); err != nil {
			log.Warnf(s.ctx, "Unable to start reminder process: %v", err)
		}
	}()

	wg.Wait()
	return nil
}

func (s *Silo) RegisterHandler(grainType string, handle GrainHandle) {
	s.grainHandler.Register(grainType, handle)
	s.Announce(grainType)
}

func (s *Silo) Announce(grainType string) error {
	for _, g := range s.silo.Grains {
		if g == grainType {
			return fmt.Errorf("grain type %s already registered", grainType)
		}
	}

	s.silo.Grains = append(s.silo.Grains, grainType)
	log.Infof(s.ctx, "Registered grain type %s", grainType)
	log.Infof(s.ctx, "Silo now has %d grain types: [%s]", len(s.silo.Grains), strings.Join(s.silo.Grains, ", "))
	log.Infof(s.ctx, "Re-announcing ourselves!")

	if err := s.membershipTable.Announce(&s.silo); err != nil {
		log.Warnf(s.ctx, "Unable to announce ourselves: %v", err)
		return fmt.Errorf("unable to announce new supported grain types: %v", err)
	} else {
		return nil
	}

}

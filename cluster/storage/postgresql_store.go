package storage

import (
	"fmt"
	"github.com/johnewart/go-orleans/cluster"
	"github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"time"
)

type PgMember struct {
	IP     string `gorm:"primaryKey"`
	Port   int    `gorm:"primaryKey"`
	Epoch  int64
	Grains pq.StringArray `gorm:"type:string[]"`
}

func (p PgMember) TableName() string {
	return "members"
}

type PgSuspicion struct {
	SuspectIP   string `gorm:"primaryKey"`
	SuspectPort int    `gorm:"primaryKey"`
	AccuserIP   string `gorm:"primaryKey"`
	AccuserPort int    `gorm:"primaryKey"`
	Timestamp   int64
}

func (p PgSuspicion) TableName() string {
	return "suspicions"
}

type PostgresqlStore struct {
	MemberStore
	db *gorm.DB
}

func NewPostgresqlMemberStore(dsn string) (*PostgresqlStore, error) {
	if db, err := gorm.Open(postgres.Open(dsn), &gorm.Config{}); err != nil {
		return nil, err
	} else {
		return &PostgresqlStore{db: db}, nil
	}
}

func (p *PostgresqlStore) GetMembers() ([]cluster.Member, error) {
	members := make([]PgMember, 0)
	if result := p.db.Find(&members); result.Error != nil {
		return nil, result.Error
	} else {
		records := make([]cluster.Member, 0)
		for _, member := range members {
			records = append(records, cluster.Member{
				IP:     member.IP,
				Port:   member.Port,
				Epoch:  member.Epoch,
				Grains: member.Grains,
			})
		}
		return records, nil
	}
}

func (p *PostgresqlStore) Announce(member *cluster.Member) error {
	upsertClause := clause.OnConflict{UpdateAll: true}
	lockingClause := clause.Locking{Strength: "UPDATE"}
	pgMember := PgMember{
		IP:     member.IP,
		Port:   member.Port,
		Epoch:  member.Epoch,
		Grains: member.Grains,
	}
	return p.db.Clauses(upsertClause, lockingClause).Create(pgMember).Error
}

func (p *PostgresqlStore) GetSuspicions(member *cluster.Member) ([]cluster.Suspicion, error) {
	suspicions := make([]PgSuspicion, 0)
	if result := p.db.Where(&PgSuspicion{SuspectIP: member.IP, SuspectPort: member.Port}).Find(&suspicions); result.Error != nil {
		return nil, result.Error
	} else {
		result := make([]cluster.Suspicion, 0)
		for _, suspicion := range suspicions {
			result = append(result, cluster.Suspicion{
				Suspect: cluster.Member{
					IP:     suspicion.SuspectIP,
					Port:   suspicion.SuspectPort,
					Epoch:  0,
					Grains: []string{},
				},
				Accuser: cluster.Member{
					IP:     suspicion.AccuserIP,
					Port:   suspicion.AccuserPort,
					Epoch:  0,
					Grains: []string{},
				},
				Timestamp: suspicion.Timestamp,
			})
		}
		return result, nil
	}
}

func (p *PostgresqlStore) DeclareDead(member *cluster.Member) error {
	return p.db.Delete(PgMember{}, "ip = ? AND port = ?", member.IP, member.Port).Error
}

func (p *PostgresqlStore) DeclareSuspect(accuser, suspect *cluster.Member) error {
	suspicion := PgSuspicion{
		SuspectIP:   suspect.IP,
		SuspectPort: suspect.Port,
		AccuserIP:   accuser.IP,
		AccuserPort: accuser.Port,
		Timestamp:   time.Now().UnixMicro(),
	}
	upsertClause := clause.OnConflict{UpdateAll: true}
	lockingClause := clause.Locking{Strength: "UPDATE"}
	return p.db.Clauses(upsertClause, lockingClause).Create(&suspicion).Error
}

func (p *PostgresqlStore) RemoveSuspicions(member *cluster.Member) error {
	return p.db.Debug().Delete(&PgSuspicion{}, PgSuspicion{SuspectIP: member.IP, SuspectPort: member.Port}).Error
}

func (p *PostgresqlStore) LatestSuspicion(member *cluster.Member) (*cluster.Suspicion, error) {
	suspicions := make([]PgSuspicion, 0)
	if result := p.db.Where(&PgSuspicion{SuspectIP: member.IP, SuspectPort: member.Port}).Order("timestamp DESC").Limit(1).Find(&suspicions); result.Error != nil {
		return nil, result.Error
	} else {
		if len(suspicions) == 0 {
			return nil, nil
		}

		if len(suspicions) == 1 {
			return &cluster.Suspicion{
				Suspect: cluster.Member{
					IP:     suspicions[0].SuspectIP,
					Port:   suspicions[0].SuspectPort,
					Epoch:  -1,
					Grains: []string{},
				},
				Accuser: cluster.Member{
					IP:     suspicions[0].AccuserIP,
					Port:   suspicions[0].AccuserPort,
					Epoch:  -1,
					Grains: []string{},
				},
				Timestamp: suspicions[0].Timestamp,
			}, nil
		} else {
			return nil, fmt.Errorf("more than one suspicion found for %s:%d", member.IP, member.Port)
		}
	}
}

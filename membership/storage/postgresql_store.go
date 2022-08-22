package storage

import (
	"github.com/johnewart/go-orleans/membership"
	"github.com/lib/pq"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
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

func (p *PostgresqlStore) GetLatestTable() (*membership.MembershipTable, error) {
	members := make([]PgMember, 0)
	if result := p.db.Find(&members); result.Error != nil {
		return nil, result.Error
	} else {
		table := &membership.MembershipTable{}
		for _, member := range members {
			table.Members = append(table.Members, membership.Member{
				IP:     member.IP,
				Port:   member.Port,
				Epoch:  member.Epoch,
				Grains: member.Grains,
			})
		}
		return table, nil
	}
}

func (p *PostgresqlStore) Announce(member *membership.Member) error {
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

func (p *PostgresqlStore) Suspect(member *membership.Member) error {
	return p.db.Delete(PgMember{}, "ip = ? AND port = ?", member.IP, member.Port).Error
}

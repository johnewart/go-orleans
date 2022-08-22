package membership

type MembershipTable struct {
	Members []Member
}

type Member struct {
	IP     string
	Port   int
	Epoch  int64
	Grains []string
}

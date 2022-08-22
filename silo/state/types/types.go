package types

type GrainState struct {
	GrainType string
	GrainId   string
	StateData []byte
}

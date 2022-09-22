package silo

import "fmt"

type IncompatibleGrainError struct {
}

func (r IncompatibleGrainError) Error() string {
	return fmt.Sprintf("unable to handle grain, incompatible with the cluster currently")
}

package balancer

import (
	"errors"

	"github.com/popeyeio/gohbase/instance"
)

var (
	ErrNoInstance = errors.New("[gohbase] no instance available")
)

type Balancer interface {
	Name() string
	NewPicker([]instance.Instance) Picker
}

type Picker interface {
	Pick() (instance.Instance, error)
}

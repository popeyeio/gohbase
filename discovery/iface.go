package discovery

import (
	"github.com/popeyeio/gohbase/instance"
)

type Discovery interface {
	Discover() ([]instance.Instance, error)
}

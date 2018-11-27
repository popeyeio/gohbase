package discovery

import (
	"github.com/popeyeio/gohbase/instance"
)

type customDiscovery struct {
	instances []instance.Instance
}

var _ Discovery = (*customDiscovery)(nil)

func NewCustomDiscovery(instances ...instance.Instance) Discovery {
	return &customDiscovery{
		instances: instances,
	}
}

func (d *customDiscovery) Discover() ([]instance.Instance, error) {
	return d.instances, nil
}

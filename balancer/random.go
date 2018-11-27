package balancer

import (
	"math/rand"
	"sync"

	"github.com/popeyeio/gohbase/instance"
)

type RandomBalancer struct {
}

var _ Balancer = (*RandomBalancer)(nil)

func NewRandomBalancer() Balancer {
	return &RandomBalancer{}
}

func (RandomBalancer) Name() string {
	return "RandomBalancer"
}

func (b *RandomBalancer) NewPicker(instances []instance.Instance) Picker {
	return &randomPicker{
		instances: instances,
	}
}

type randomPicker struct {
	mu        sync.Mutex
	instances []instance.Instance
}

var _ Picker = (*randomPicker)(nil)

func (p *randomPicker) Pick() (instance.Instance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.instances) <= 0 {
		return nil, ErrNoInstance
	}

	i := rand.Intn(len(p.instances))
	return p.instances[i], nil
}

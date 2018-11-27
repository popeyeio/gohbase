package balancer

import (
	"sync"

	"github.com/popeyeio/gohbase/instance"
)

type RRBalancer struct {
}

var _ Balancer = (*RRBalancer)(nil)

func NewRRBalancer() Balancer {
	return &RRBalancer{}
}

func (RRBalancer) Name() string {
	return "RRBalancer"
}

func (b *RRBalancer) NewPicker(instances []instance.Instance) Picker {
	return &rrPicker{
		instances: instances,
	}
}

type rrPicker struct {
	mu        sync.Mutex
	instances []instance.Instance
	next      int
}

var _ Picker = (*rrPicker)(nil)

func (p *rrPicker) Pick() (instance.Instance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.instances) <= 0 {
		return nil, ErrNoInstance
	}

	ins := p.instances[p.next]
	p.next = (p.next + 1) % len(p.instances)
	return ins, nil
}

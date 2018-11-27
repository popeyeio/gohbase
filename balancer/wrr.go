package balancer

import (
	"sync"

	"github.com/popeyeio/gohbase/instance"
)

type WRRBalancer struct {
}

var _ Balancer = (*WRRBalancer)(nil)

func NewWRRBalancer() Balancer {
	return &WRRBalancer{}
}

func (WRRBalancer) Name() string {
	return "WRRBalancer"
}

func (b *WRRBalancer) NewPicker(instances []instance.Instance) Picker {
	balancer := &wrrPicker{
		instances: instances,
		weights:   make([]int, len(instances)),
	}
	for _, ins := range instances {
		balancer.total += ins.GetWeight()
	}
	return balancer
}

type wrrPicker struct {
	mu        sync.Mutex
	instances []instance.Instance
	weights   []int
	total     int
}

var _ Picker = (*wrrPicker)(nil)

func (p *wrrPicker) Pick() (instance.Instance, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if len(p.instances) <= 0 {
		return nil, ErrNoInstance
	}

	max := 0
	for i, ins := range p.instances {
		p.weights[i] += ins.GetWeight()
		if p.weights[i] > p.weights[max] {
			max = i
		}
	}
	p.weights[max] -= p.total
	return p.instances[max], nil
}

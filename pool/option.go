package pool

import (
	"time"

	"github.com/popeyeio/gohbase/balancer"
	"github.com/popeyeio/gohbase/discovery"
	"github.com/popeyeio/gohbase/gen/hbase"
	"github.com/popeyeio/gohbase/instance"
	"github.com/popeyeio/gohbase/lib/thrift"
)

type Option func(*pool)

func WithAddrs(addrs ...string) Option {
	return func(p *pool) {
		if len(addrs) == 0 {
			return
		}

		instances := make([]instance.Instance, len(addrs))
		for i, addr := range addrs {
			instances[i] = instance.NewCustomInstance(addr)
		}

		p.discovery = discovery.NewCustomDiscovery(instances...)
	}
}

func WithInstances(instances ...instance.Instance) Option {
	return func(p *pool) {
		if len(instances) == 0 {
			return
		}

		p.discovery = discovery.NewCustomDiscovery(instances...)
	}
}

func WithDiscovery(sd discovery.Discovery) Option {
	return func(p *pool) {
		if sd != nil {
			p.discovery = sd
		}
	}
}

func WithBalancer(lb balancer.Balancer) Option {
	return func(p *pool) {
		if lb != nil {
			p.balancer = lb
		}
	}
}

func WithUpdatePickerInterval(interval time.Duration) Option {
	return func(p *pool) {
		if interval >= 0 {
			p.updatePickerInterval = interval
		}
	}
}

func WithSocketTimeout(timeout time.Duration) Option {
	return func(p *pool) {
		if timeout >= 0 {
			p.socketTimeout = timeout
		}
	}
}

func WithTransportFactory(factory thrift.TTransportFactory) Option {
	return func(p *pool) {
		if factory != nil {
			p.transportFactory = factory
		}
	}
}

func WithMaxActive(maxActive int) Option {
	return func(p *pool) {
		if maxActive >= 0 {
			p.maxActive = maxActive
		}
	}
}

func WithMaxIdle(maxIdle int) Option {
	return func(p *pool) {
		if maxIdle >= 0 {
			p.maxIdle = maxIdle
		}
	}
}

func WithIdleTimeout(timeout time.Duration) Option {
	return func(p *pool) {
		if timeout >= 0 {
			p.idleTimeout = timeout
		}
	}
}

func WithCleanUpInterval(interval time.Duration) Option {
	return func(p *pool) {
		if interval >= 0 {
			p.cleanUpInterval = interval
		}
	}
}

func WithBlockMode(isBlocked bool) Option {
	return func(p *pool) {
		p.isBlocked = isBlocked
	}
}

func WithHealthChecker(checker func(*hbase.HbaseClient, time.Time) error) Option {
	return func(p *pool) {
		if checker != nil {
			p.healthChecker = checker
		}
	}
}

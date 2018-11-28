package pool

import (
	"container/list"
	"errors"
	"sync"
	"sync/atomic"
	"time"

	"github.com/popeyeio/gohbase/balancer"
	"github.com/popeyeio/gohbase/discovery"
	"github.com/popeyeio/gohbase/gen/hbase"
	"github.com/popeyeio/gohbase/lib/thrift"
)

var (
	now = time.Now

	ErrPoolFull   = errors.New("[gohbase] pool is full")
	ErrPoolClosed = errors.New("[gohbase] pool is closed")
)

type pool struct {
	sync.Mutex

	discovery            discovery.Discovery
	balancer             balancer.Balancer
	picker               balancer.Picker
	updatePickerInterval time.Duration

	socketTimeout    time.Duration
	transportFactory thrift.TTransportFactory
	protocolFactory  thrift.TProtocolFactory

	active          int
	maxActive       int
	maxIdle         int
	idleTimeout     time.Duration
	cleanUpInterval time.Duration
	idleNodes       *list.List

	isBlocked bool
	cond      *sync.Cond

	closed    int32
	closeChan chan struct{}

	healthChecker func(*hbase.HbaseClient, time.Time) error
}

var _ Pool = (*pool)(nil)

type idleNode struct {
	hc *hbase.HbaseClient
	t  time.Time
}

func NewPool(opts ...Option) Pool {
	p := &pool{
		discovery:        discovery.NewCustomDiscovery(),
		balancer:         balancer.NewRRBalancer(),
		transportFactory: thrift.NewTBufferedTransportFactory(4096),
		protocolFactory:  thrift.NewTBinaryProtocolFactoryDefault(),
		idleNodes:        list.New(),
		closeChan:        make(chan struct{}),
	}
	for _, opt := range opts {
		opt(p)
	}

	p.asyncUpdatePicker()
	p.asyncCleanUp()
	return p
}

func (p *pool) Get() (Client, error) {
	if p.IsClosed() {
		return nil, ErrPoolClosed
	}

	p.cleanUpIdleNodes(false)

	p.Lock()

	for {
		if p.IsClosed() {
			p.Unlock()
			return nil, ErrPoolClosed
		}

		for i, n := 0, p.idleNodes.Len(); i < n; i++ {
			e := p.idleNodes.Front()
			if e == nil {
				break
			}

			p.idleNodes.Remove(e)
			checker := p.healthChecker
			p.Unlock()

			in := e.Value.(*idleNode)
			if checker == nil || checker(in.hc, in.t) == nil {
				return &client{p: p, hc: in.hc}, nil
			}

			in.hc.Transport.Close()
			p.Lock()
			p.release()
		}

		if p.maxActive == 0 || p.active < p.maxActive {
			p.active += 1
			picker := p.picker
			p.Unlock()

			hc, err := p.getHbaseClient(picker)
			if err != nil {
				p.Lock()
				p.release()
				p.Unlock()
				return nil, err
			}

			return &client{p: p, hc: hc}, nil
		}

		if !p.isBlocked {
			p.Unlock()
			return nil, ErrPoolFull
		}

		p.wait()
	}
}

func (p *pool) Close() error {
	if !atomic.CompareAndSwapInt32(&p.closed, 0, 1) {
		return ErrPoolClosed
	}

	close(p.closeChan)
	p.cleanUpIdleNodes(true)
	return nil
}

func (p *pool) IsClosed() bool {
	return atomic.LoadInt32(&p.closed) == 1
}

func (p *pool) getHbaseClient(picker balancer.Picker) (*hbase.HbaseClient, error) {
	ins, err := picker.Pick()
	if err != nil {
		return nil, err
	}

	socket, err := thrift.NewTSocketTimeout(ins.GetAddr(), p.socketTimeout)
	if err != nil {
		return nil, err
	}

	transport, err := p.transportFactory.GetTransport(socket)
	if err != nil {
		return nil, err
	}

	if err = transport.Open(); err != nil {
		return nil, err
	}

	return hbase.NewHbaseClientFactory(transport, p.protocolFactory), nil
}

func (p *pool) asyncUpdatePicker() {
	p.updatePicker()

	if p.updatePickerInterval == 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(p.updatePickerInterval)
		for {
			select {
			case <-ticker.C:
				p.updatePicker()
			case <-p.closeChan:
				ticker.Stop()
			}
		}
	}()
}

func (p *pool) updatePicker() error {
	instances, err := p.discovery.Discover()
	if err != nil {
		return err
	}

	p.Lock()
	p.picker = p.balancer.NewPicker(instances)
	p.Unlock()
	return nil
}

func (p *pool) asyncCleanUp() {
	if p.cleanUpInterval == 0 {
		return
	}

	go func() {
		ticker := time.NewTicker(p.cleanUpInterval)
		for {
			select {
			case <-ticker.C:
				p.cleanUpIdleNodes(false)
			case <-p.closeChan:
				ticker.Stop()
			}
		}
	}()
}

func (p *pool) cleanUpIdleNodes(force bool) {
	p.Lock()
	defer p.Unlock()

	if !force && p.idleTimeout == 0 {
		return
	}

	for i, n := 0, p.idleNodes.Len(); i < n; i++ {
		e := p.idleNodes.Back()
		if e == nil {
			return
		}

		in := e.Value.(*idleNode)
		if !force && in.t.Add(p.idleTimeout).After(now()) {
			return
		}

		p.idleNodes.Remove(e)
		p.release()
		p.Unlock()

		in.hc.Transport.Close()
		p.Lock()
	}
}

func (p *pool) put(hc *hbase.HbaseClient, forceClose bool) error {
	if p.IsClosed() {
		_ = hc.Transport.Close()
		return ErrPoolClosed
	}

	p.Lock()

	if !forceClose {
		p.idleNodes.PushFront(&idleNode{hc: hc, t: now()})
		if p.maxIdle > 0 && p.idleNodes.Len() > p.maxIdle {
			hc = p.idleNodes.Remove(p.idleNodes.Back()).(*idleNode).hc
		} else {
			hc = nil
		}
	}

	if hc != nil {
		p.release()
		p.Unlock()
		return hc.Transport.Close()
	}

	p.notify()
	p.Unlock()
	return nil
}

func (p *pool) release() {
	p.active -= 1
	p.notify()
}

func (p *pool) notify() {
	if p.cond != nil {
		p.cond.Signal()
	}
}

func (p *pool) wait() {
	if p.cond == nil {
		p.cond = sync.NewCond(&p.Mutex)
	}
	p.cond.Wait()
}

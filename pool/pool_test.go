package pool

import (
	"sync"
	"testing"
	"time"
)

func TestPool_Get(t *testing.T) {
	p := newPool()
	defer p.Close()

	var wg sync.WaitGroup
	for i := 0; i < 5; i++ {
		c, err := p.Get()
		if err != nil {
			t.Logf("Get error - %v", err)
			return
		}

		wg.Add(1)
		go func(c Client, i int) {
			defer wg.Done()
			defer c.Close()

			time.Sleep(time.Second * time.Duration(i))

			names, err := c.GetTableNames()
			if err != nil {
				t.Logf("GetTableNames error - %v", err)
			} else {
				t.Logf("table names are %v", names)
			}
		}(c, i)
	}

	wg.Wait()
}

func TestPool_Close(t *testing.T) {
	p := newPool()
	c, err := p.Get()
	if err != nil {
		t.Logf("Get error - %v", err)
		return
	}

	p.Close()
	if err = c.Close(); err != ErrPoolClosed {
		t.Logf("close pool error - %v", err)
	}
	if _, err = p.Get(); err != ErrPoolClosed {
		t.Logf("close pool error - %v", err)
	}
}

func TestPool_IsClosed(t *testing.T) {
	p := newPool()
	p.Close()

	if !p.IsClosed() {
		t.Logf("close pool error")
	}
}

func newPool() Pool {
	opts := []Option{
		WithAddrs("6.6.6.6:6666", "8.8.8.8:8888"),
		WithUpdatePickerInterval(time.Second * 2),
		WithSocketTimeout(time.Second * 5),
		WithMaxActive(1),
		WithMaxIdle(1),
		WithIdleTimeout(time.Second * 1),
		WithCleanUpInterval(time.Second * 2),
		WithBlockMode(true),
	}
	return NewPool(opts...)
}

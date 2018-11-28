package main

import (
	"fmt"
	"time"

	"github.com/popeyeio/gohbase/gen/hbase"
	"github.com/popeyeio/gohbase/pool"
)

func main() {
	hbasePool := NewHbasePool()
	defer hbasePool.Close()

	hbaseClient, err := hbasePool.Get()
	if err != nil {
		fmt.Printf("Get error - %v\n", err)
		return
	}
	defer hbaseClient.Close()

	events := []*Event{
		&Event{
			EventTime: "20181127",
			Publisher: "popeye",
		},
	}
	if err = MutateEvents(hbaseClient, events); err != nil {
		fmt.Printf("MutateEvents error - %v\n", err)
	}
}

type Event struct {
	EventTime string
	Publisher string
}

func (e *Event) ParseToHbase() *hbase.BatchMutation {
	return &hbase.BatchMutation{
		Row: hbase.Text(e.EventTime),
		Mutations: []*hbase.Mutation{
			NewMutation("c:event_time", e.EventTime),
			NewMutation("c:publisher", e.Publisher),
		},
	}
}

func NewMutation(column, value string) *hbase.Mutation {
	return &hbase.Mutation{
		Column:     hbase.Text(column),
		Value:      hbase.Text(value),
		WriteToWAL: true,
	}
}

func NewHbasePool() pool.Pool {
	opts := []pool.Option{
		pool.WithAddrs("6.6.6.6:6666", "8.8.8.8:8888"),
		pool.WithUpdatePickerInterval(time.Second * 10),
		pool.WithSocketTimeout(time.Second * 5),
		pool.WithMaxActive(8),
		pool.WithMaxIdle(8),
		pool.WithIdleTimeout(time.Second * 5),
		pool.WithCleanUpInterval(time.Second * 30),
		pool.WithBlockMode(true),
	}
	return pool.NewPool(opts...)
}

func MutateEvents(cli pool.Client, events []*Event) error {
	batches := make([]*hbase.BatchMutation, len(events))
	for i, event := range events {
		batches[i] = event.ParseToHbase()
	}

	return cli.MutateRows("event", batches, nil)
}

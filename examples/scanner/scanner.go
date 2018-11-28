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

	events, err := ScanEvents(hbaseClient, "20181127", "20181128")
	if err != nil {
		fmt.Printf("ScanEvents error - %v\n", err)
		return
	}
	fmt.Printf("events:%v\n", events)
}

type Event struct {
	EventTime string
	Publisher string
}

func (e *Event) ParseFromHbase(r *hbase.TRowResult_) {
	e.EventTime = GetColumn(r, "c:event_time")
	e.Publisher = GetColumn(r, "c:publisher")
}

func GetColumn(r *hbase.TRowResult_, c string) (v string) {
	if tcell := r.Columns[c]; tcell != nil {
		v = string(tcell.Value)
	}
	return
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

func ScanEvents(cli pool.Client, startRow, stopRow string) ([]*Event, error) {
	tscan := &hbase.TScan{
		StartRow: hbase.Text(startRow),
		StopRow:  hbase.Text(stopRow),
	}

	scanID, err := cli.ScannerOpenWithScan("event", tscan, nil)
	if err != nil {
		return nil, err
	}
	defer cli.ScannerClose(scanID)

	var events []*Event
	for {
		results, err := cli.ScannerGetList(scanID, 128)
		if err != nil {
			return nil, err
		}
		if len(results) == 0 {
			return events, nil
		}

		for _, result := range results {
			event := &Event{}
			event.ParseFromHbase(result)
			events = append(events, event)
		}
	}
}

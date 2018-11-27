package pool

import (
	"errors"
	"sync"
	"sync/atomic"

	"github.com/popeyeio/gohbase/gen/hbase"
)

var (
	ErrClientClosed = errors.New("[gohbase] client is closed")
)

type client struct {
	sync.Mutex

	p    *pool
	hc   *hbase.HbaseClient
	errs Errors

	closed int32
}

var _ Client = (*client)(nil)

func (c *client) Close() error {
	if !atomic.CompareAndSwapInt32(&c.closed, 0, 1) {
		return ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	if c.p == nil || c.hc == nil {
		return nil
	}
	return c.p.put(c.hc, c.errs.Len() > 0)
}

func (c *client) IsClosed() bool {
	return atomic.LoadInt32(&c.closed) == 1
}

func (c *client) EnableTable(name string) (err error) {
	if c.IsClosed() {
		return ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	err = c.hc.EnableTable(hbase.Bytes(name))
	c.errs.Add(err)
	return
}

func (c *client) DisableTable(name string) (err error) {
	if c.IsClosed() {
		return ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	err = c.hc.DisableTable(hbase.Bytes(name))
	c.errs.Add(err)
	return
}

func (c *client) IsTableEnabled(name string) (rsp bool, err error) {
	if c.IsClosed() {
		return false, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	rsp, err = c.hc.IsTableEnabled(hbase.Bytes(name))
	c.errs.Add(err)
	return
}

func (c *client) GetTableNames() ([]string, error) {
	if c.IsClosed() {
		return nil, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	res, err := c.hc.GetTableNames()
	c.errs.Add(err)
	if err != nil {
		return nil, err
	}

	names := make([]string, len(res))
	for i := range res {
		names[i] = string(res[i])
	}
	return names, nil
}

func (c *client) GetColumnDescriptors(name string) (rsp map[string]*hbase.ColumnDescriptor, err error) {
	if c.IsClosed() {
		return nil, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	rsp, err = c.hc.GetColumnDescriptors(hbase.Text(name))
	c.errs.Add(err)
	return
}

func (c *client) GetTableRegions(name string) (rsp []*hbase.TRegionInfo, err error) {
	if c.IsClosed() {
		return nil, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	rsp, err = c.hc.GetTableRegions(hbase.Text(name))
	c.errs.Add(err)
	return
}

func (c *client) CreateTable(name string, cfs []*hbase.ColumnDescriptor) (err error) {
	if c.IsClosed() {
		return ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	err = c.hc.CreateTable(hbase.Text(name), cfs)
	c.errs.Add(err)
	return
}

func (c *client) DeleteTable(name string) (err error) {
	if c.IsClosed() {
		return ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	err = c.hc.DeleteTable(hbase.Text(name))
	c.errs.Add(err)
	return
}

func (c *client) Get(name, row, column string, attributes map[string]string) (rsp []*hbase.TCell, err error) {
	if c.IsClosed() {
		return nil, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	n := hbase.Text(name)
	r := hbase.Text(row)
	col := hbase.Text(column)
	attrs := make(map[string]hbase.Text)
	for k, v := range attributes {
		attrs[k] = hbase.Text(v)
	}

	rsp, err = c.hc.Get(n, r, col, attrs)
	c.errs.Add(err)
	return
}

func (c *client) GetRow(name, row string, attributes map[string]string) (rsp []*hbase.TRowResult_, err error) {
	if c.IsClosed() {
		return nil, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	n := hbase.Text(name)
	r := hbase.Text(row)
	attrs := make(map[string]hbase.Text)
	for k, v := range attributes {
		attrs[k] = hbase.Text(v)
	}

	rsp, err = c.hc.GetRow(n, r, attrs)
	c.errs.Add(err)
	return
}

func (c *client) GetRowWithColumns(name, row string, columns []string, attributes map[string]string) (rsp []*hbase.TRowResult_, err error) {
	if c.IsClosed() {
		return nil, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	n := hbase.Text(name)
	r := hbase.Text(row)
	cols := make([][]byte, len(columns))
	for i := range columns {
		cols[i] = []byte(columns[i])
	}
	attrs := make(map[string]hbase.Text)
	for k, v := range attributes {
		attrs[k] = hbase.Text(v)
	}

	rsp, err = c.hc.GetRowWithColumns(n, r, cols, attrs)
	c.errs.Add(err)
	return
}

func (c *client) GetRows(name string, rows []string, attributes map[string]string) (rsp []*hbase.TRowResult_, err error) {
	if c.IsClosed() {
		return nil, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	n := hbase.Text(name)
	rs := make([][]byte, len(rows))
	for i := range rows {
		rs[i] = []byte(rows[i])
	}
	attrs := make(map[string]hbase.Text)
	for k, v := range attributes {
		attrs[k] = hbase.Text(v)
	}

	rsp, err = c.hc.GetRows(n, rs, attrs)
	c.errs.Add(err)
	return
}

func (c *client) GetRowsWithColumns(name string, rows, columns []string, attributes map[string]string) (rsp []*hbase.TRowResult_, err error) {
	if c.IsClosed() {
		return nil, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	n := hbase.Text(name)
	rs := make([][]byte, len(rows))
	for i := range rows {
		rs[i] = []byte(rows[i])
	}
	cols := make([][]byte, len(columns))
	for i := range columns {
		cols[i] = []byte(columns[i])
	}
	attrs := make(map[string]hbase.Text)
	for k, v := range attributes {
		attrs[k] = hbase.Text(v)
	}

	rsp, err = c.hc.GetRowsWithColumns(n, rs, cols, attrs)
	c.errs.Add(err)
	return
}

func (c *client) MutateRow(name, row string, mutations []*hbase.Mutation, attributes map[string]string) (err error) {
	if c.IsClosed() {
		return ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	n := hbase.Text(name)
	r := hbase.Text(row)
	attrs := make(map[string]hbase.Text)
	for k, v := range attributes {
		attrs[k] = hbase.Text(v)
	}

	err = c.hc.MutateRow(n, r, mutations, attrs)
	c.errs.Add(err)
	return
}

func (c *client) MutateRows(name string, rowBatches []*hbase.BatchMutation, attributes map[string]string) (err error) {
	if c.IsClosed() {
		return ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	n := hbase.Text(name)
	attrs := make(map[string]hbase.Text)
	for k, v := range attributes {
		attrs[k] = hbase.Text(v)
	}

	err = c.hc.MutateRows(n, rowBatches, attrs)
	c.errs.Add(err)
	return
}

func (c *client) ScannerOpenWithScan(name string, scan *hbase.TScan, attributes map[string]string) (rsp hbase.ScannerID, err error) {
	if c.IsClosed() {
		return 0, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	n := hbase.Text(name)
	attrs := make(map[string]hbase.Text)
	for k, v := range attributes {
		attrs[k] = hbase.Text(v)
	}

	rsp, err = c.hc.ScannerOpenWithScan(n, scan, attrs)
	c.errs.Add(err)
	return
}

func (c *client) ScannerOpen(name, startRow string, columns []string, attributes map[string]string) (rsp hbase.ScannerID, err error) {
	if c.IsClosed() {
		return 0, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	n := hbase.Text(name)
	startR := hbase.Text(startRow)
	cols := make([][]byte, len(columns))
	for i := range columns {
		cols[i] = []byte(columns[i])
	}
	attrs := make(map[string]hbase.Text)
	for k, v := range attributes {
		attrs[k] = hbase.Text(v)
	}

	rsp, err = c.hc.ScannerOpen(n, startR, cols, attrs)
	c.errs.Add(err)
	return
}

func (c *client) ScannerOpenWithStop(name, startRow, stopRow string, columns []string, attributes map[string]string) (rsp hbase.ScannerID, err error) {
	if c.IsClosed() {
		return 0, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	n := hbase.Text(name)
	startR := hbase.Text(startRow)
	stopR := hbase.Text(stopRow)
	cols := make([][]byte, len(columns))
	for i := range columns {
		cols[i] = []byte(columns[i])
	}
	attrs := make(map[string]hbase.Text)
	for k, v := range attributes {
		attrs[k] = hbase.Text(v)
	}

	rsp, err = c.hc.ScannerOpenWithStop(n, startR, stopR, cols, attrs)
	c.errs.Add(err)
	return
}

func (c *client) ScannerOpenWithPrefix(name, startAndPrefix string, columns []string, attributes map[string]string) (rsp hbase.ScannerID, err error) {
	if c.IsClosed() {
		return 0, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	n := hbase.Text(name)
	p := hbase.Text(startAndPrefix)
	cols := make([][]byte, len(columns))
	for i := range columns {
		cols[i] = []byte(columns[i])
	}
	attrs := make(map[string]hbase.Text)
	for k, v := range attributes {
		attrs[k] = hbase.Text(v)
	}

	rsp, err = c.hc.ScannerOpenWithPrefix(n, p, cols, attrs)
	c.errs.Add(err)
	return
}

func (c *client) ScannerGet(id hbase.ScannerID) (rsp []*hbase.TRowResult_, err error) {
	if c.IsClosed() {
		return nil, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	rsp, err = c.hc.ScannerGet(id)
	c.errs.Add(err)
	return
}

func (c *client) ScannerGetList(id hbase.ScannerID, nbRows int32) (rsp []*hbase.TRowResult_, err error) {
	if c.IsClosed() {
		return nil, ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	rsp, err = c.hc.ScannerGetList(id, nbRows)
	c.errs.Add(err)
	return
}

func (c *client) ScannerClose(id hbase.ScannerID) (err error) {
	if c.IsClosed() {
		return ErrClientClosed
	}

	c.Lock()
	defer c.Unlock()

	err = c.hc.ScannerClose(id)
	c.errs.Add(err)
	return
}

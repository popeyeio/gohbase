package pool

import (
	"github.com/popeyeio/gohbase/gen/hbase"
)

type Pool interface {
	Get() (Client, error)
	Close() error
	IsClosed() bool
}

type Client interface {
	Close() error
	IsClosed() bool

	EnableTable(string) error
	DisableTable(string) error
	IsTableEnabled(string) (bool, error)
	GetTableNames() ([]string, error)
	GetColumnDescriptors(string) (map[string]*hbase.ColumnDescriptor, error)
	GetTableRegions(string) ([]*hbase.TRegionInfo, error)
	CreateTable(string, []*hbase.ColumnDescriptor) error
	DeleteTable(string) error
	Get(string, string, string, map[string]string) ([]*hbase.TCell, error)
	GetRow(string, string, map[string]string) ([]*hbase.TRowResult_, error)
	GetRowWithColumns(string, string, []string, map[string]string) ([]*hbase.TRowResult_, error)
	GetRows(string, []string, map[string]string) ([]*hbase.TRowResult_, error)
	GetRowsWithColumns(string, []string, []string, map[string]string) ([]*hbase.TRowResult_, error)
	MutateRow(string, string, []*hbase.Mutation, map[string]string) error
	MutateRows(string, []*hbase.BatchMutation, map[string]string) error
	ScannerOpenWithScan(string, *hbase.TScan, map[string]string) (hbase.ScannerID, error)
	ScannerOpen(string, string, []string, map[string]string) (hbase.ScannerID, error)
	ScannerOpenWithStop(string, string, string, []string, map[string]string) (hbase.ScannerID, error)
	ScannerOpenWithPrefix(string, string, []string, map[string]string) (hbase.ScannerID, error)
	ScannerGet(hbase.ScannerID) ([]*hbase.TRowResult_, error)
	ScannerGetList(hbase.ScannerID, int32) ([]*hbase.TRowResult_, error)
	ScannerClose(hbase.ScannerID) error
}

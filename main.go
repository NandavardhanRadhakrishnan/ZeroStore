package main

import (
	"fmt"
	"reflect"
)

type DataRow[K any] struct {
	len        uint16
	primaryKey K
	data       any
}

type IndexRow[K any] struct {
	primaryKey K
	location   int
}

type DataTable[K any] struct {
	dataTable  []DataRow[K]
	indexTable IndexTable[K]
	next       int
	compare    func(a, b K) int
}

type IndexTable[K any] struct {
	indexTable []IndexRow[K]
}

func NewDataTable[K any](compare func(a, b K) int) *DataTable[K] {
	return &DataTable[K]{
		dataTable:  []DataRow[K]{},
		indexTable: IndexTable[K]{},
		next:       -1,
		compare:    compare,
	}
}

func (dt *DataTable[K]) insert(primaryKey K, data any) {
	dt.next++
	newRow := newRow(primaryKey, data)
	if dt.next < len(dt.dataTable) {
		dt.dataTable[dt.next] = newRow
	} else {
		dt.dataTable = append(dt.dataTable, newRow)
	}
	dt.indexTable.insert(primaryKey, dt.next, dt.compare)
}

func (it *IndexTable[K]) insert(primaryKey K, location int, compare func(a, b K) int) {
	idx := 0
	for idx < len(it.indexTable) && compare(it.indexTable[idx].primaryKey, primaryKey) < 1 {
		idx++
	}
	it.indexTable = append(it.indexTable[:idx], append([]IndexRow[K]{{primaryKey: primaryKey, location: location}}, it.indexTable[idx:]...)...)
}

func newRow[K any](primaryKey K, data any) DataRow[K] {
	var p DataRow[K]
	dataSize := reflect.TypeOf(data).Size()
	if int(dataSize) < 65535 {
		p.len = uint16(dataSize)
		p.primaryKey = primaryKey
		p.data = data
	}
	return p
}

func main() {
	dt := NewDataTable(func(a, b int) int {
		if a > b {
			return 1
		} else {
			return 0
		}
	})
	dt.insert(1, []int{1, 2, 3, 4, 5})
	dt.insert(3, "data")
	dt.insert(2, 10284)
	fmt.Println(dt.dataTable)
	fmt.Println(dt.indexTable.indexTable)
}

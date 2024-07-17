package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"reflect"
)

type DataRow[K any] struct {
	Len           uint16
	PrimaryKeyLen uint16
	PrimaryKey    K
	Data          any
}

type IndexRow[K any] struct {
	primaryKey K
	offset     int
}

type DataTable[K any] struct {
	// dataTable  []DataRow[K]
	// next       int
	indexTable IndexTable[K]
	compare    func(a, b K) int
	dataFile   *os.File
	indexFile  *os.File
}

type IndexTable[K any] struct {
	indexTable []IndexRow[K]
}

func NewDataTable[K any](compare func(a, b K) int, dataFilePath string, indexFilePath string) (*DataTable[K], error) {

	dataFile, err := os.Create((dataFilePath))
	if err != nil {
		return nil, err
	}

	indexFile, err := os.Create(indexFilePath)
	if err != nil {
		dataFile.Close()
		return nil, err
	}

	return &DataTable[K]{
		indexTable: IndexTable[K]{},
		compare:    compare,
		dataFile:   dataFile,
		indexFile:  indexFile,
	}, nil
}

func (dt *DataTable[K]) insert(primaryKey K, data any) error {
	dataRow := newRow(primaryKey, data)
	offset, err := dt.serializeData(dataRow)
	if err != nil {
		return err
	}

	dt.indexTable.insert(primaryKey, offset, dt.compare)
	return nil
}

func (it *IndexTable[K]) insert(primaryKey K, offset int, compare func(a, b K) int) {
	idx := 0
	for idx < len(it.indexTable) && compare(it.indexTable[idx].primaryKey, primaryKey) < 0 {
		idx++
	}
	it.indexTable = append(it.indexTable[:idx], append([]IndexRow[K]{{primaryKey: primaryKey, offset: offset}}, it.indexTable[idx:]...)...)
}

func (dt *DataTable[K]) serializeData(dataRow DataRow[K]) (int, error) {
	offset, err := dt.dataFile.Seek(0, os.SEEK_END)
	if err != nil {
		return 0, err
	}

	encoder := gob.NewEncoder(dt.dataFile)
	if err := encoder.Encode(dataRow); err != nil {
		return 0, err
	}

	return int(offset), nil
}

func (dt *DataTable[K]) unserializeData(offset int) (DataRow[K], error) {
	_, err := dt.dataFile.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return DataRow[K]{}, err
	}

	var dataRow DataRow[K]

	decoder := gob.NewDecoder(dt.dataFile)
	if err := decoder.Decode(&dataRow); err != nil {
		return DataRow[K]{}, err
	}

	return dataRow, nil
}

func (dt *DataTable[K]) Close() {
	dt.dataFile.Close()
}

func (dt *DataTable[K]) SaveIndex() error {
	if err := dt.indexFile.Truncate(0); err != nil {
		return err
	}
	if _, err := dt.indexFile.Seek(0, 0); err != nil {
		return err
	}
	encoder := gob.NewEncoder(dt.indexFile)
	return encoder.Encode(dt.indexTable.indexTable)
}

func (dt *DataTable[K]) LoadIndex(indexFilePath string) error {
	indexFile, err := os.Open(indexFilePath)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	decoder := gob.NewDecoder(indexFile)
	if err := decoder.Decode(&dt.indexTable.indexTable); err != nil {
		return err
	}

	return nil
}

func newRow[K any](primaryKey K, data any) DataRow[K] {
	var p DataRow[K]
	dataSize := reflect.TypeOf(data).Size()
	pkSize := reflect.TypeOf(primaryKey).Size()
	if int(dataSize) < 65535 && int(pkSize) < 65535 {
		p.Len = uint16(dataSize)
		p.PrimaryKeyLen = uint16(pkSize)
		p.PrimaryKey = primaryKey
		p.Data = data
	}
	return p
}

func (dt DataTable[K]) loadData() {

}

func main() {
	dt, _ := NewDataTable(func(a, b int) int {
		if a > b {
			return 1
		} else {
			return 0
		}
	}, "data.bin", "index.bin")
	dt.insert(1, []int{1, 2, 3, 4, 5})
	dt.insert(3, "data")
	dt.insert(4, "hello world")
	dt.insert(2, 10284)
	dt.SaveIndex()
	dt.LoadIndex("index.bin")
	fmt.Println(dt.indexTable.indexTable)
	// fmt.Println(dt.dataTable)
	// fmt.Println(dt.indexTable.indexTable)
	fmt.Println(dt.unserializeData(dt.indexTable.indexTable[0].offset))
	fmt.Println(dt.unserializeData(dt.indexTable.indexTable[1].offset))
	fmt.Println(dt.unserializeData(dt.indexTable.indexTable[2].offset))
	fmt.Println(dt.unserializeData(dt.indexTable.indexTable[3].offset))
}

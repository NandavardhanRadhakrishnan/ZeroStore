package main

import (
	"ZeroStore/btree"
	"encoding/gob"
	"fmt"
	"os"
)

type DataRow[K any] struct {
	PrimaryKey K
	Data       any
}

type DataTable[K any] struct {
	indexTable btree.BTree[K, int]
	compare    func(a, b K) int
	dataFile   *os.File
	indexFile  *os.File
}

func NewDataTable[K any](compare func(a, b K) int, dataFilePath string, indexFilePath string, btreeDegree int) (*DataTable[K], error) {

	dataFile, err := os.Create((dataFilePath))
	if err != nil {
		return nil, err
	}

	indexFile, err := os.Create(indexFilePath)
	if err != nil {
		dataFile.Close()
		return nil, err
	}
	bt := btree.NewBTree[K, int](btreeDegree, compare)
	return &DataTable[K]{
		indexTable: *bt,
		compare:    compare,
		dataFile:   dataFile,
		indexFile:  indexFile,
	}, nil
}

func (dt *DataTable[K]) Insert(primaryKey K, data any) error {
	dataRow := newRow(primaryKey, data)
	offset, err := dt.serializeData(dataRow)
	if err != nil {
		return err
	}

	dt.indexTable.Insert(primaryKey, offset)
	return nil
}

// func (it *IndexTable[K]) insert(primaryKey K, offset int, compare func(a, b K) int) {
// 	idx := 0
// 	for idx < len(it.indexTable) && compare(it.indexTable[idx].primaryKey, primaryKey) < 1 {
// 		idx++
// 	}
// 	it.indexTable = append(it.indexTable[:idx], append([]IndexRow[K]{{primaryKey: primaryKey, offset: offset}}, it.indexTable[idx:]...)...)
// }

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

func (dt *DataTable[K]) SaveIndex() error {
	if err := dt.indexFile.Truncate(0); err != nil {
		return err
	}
	if _, err := dt.indexFile.Seek(0, 0); err != nil {
		return err
	}
	encoder := gob.NewEncoder(dt.indexFile)
	return encoder.Encode(dt.indexTable)
}

func (dt *DataTable[K]) LoadIndex(indexFilePath string) error {
	indexFile, err := os.Open(indexFilePath)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	decoder := gob.NewDecoder(indexFile)
	if err := decoder.Decode(&dt.indexTable); err != nil {
		return err
	}

	return nil
}

func newRow[K any](primaryKey K, data any) DataRow[K] {
	var p DataRow[K]
	p.PrimaryKey = primaryKey
	p.Data = data
	return p
}

func main() {

	dt, _ := NewDataTable(func(a, b int) int {
		if a > b {
			return 1
		} else if a == b {
			return 0
		} else {
			return -1
		}
	}, "data.bin", "index.bin", 2)

	for i := 0; i < 10; i++ {
		s := fmt.Sprintf("data:%d", i)
		dt.Insert(i, s)
	}

	dt.SaveIndex()
	dt.LoadIndex("index.bin")

	for i := 0; i < 10; i++ {
		offset, _ := dt.indexTable.Search(i)
		fmt.Println(dt.unserializeData(offset))
	}

}

// TODO figure out UD of CRUD
// look at boltDB for data storage strats

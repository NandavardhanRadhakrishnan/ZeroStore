package main

import (
	"ZeroStore/datastructure/btree"
	"ZeroStore/helper"
	test "ZeroStore/utility"
	"encoding/gob"
	"fmt"
	"os"
)

type DataRow[K any, V any] struct {
	PrimaryKey K
	Data       V
}

type DataTable[K any, V any] struct {
	indexTable  btree.BTree[K, int]
	compare     func(a, b K) int
	dataFile    *os.File
	indexFile   *os.File
	btreeDegree int
}

func NewDataTable[K any, V any](compare func(a, b K) int, dataFilePath string, indexFilePath string, btreeDegree int, forceOverwrite bool) (*DataTable[K, V], error) {

	var dataFile *os.File
	var indexFile *os.File
	var err error

	if helper.FileExists(dataFilePath) && !forceOverwrite {
		dataFile, err = os.Open(dataFilePath)
		if err != nil {
			return nil, err
		}
	} else {
		dataFile, err = os.Create((dataFilePath))
		if err != nil {
			return nil, err
		}
	}

	if helper.FileExists(indexFilePath) && !forceOverwrite {
		indexFile, err = os.Open(indexFilePath)
		if err != nil {
			return nil, err
		}
	} else {
		indexFile, err = os.Create(indexFilePath)
		if err != nil {
			return nil, err
		}
	}
	gob.Register(DataRow[K, V]{})

	bt := btree.NewBTree[K, int](btreeDegree, compare)
	return &DataTable[K, V]{
		indexTable:  *bt,
		compare:     compare,
		dataFile:    dataFile,
		indexFile:   indexFile,
		btreeDegree: btreeDegree,
	}, nil
}

func (dt *DataTable[K, V]) Insert(primaryKey K, data V) error {
	dataRow := newRow(primaryKey, data)
	offset, err := dt.SerializeData(dataRow)
	if err != nil {
		return err
	}

	dt.indexTable.Insert(primaryKey, offset)
	return nil
}

func (dt *DataTable[K, V]) SerializeData(dataRow DataRow[K, V]) (int, error) {
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

func (dt *DataTable[K, V]) UnserializeData(offset int) (DataRow[K, V], error) {
	_, err := dt.dataFile.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return DataRow[K, V]{}, err
	}

	var dataRow DataRow[K, V]

	decoder := gob.NewDecoder(dt.dataFile)
	if err := decoder.Decode(&dataRow); err != nil {
		return DataRow[K, V]{}, err
	}

	return dataRow, nil
}

func (dt *DataTable[K, V]) SaveIndex() error {
	err := dt.indexTable.Save(dt.indexFile)
	if err != nil {
		return err
	}
	return nil
}

func (dt *DataTable[K, V]) LoadIndex(indexFilePath string) error {
	indexFile, err := os.Open(indexFilePath)
	if err != nil {
		return err
	}
	defer indexFile.Close()

	err = dt.indexTable.Load(indexFile)
	if err != nil {
		return err
	}
	return nil
}

func newRow[K any, V any](primaryKey K, data V) DataRow[K, V] {
	var p DataRow[K, V]
	p.PrimaryKey = primaryKey
	p.Data = data
	return p
}

func compare(a, b int) int {
	if a > b {
		return 1
	} else if a == b {
		return 0
	} else {
		return -1
	}
}

// func main() {

// 	dt, _ := NewDataTable(compare, "data.bin", "index.bin", 4, true)

// 	for i := 1; i < 100001; i++ {
// 		s := fmt.Sprintf("data:%d", i)
// 		dt.Insert(i, s)
// 	}

// 	dt.SaveIndex()
// 	dt.LoadIndex("index.bin")
// 	// dt.indexTable.PrettyPrint()

// 	for i := 1; i < 11; i++ {
// 		offset, found := dt.indexTable.Search(i)
// 		if found {
// 			fmt.Println(dt.UnserializeData(offset))
// 		} else {
// 			fmt.Printf("%d not found\n", i)
// 		}
// 	}

// 	offset, found := dt.indexTable.Search(582)
// 	if found {
// 		fmt.Println(dt.UnserializeData(offset))
// 	} else {
// 		fmt.Printf("%d not found\n", 582)
// 	}

// }

func main() {
	dt, _ := NewDataTable[int, test.Row](compare, "data.bin", "index.bin", 4, false)

	// for i := 1; i < test.NumberOfRows; i++ {
	// 	a := test.GenerateRow(1024)
	// 	dt.Insert(i, a)
	// }

	// dt.SaveIndex()
	dt.LoadIndex("index.bin")
	// dt.indexTable.PrettyPrint()

	i := test.NumberOfRows - 32
	offset, found := dt.indexTable.Search(i)
	if found {
		fmt.Println(dt.UnserializeData(offset))
	} else {
		fmt.Printf("%d not found\n", i)
	}

}

// TODO figure out UD of CRUD
// look at boltDB for data storage strats

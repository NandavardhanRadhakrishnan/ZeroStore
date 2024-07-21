package engine

import (
	"ZeroStore/datastructure/btree"
	"ZeroStore/helper"
	"encoding/gob"
	"fmt"
	"os"
	"unsafe"
)

type DataRow[K any, V any] struct {
	PrimaryKey K
	Data       V
	IsValid    bool
}

type FreeNode struct {
	Offset int64
	Size   int64
}

type DataTable[K any, V any] struct {
	IndexTable  btree.BTree[K, int]
	Compare     func(a, b K) int
	DataFile    *os.File
	IndexFile   *os.File
	freeFile    *os.File
	BtreeDegree int
	FreeList    []FreeNode
}

func NewDataTable[K any, V any](compare func(a, b K) int, dbName string, btreeDegree int, forceOverwrite bool) (*DataTable[K, V], error) {

	var dataFile *os.File
	var indexFile *os.File
	var freeFile *os.File
	var err error
	var freeList []FreeNode
	dataFilePath := dbName + "_data.bin"
	indexFilePath := dbName + "_index.bin"
	freeFilePath := dbName + "_free.bin"
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

	if helper.FileExists(freeFilePath) && !forceOverwrite {
		freeFile, err = os.Open(freeFilePath)
		if err != nil {
			return nil, err
		}
		defer freeFile.Close()

		info, err := freeFile.Stat()
		if err != nil {
			return nil, err
		}

		if info.Size() > 0 {
			decoder := gob.NewDecoder(freeFile)
			if err := decoder.Decode(&freeList); err != nil {
				return nil, err
			}
		}
	} else {
		freeFile, err = os.Create(freeFilePath)
		if err != nil {
			return nil, err
		}
	}

	gob.Register(DataRow[K, V]{})

	bt := btree.NewBTree[K, int](btreeDegree, compare)
	return &DataTable[K, V]{
		IndexTable:  *bt,
		Compare:     compare,
		DataFile:    dataFile,
		IndexFile:   indexFile,
		freeFile:    freeFile,
		BtreeDegree: btreeDegree,
		FreeList:    freeList,
	}, nil
}

func (dt *DataTable[K, V]) Insert(primaryKey K, data V) error {
	dataRow := newRow(primaryKey, data)
	offset, err := dt.SerializeData(dataRow, -1)
	if err != nil {
		return err
	}

	dt.IndexTable.Insert(primaryKey, offset)
	return nil
}

func (dt *DataTable[K, V]) UpdateWithData(primaryKey K, data V) error {

	if _, err := dt.Delete(primaryKey); err != nil {
		return err
	}

	if err := dt.Insert(primaryKey, data); err != nil {
		return err
	}

	return nil
}

func (dt *DataTable[K, V]) UpdateWithFunc(primaryKey K, updateFunc func(data V) V) error {
	var oldDr DataRow[K, V]
	var err error
	if oldDr, err = dt.Delete(primaryKey); err != nil {
		return err
	}
	data := updateFunc(oldDr.Data)
	if err := dt.Insert(primaryKey, data); err != nil {
		return err
	}

	return nil
}

func (dt *DataTable[K, V]) Delete(primaryKey K) (DataRow[K, V], error) {
	var zero DataRow[K, V]

	offset, found := dt.IndexTable.Delete(primaryKey)
	if !found {
		return zero, fmt.Errorf("key not found")
	}
	dr, err := dt.UnserializeData(offset)
	if err != nil {
		return zero, err
	}
	dr.IsValid = false
	_, err = dt.SerializeData(dr, offset)
	if err != nil {
		return zero, err
	}

	dt.FreeList = append(dt.FreeList, FreeNode{Offset: int64(offset), Size: int64(unsafe.Sizeof(dr))})

	if err := gob.NewEncoder(dt.freeFile).Encode(dt.FreeList); err != nil {
		return zero, err
	}

	return dr, nil
}

func (dt *DataTable[K, V]) SerializeData(dataRow DataRow[K, V], location int) (int, error) {
	var offset int64
	var err error

	if location == -1 {
		offset, err = dt.DataFile.Seek(0, os.SEEK_END)
		if err != nil {
			return 0, err
		}
	} else if location >= 0 {
		offset, err = dt.DataFile.Seek(int64(location), os.SEEK_SET)
		if err != nil {
			return 0, err
		}
	}

	encoder := gob.NewEncoder(dt.DataFile)
	if err := encoder.Encode(dataRow); err != nil {
		return 0, err
	}

	return int(offset), nil
}

func (dt *DataTable[K, V]) UnserializeData(offset int) (DataRow[K, V], error) {
	_, err := dt.DataFile.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return DataRow[K, V]{}, err
	}

	var dataRow DataRow[K, V]

	decoder := gob.NewDecoder(dt.DataFile)
	if err := decoder.Decode(&dataRow); err != nil {
		return DataRow[K, V]{}, err
	}

	return dataRow, nil
}

func (dt *DataTable[K, V]) SaveIndex() error {
	err := dt.IndexTable.Save(dt.IndexFile)
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

	err = dt.IndexTable.Load(indexFile)
	if err != nil {
		return err
	}
	return nil
}

func newRow[K any, V any](primaryKey K, data V) DataRow[K, V] {
	var p DataRow[K, V]
	p.PrimaryKey = primaryKey
	p.Data = data
	p.IsValid = true
	return p
}

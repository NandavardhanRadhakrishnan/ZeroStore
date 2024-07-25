package storageEngine

import (
	"ZeroStore/datastructure/btree"
	"ZeroStore/helper"
	"encoding/gob"
	"fmt"
	"os"
	"reflect"
	"sort"
)

type DataRow[K comparable, V any] struct {
	PrimaryKey K
	Data       V
	IsValid    bool
}

type FreeNode struct {
	Offset int64
	Size   int64
}

type DataTable[K comparable, V any] struct {
	Columns     []string
	IndexTable  btree.BTree[K, int]
	Compare     func(a, b K) int
	DataFile    *os.File
	IndexFile   *os.File
	freeFile    *os.File
	BtreeDegree int
	FreeList    []FreeNode
}

func NewDataTable[K comparable, V any](compare func(a, b K) int, dbName string, btreeDegree int, forceOverwrite bool) (*DataTable[K, V], error) {

	var dataFile *os.File
	var indexFile *os.File
	var freeFile *os.File
	var cols []string
	var err error
	var freeList []FreeNode
	dataFilePath := dbName + "_data.bin"
	indexFilePath := dbName + "_index.bin"
	freeFilePath := dbName + "_free.bin"

	if cols, err = helper.GetFieldNames[V](); err != nil {
		return nil, err
	}

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
		Columns:     cols,
		IndexTable:  *bt,
		Compare:     compare,
		DataFile:    dataFile,
		IndexFile:   indexFile,
		freeFile:    freeFile,
		BtreeDegree: btreeDegree,
		FreeList:    freeList,
	}, nil
}

func (dt *DataTable[K, V]) GetAll() ([]DataRow[K, V], error) {
	var rows []DataRow[K, V]
	var dr DataRow[K, V]
	var err error

	for _, offset := range dt.IndexTable.GetAll() {
		if dr, err = dt.UnserializeData(offset.Value); err != nil {
			return nil, err
		}
		rows = append(rows, dr)
	}
	return rows, nil
}

func (dt *DataTable[K, V]) Search(primaryKey K) (DataRow[K, V], error) {
	if offset, found := dt.IndexTable.Search(primaryKey); found {
		return dt.UnserializeData(offset)
	}
	var zero DataRow[K, V]
	return zero, fmt.Errorf("key not found")
}

func (dt *DataTable[K, V]) Insert(primaryKey K, data V) error {
	dataRow := newRow(primaryKey, data)
	reqSize := int64(helper.RealSizeOf(dataRow))
	offset := -1
	var err error

	for i, f := range dt.FreeList {
		if reqSize <= f.Size {
			offset = int(f.Offset)
			if f.Size-reqSize > 0 {
				dt.FreeList[i] = FreeNode{Offset: int64(offset) + reqSize, Size: f.Size - reqSize}
			} else {
				dt.FreeList = dt.FreeList[1:]
			}
			if err := gob.NewEncoder(dt.freeFile).Encode(dt.FreeList); err != nil {
				return err
			}
			break
		}
	}

	offset, err = dt.SerializeData(dataRow, offset)
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

	dt.FreeList = append(dt.FreeList, FreeNode{Offset: int64(offset), Size: int64(helper.RealSizeOf(dr))})

	sort.Slice(dt.FreeList, func(i, j int) bool {
		return dt.FreeList[i].Size < dt.FreeList[j].Size
	})

	if err := gob.NewEncoder(dt.freeFile).Encode(dt.FreeList); err != nil {
		return zero, err
	}

	return dr, nil
}

// TODO error handling for where and select

func (dt *DataTable[K, V]) Where(filter func(DataRow[K, V]) bool) ([]K, error) {

	var rows []DataRow[K, V]
	var keys []K
	var err error

	rows, err = dt.GetAll()
	if err != nil {
		return nil, err
	}
	for _, row := range rows {
		if filter(row) {
			keys = append(keys, row.PrimaryKey)
		}
	}
	return keys, nil
}

func (dt *DataTable[K, V]) Select(keys []K, columns []string) ([]interface{}, error) {

	var dataRow DataRow[K, V]
	var result []interface{}
	var projectedRow interface{}
	var err error

	for _, k := range keys {
		if dataRow, err = dt.Search(k); err != nil {
			return nil, err
		}
		if projectedRow, err = projectRow(dataRow, columns); err != nil {
			return nil, err
		}
		result = append(result, projectedRow)
	}
	return result, nil
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

func (dt *DataTable[K, V]) Compact() error {
	tempDataFilePath := dt.DataFile.Name() + ".tmp"
	tempDataFile, err := os.Create(tempDataFilePath)
	if err != nil {
		return err
	}

	newOffsets := make(map[K]int)
	for _, item := range dt.IndexTable.GetAll() {
		oldOffset := item.Value
		dataRow, err := dt.UnserializeData(oldOffset)
		if err != nil {
			return err
		}

		newOffset, err := dt.serializeDataToFile(dataRow, tempDataFile)
		if err != nil {
			return err
		}
		newOffsets[item.Key] = newOffset
	}

	dt.DataFile.Close()
	tempDataFile.Close()

	err = os.Rename(tempDataFilePath, dt.DataFile.Name())
	if err != nil {
		return err
	}

	dt.DataFile, err = os.OpenFile(dt.DataFile.Name(), os.O_RDWR, 0666)
	if err != nil {
		return err
	}

	dt.IndexTable.Clear()
	for key, newOffset := range newOffsets {
		dt.IndexTable.Insert(key, newOffset)
	}

	err = dt.SaveIndex()
	if err != nil {
		return err
	}

	err = dt.freeFile.Truncate(0)
	if err != nil {
		return err
	}

	return nil
}

func (dt *DataTable[K, V]) serializeDataToFile(dataRow DataRow[K, V], file *os.File) (int, error) {
	offset, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return 0, err
	}

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(dataRow); err != nil {
		return 0, err
	}

	return int(offset), nil
}

func newRow[K comparable, V any](primaryKey K, data V) DataRow[K, V] {
	var p DataRow[K, V]
	p.PrimaryKey = primaryKey
	p.Data = data
	p.IsValid = true
	return p
}

func projectRow[K comparable, V any](row DataRow[K, V], columns []string) (interface{}, error) {
	val := reflect.ValueOf(row.Data)
	// typ := reflect.TypeOf(row.Data)

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("Data is not a struct")
	}

	result := make(map[string]interface{})
	for _, column := range columns {
		field := val.FieldByName(column)
		if !field.IsValid() {
			return nil, fmt.Errorf("field %s not found in struct", column)
		}
		result[column] = field.Interface()
	}
	return result, nil
}

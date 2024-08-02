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

type Result[T any] struct {
	Value T
	Err   error
}

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

func NewResult[T any](value T, err error) Result[T] {
	return Result[T]{Value: value, Err: err}
}

func NewDataTable[K comparable, V any](compare func(a, b K) int, dbName string, btreeDegree int) (*DataTable[K, V], error) {

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

	dataFile, err = os.OpenFile(dataFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
	indexFile, err = os.OpenFile(indexFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	freeFile, err = os.OpenFile(freeFilePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}
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

func (dt *DataTable[K, V]) GetAll() <-chan Result[DataRow[K, V]] {
	resultsChan := make(chan Result[DataRow[K, V]])

	go func() {
		defer close(resultsChan)
		for _, offset := range dt.IndexTable.GetAll() {
			res := dt.UnserializeData(offset.Value)
			if res.Err != nil {
				resultsChan <- Result[DataRow[K, V]]{Err: res.Err}
				return
			}
			resultsChan <- Result[DataRow[K, V]]{Value: res.Value}
		}
	}()

	return resultsChan
}

func (dt *DataTable[K, V]) Search(primaryKey K) Result[DataRow[K, V]] {
	if offset, found := dt.IndexTable.Search(primaryKey); found {
		res := dt.UnserializeData(offset)
		return Result[DataRow[K, V]]{Value: res.Value, Err: res.Err}
	}
	return Result[DataRow[K, V]]{Err: fmt.Errorf("key not found")}
}

func (dt *DataTable[K, V]) Insert(primaryKey K, data V) Result[any] {
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
				return Result[any]{Err: err}
			}
			break
		}
	}

	res := dt.SerializeData(dataRow, offset)
	if res.Err != nil {
		return Result[any]{Err: err}
	}

	dt.IndexTable.Insert(primaryKey, res.Value)

	return Result[any]{Value: nil}
}

func (dt *DataTable[K, V]) UpdateWithData(primaryKey K, data V) Result[any] {
	if res := dt.Delete(primaryKey); res.Err != nil {
		return Result[any]{Err: res.Err}
	}

	if res := dt.Insert(primaryKey, data); res.Err != nil {
		return Result[any]{Err: res.Err}
	}

	return Result[any]{Value: nil}
}

func (dt *DataTable[K, V]) UpdateWithFunc(primaryKey K, updateFunc func(data V) V) Result[any] {
	deleteResult := dt.Delete(primaryKey)
	if deleteResult.Err != nil {
		return Result[any]{Err: deleteResult.Err}
	}

	oldDr := deleteResult.Value

	data := updateFunc(oldDr.Data)

	insertResult := dt.Insert(primaryKey, data)
	if insertResult.Err != nil {
		return Result[any]{Err: insertResult.Err}
	}

	return Result[any]{Value: nil}
}

func (dt *DataTable[K, V]) Delete(primaryKey K) Result[DataRow[K, V]] {

	offset, found := dt.IndexTable.Delete(primaryKey)
	if !found {
		return Result[DataRow[K, V]]{Err: fmt.Errorf("key not found")}
	}
	res := dt.UnserializeData(offset)
	if res.Err != nil {
		return Result[DataRow[K, V]]{Err: res.Err}
	}
	res.Value.IsValid = false
	resOffset := dt.SerializeData(res.Value, offset)
	if resOffset.Err != nil {
		return Result[DataRow[K, V]]{Err: resOffset.Err}
	}

	dt.FreeList = append(dt.FreeList, FreeNode{Offset: int64(offset), Size: int64(helper.RealSizeOf(res.Value))})

	sort.Slice(dt.FreeList, func(i, j int) bool {
		return dt.FreeList[i].Size < dt.FreeList[j].Size
	})

	if err := gob.NewEncoder(dt.freeFile).Encode(dt.FreeList); err != nil {
		return Result[DataRow[K, V]]{Err: err}
	}

	return Result[DataRow[K, V]]{Value: res.Value}
}

func (dt *DataTable[K, V]) GetFromKeys(keys []K) Result[[]DataRow[K, V]] {
	var rows []DataRow[K, V]
	for _, key := range keys {
		res := dt.Search(key)
		if res.Err != nil {
			return Result[[]DataRow[K, V]]{Err: res.Err}
		}
		rows = append(rows, res.Value)
	}
	return Result[[]DataRow[K, V]]{Value: rows}
}

func (dt *DataTable[K, V]) Where(filter func(DataRow[K, V]) bool) Result[[]K] {
	rowsChan := dt.GetAll()
	var keys []K

	for result := range rowsChan {
		if result.Err != nil {
			return Result[[]K]{Err: result.Err}
		}
		if filter(result.Value) {
			keys = append(keys, result.Value.PrimaryKey)
		}
	}
	return Result[[]K]{Value: keys}
}

func (dt *DataTable[K, V]) Select(keys []K, resultType interface{}) chan Result[interface{}] {
	result := make(chan Result[interface{}])
	resType := reflect.TypeOf(resultType)
	go func() {
		defer close(result)
		var dataRow DataRow[K, V]
		var projectedRow interface{}
		var err error

		for _, k := range keys {
			searchResult := dt.Search(k)
			if searchResult.Err != nil {
				result <- Result[interface{}]{Err: searchResult.Err}
				continue // Use continue instead of return to process all keys
			}
			dataRow = searchResult.Value

			projectedRow, err = ProjectRow(dataRow, resType)
			if err != nil {
				result <- Result[interface{}]{Err: err}
				continue // Use continue instead of return to process all keys
			}

			result <- Result[interface{}]{Value: projectedRow}
		}
	}()

	return result
}

func (dt *DataTable[K, V]) SerializeData(dataRow DataRow[K, V], location int) Result[int] {
	var offset int64
	var err error

	if location == -1 {
		offset, err = dt.DataFile.Seek(0, os.SEEK_END)
		if err != nil {
			return Result[int]{Err: err}
		}
	} else if location >= 0 {
		offset, err = dt.DataFile.Seek(int64(location), os.SEEK_SET)
		if err != nil {
			return Result[int]{Err: err}
		}
	}

	encoder := gob.NewEncoder(dt.DataFile)
	if err := encoder.Encode(dataRow); err != nil {
		return Result[int]{Err: err}
	}

	return Result[int]{Value: int(offset)}
}

func (dt *DataTable[K, V]) UnserializeData(offset int) Result[DataRow[K, V]] {
	_, err := dt.DataFile.Seek(int64(offset), os.SEEK_SET)
	if err != nil {
		return Result[DataRow[K, V]]{Err: err}
	}

	var dataRow DataRow[K, V]

	decoder := gob.NewDecoder(dt.DataFile)
	if err := decoder.Decode(&dataRow); err != nil {
		return Result[DataRow[K, V]]{Err: err}
	}

	return Result[DataRow[K, V]]{Value: dataRow}
}

func (dt *DataTable[K, V]) SaveIndex() Result[any] {
	err := dt.IndexTable.Save(dt.IndexFile)
	if err != nil {
		return Result[any]{Err: err}
	}
	return Result[any]{Value: nil}
}

func (dt *DataTable[K, V]) LoadIndex(indexFilePath string) Result[any] {
	indexFile, err := os.Open(indexFilePath)
	if err != nil {
		return Result[any]{Err: err}
	}
	defer indexFile.Close()

	err = dt.IndexTable.Load(indexFile)
	if err != nil {
		return Result[any]{Err: err}
	}
	return Result[any]{Value: nil}
}

func (dt *DataTable[K, V]) Compact() Result[any] {
	tempDataFilePath := dt.DataFile.Name() + ".tmp"
	tempDataFile, err := os.Create(tempDataFilePath)
	if err != nil {
		return Result[any]{Err: err}
	}

	newOffsets := make(map[K]int)
	for _, item := range dt.IndexTable.GetAll() {
		oldOffset := item.Value
		dataRowResult := dt.UnserializeData(oldOffset)
		if dataRowResult.Err != nil {
			return Result[any]{Err: dataRowResult.Err}
		}
		dataRow := dataRowResult.Value

		newOffsetResult := dt.serializeDataToFile(dataRow, tempDataFile)
		if newOffsetResult.Err != nil {
			return Result[any]{Err: newOffsetResult.Err}
		}
		newOffsets[item.Key] = newOffsetResult.Value
	}

	dt.DataFile.Close()
	tempDataFile.Close()

	err = os.Rename(tempDataFilePath, dt.DataFile.Name())
	if err != nil {
		return Result[any]{Err: err}
	}

	dt.DataFile, err = os.OpenFile(dt.DataFile.Name(), os.O_RDWR, 0666)
	if err != nil {
		return Result[any]{Err: err}
	}

	dt.IndexTable.Clear()
	for key, newOffset := range newOffsets {
		dt.IndexTable.Insert(key, newOffset)
	}

	saveIndexResult := dt.SaveIndex()
	if saveIndexResult.Err != nil {
		return Result[any]{Err: saveIndexResult.Err}
	}

	err = dt.freeFile.Truncate(0)
	if err != nil {
		return Result[any]{Err: err}
	}

	return Result[any]{Value: nil}
}

func (dt *DataTable[K, V]) serializeDataToFile(dataRow DataRow[K, V], file *os.File) Result[int] {
	offset, err := file.Seek(0, os.SEEK_END)
	if err != nil {
		return Result[int]{Err: err}
	}

	encoder := gob.NewEncoder(file)
	if err := encoder.Encode(dataRow); err != nil {
		return Result[int]{Err: err}
	}

	return Result[int]{Value: int(offset)}
}

func newRow[K comparable, V any](primaryKey K, data V) DataRow[K, V] {
	var p DataRow[K, V]
	p.PrimaryKey = primaryKey
	p.Data = data
	p.IsValid = true
	return p
}

// func projectRow[K comparable, V any](row DataRow[K, V], columns []string) (interface{}, error) {
// 	val := reflect.ValueOf(row.Data)

// 	if val.Kind() != reflect.Struct {
// 		return nil, fmt.Errorf("data is not a struct")
// 	}

// 	result := make(map[string]interface{})
// 	for _, column := range columns {
// 		field := val.FieldByName(column)
// 		if !field.IsValid() {
// 			return nil, fmt.Errorf("field %s not found in struct", column)
// 		}
// 		result[column] = field.Interface()
// 	}
// 	return result, nil
// }

func ProjectRow[K comparable, V any](row DataRow[K, V], resultType reflect.Type) (interface{}, error) {
	srcVal := reflect.ValueOf(row.Data)

	if resultType.Kind() != reflect.Struct {
		return nil, fmt.Errorf("resultType must be a struct type")
	}

	dstVal := reflect.New(resultType)

	for i := 0; i < dstVal.Elem().NumField(); i++ {
		dstField := dstVal.Elem().Type().Field(i)
		srcField := srcVal.FieldByName(dstField.Name)

		if !srcField.IsValid() {
			return nil, fmt.Errorf("field %s not found in source struct", dstField.Name)
		}

		if !srcField.Type().AssignableTo(dstField.Type) {
			return nil, fmt.Errorf("field %s types are not assignable", dstField.Name)
		}

		dstVal.Elem().Field(i).Set(srcField)
	}

	return dstVal.Interface(), nil
}

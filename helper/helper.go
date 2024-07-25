package helper

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"reflect"
)

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}

func RealSizeOf(v interface{}) int {
	b := new(bytes.Buffer)
	if err := gob.NewEncoder(b).Encode(v); err != nil {
		return 0
	}
	return b.Len()
}

func GetFieldNames[V any]() ([]string, error) {
	var v V
	val := reflect.ValueOf(v)

	if val.Kind() != reflect.Struct {
		return nil, fmt.Errorf("provided Data type is not a struct")
	}

	fieldNames := make([]string, val.NumField())
	for i := 0; i < val.NumField(); i++ {
		fieldNames[i] = val.Type().Field(i).Name
	}

	return fieldNames, nil
}

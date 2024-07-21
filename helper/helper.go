package helper

import (
	"bytes"
	"encoding/gob"
	"os"
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

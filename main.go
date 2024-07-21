package main

import (
	"ZeroStore/engine"
	"ZeroStore/test"
	"fmt"
)

func storeTest() {
	fmt.Println("running test on ZeroStore")

	dt, _ := engine.NewDataTable[int, test.Row](compare, "test/test", 4, true)
	for i := 1; i < test.NumberOfRows; i++ {
		a := test.GenerateRow(1024)
		dt.Insert(i, a)
	}
	dt.SaveIndex()

	test.CalculateEfficiencyPercentage("test/test_data.bin", test.NumberOfRows, 1024)
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

func main() {
	var dt *engine.DataTable[int, string]
	var err error

	if dt, err = engine.NewDataTable[int, string](compare, "testing", 4, true); err != nil {
		panic(err)
	}

	// for i := 1; i < 101; i++ {
	// 	s := fmt.Sprintf("data:%d", i)
	// 	dt.Insert(i, s)
	// }
	dt.Insert(1, "aaaaaa")
	dt.Insert(4, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	dt.Insert(3, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
	dt.Insert(2, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")

	for k, v := range dt.IndexTable.GetAll() {
		fmt.Println(k, v)
	}

	dt.Delete(1)
	dt.Delete(2)
	dt.Delete(3)
	dt.Delete(4)

	dt.Insert(1, "hello")

	for k, v := range dt.IndexTable.GetAll() {
		fmt.Println(k, v)
	}

	dt.Compact()

	for k, v := range dt.IndexTable.GetAll() {
		fmt.Println(k, v)
	}
}
func mulTwo(i int) int {
	return i * 2
}

// TODO make wrapper functions for SQL like where select etc
// TODO batch processing optimising
// TODO background threads for compaction and serialisation
// TODO multi-table joins

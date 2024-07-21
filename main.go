package main

import (
	"ZeroStore/engine"
	"ZeroStore/test"
	"fmt"
)

func storeTest() {
	fmt.Println("running test on ZeroStore\n")

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
	dt.Insert(1, "data 1")
	dt.Insert(2, "data 2")
	dt.Insert(3, "data 3")
	dt.Insert(4, "data 4")

	for k, v := range dt.IndexTable.GetAll() {
		fmt.Println(k, v)
	}

	dt.Delete(3)
	dt.Compact()

	for k, v := range dt.IndexTable.GetAll() {
		fmt.Println(k, v)
	}
}
func mulTwo(i int) int {
	return i * 2
}

// TODO insertion based on FreeList
// TODO make wrapper functions for SQL like where select etc
// TODO batch processing optimising
// TODO background threads for compaction and serialisation
// TODO multi-table joins

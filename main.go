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
	var dt *engine.DataTable[int, int]
	var err error

	if dt, err = engine.NewDataTable[int, int](compare, "testing", 4, true); err != nil {
		panic(err)
	}

	for i := 1; i < 10; i++ {
		// s := fmt.Sprintf("data:%d", i)
		dt.Insert(i, i)
	}

	for _, k := range dt.Select(dt.Where(gtThree)) {
		fmt.Println(k)
	}
}
func mulTwo(i int) int {
	return i * 2
}

func gtThree(d engine.DataRow[int, int]) bool {
	return d.Data > 3
}

// TODO make wrapper functions for SQL like where select etc
// TODO batch processing optimising
// TODO background threads for compaction and serialisation
// TODO multi-table joins

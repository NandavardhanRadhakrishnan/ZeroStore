package main

import (
	"ZeroStore/engine"
	"ZeroStore/test"
	"fmt"
)

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

	// for i := 1; i < 101; i++ {
	// 	s := fmt.Sprintf("data:%d", i)
	// 	dt.Insert(i, s)
	// }
	dt.Insert(1, 1)
	dt.Insert(2, 2)
	dt.Insert(3, 3)
	dt.Insert(4, 4)

	for i := 1; i < 5; i++ {
		o, _ := dt.IndexTable.Search(i)
		fmt.Println(dt.UnserializeData(o))
	}

	for i := 1; i < 5; i++ {
		dt.UpdateWithFunc(i, mulTwo)
	}
	fmt.Println("update")
	for i := 1; i < 5; i++ {
		o, _ := dt.IndexTable.Search(i)
		fmt.Println(dt.UnserializeData(o))
	}

}

func mulTwo(i int) int {
	return i * 2
}

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

// func main() {
// 	storeTest()
// }

// TODO batch processing optimising
// TODO background threads for compaction and serialisation
// TODO implement data compaction based on freelist
// TODO make wrapper functions for SQL like where select etc
// TODO multi-table joins

package main

import (
	"ZeroStore/storageEngine"
	"ZeroStore/test"
	"fmt"
)

func storeTest() {
	fmt.Println("running test on ZeroStore")

	dt, _ := storageEngine.NewDataTable[int, test.Row](compare, "test/test", 4, true)
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

type emp struct {
	Id   int
	Name string
}

func main() {
	var dt *storageEngine.DataTable[int, emp]
	var err error

	if dt, err = storageEngine.NewDataTable[int, emp](compare, "testing", 4, true); err != nil {
		panic(err)
	}

	for i := 1; i < 10; i++ {
		// s := fmt.Sprintf("data:%d", i)
		dt.Insert(i, emp{Id: i, Name: string(i)})
	}

	cols := []string{"Name"}
	var keys []int
	var dataRows []interface{}

	if keys, err = dt.Where(all); err != nil {
		panic(err)
	}
	if dataRows, err = dt.Select(keys, cols); err != nil {
		panic(err)
	}

	for _, k := range dataRows {
		fmt.Println(k)
	}
	// fmt.Println(dt.Columns)
}

func mulTwo(i int) int {
	return i * 2
}

func gtThree(d storageEngine.DataRow[int, emp]) bool {
	return d.Data.Id > 3
}

func all(d storageEngine.DataRow[int, emp]) bool {
	return true
}

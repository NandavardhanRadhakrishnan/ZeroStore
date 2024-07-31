package main

import (
	"ZeroStore/helper"
	"ZeroStore/queryEngine"
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
	Name int
}

func main() {
	// var dt *storageEngine.DataTable[int, emp]
	// var err error

	// if dt, err = storageEngine.NewDataTable[int, emp](compare, "testing", 4, true); err != nil {
	// 	panic(err)
	// }

	// for i := 1; i < 10; i++ {
	// 	// s := fmt.Sprintf("data:%d", i)
	// 	dt.Insert(i, emp{Id: i, Name: i})
	// }

	// qb := queryEngine.NewQueryBuilder(dt)
	// data, err := qb.Select([]string{"Name", "Id"}).Where(gtThree).Execute()
	// if err != nil {
	// 	panic(err)
	// }

	// for _, d := range data {
	// 	fmt.Println(d)
	// }

	var ut *storageEngine.DataTable[int, helper.User]
	var pt *storageEngine.DataTable[int, helper.Post]
	var err error

	if ut, err = storageEngine.NewDataTable[int, helper.User](compare, "users", 4, false); err != nil {
		panic(err)
	}
	if pt, err = storageEngine.NewDataTable[int, helper.Post](compare, "posts", 4, false); err != nil {
		panic(err)
	}

	// u, p := helper.MockData()
	// for _, user := range u {
	// 	ut.Insert(user.ID, user)
	// }
	// for _, post := range p {
	// 	pt.Insert(post.UserID, post)
	// }
	// ut.SaveIndex()
	// pt.SaveIndex()

	ut.LoadIndex(ut.IndexFile.Name())
	pt.LoadIndex(pt.IndexFile.Name())
	uqb := queryEngine.NewQueryBuilder(ut)
	pqb := queryEngine.NewQueryBuilder(pt)
	uqb.Where(firstFiveUser)
	uList := queryEngine.Execute[int, helper.User, []int](uqb)
	pqb.GetFromKeys(uList.Value).UpdateWithFunc(changeName)
	queryEngine.Execute[int, helper.Post, queryEngine.Result[helper.Post]](pqb)
	pqb.GetFromKeys([]int{1, 2, 3, 4, 5, 6, 7, 8, 9})
	pList := queryEngine.Execute[int, helper.Post, []helper.Post](pqb)
	fmt.Println(pList)

	// var out foo
	// dr := storageEngine.DataRow[int, helper.User]{PrimaryKey: 1, Data: helper.User{ID: 1, Name: "abc", Username: "alsoabc", Email: "abc@def.com"}}
	// a, _ := storageEngine.ProjectRow(dr, &out)
	// fmt.Println(a)
}

type foo struct {
	ID    int
	Title string
}

func changeName(dr helper.Post) helper.Post {
	dr.Title = "changed"
	return dr
}

func gtThree(d storageEngine.DataRow[int, emp]) bool {
	return d.Data.Id > 3
}

func firstFiveUser(r storageEngine.DataRow[int, helper.User]) bool {
	return r.Data.ID < 6
}

func allPost(storageEngine.DataRow[int, helper.Post]) bool {
	return true
}

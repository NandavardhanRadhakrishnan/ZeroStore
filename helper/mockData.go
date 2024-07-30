package helper

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
)

type User struct {
	ID       int    `json:"id"`
	Name     string `json:"name"`
	Username string `json:"username"`
	Email    string `json:"email"`
}

type Post struct {
	UserID int    `json:"userId"`
	ID     int    `json:"id"`
	Title  string `json:"title"`
	Body   string `json:"body"`
}

func fetchAndParseJSON(url string, target interface{}) error {
	resp, err := http.Get(url)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return err
	}

	err = json.Unmarshal(body, target)
	if err != nil {
		return err
	}

	return nil
}

func MockData() ([]User, []Post) {
	usersURL := "https://jsonplaceholder.typicode.com/users"
	postsURL := "https://jsonplaceholder.typicode.com/posts"

	var users []User
	if err := fetchAndParseJSON(usersURL, &users); err != nil {
		fmt.Println("Error fetching users:", err)
	}

	var posts []Post
	if err := fetchAndParseJSON(postsURL, &posts); err != nil {
		fmt.Println("Error fetching posts:", err)
	}
	return users, posts
}

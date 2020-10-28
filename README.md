# MyOrm

Simple GO ORM for improved developer experience for interacting with a MySQL database.

## Installation

1) Go Module

```sh
$ go get -u github.com/daryanka/myorm/v2
```

2) Import

```go
import "github.com/daryanka/myorm/v2"
```

## Basic Example

```go
package main

import (
	"database/sql"
	"github.com/daryanka/myorm/v2"
	_ "github.com/go-sql-driver/mysql"
	"time"
)

type Post struct {
	ID        int64     `db:"id"`
	Title     string    `db:"title"`
	Content   string    `db:"content"`
	CreatedAt time.Time `db:"created_at"`
}

func main() {
    db, err := sql.Open("mysql", "root:@tcp(127.0.0.1:3306)/test_db?parseTime=true")
	
    if err != nil {
	    panic(err.Error())
    }

	orm := myorm.DBInit(myorm.DBConnection{
		DB:             db,
		DBDriver:       "mysql",
		ConnectionName: "default",
	})
    
    var postsData []Post
	
	_ = orm.Table("posts").Get(&postsData)
}
```
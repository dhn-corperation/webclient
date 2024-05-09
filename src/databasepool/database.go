package databasepool

import (
	_ "github.com/go-sql-driver/mysql"
	"database/sql"
	//"log"
	"config"
	"fmt"
)

var DB *sql.DB

func InitDatabase() {
	db, err := sql.Open(config.Conf.DB, config.Conf.DBURL)
	if err != nil {
		fmt.Println(err)
	}

	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)

	DB = db

}
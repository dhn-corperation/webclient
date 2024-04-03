package databasepool

import (
	"database/sql"

	_ "github.com/go-sql-driver/mysql"

	//"log"
	"fmt"
	"webclient/src/config"
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

package databasepool

import (
	"database/sql"

	//_ "github.com/go-sql-driver/mysql"
	_ "github.com/lib/pq"

	//"log"
	"fmt"
	"webclient/src/config"
)

var DB *sql.DB

func InitDatabase() {
	psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", config.Conf.HOST, config.Conf.PORT, config.Conf.DBID, config.Conf.DBPW, config.Conf.DBNAME)
	//psqlInfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", "210.114.225.58", "5432", "postgres", "dhn7985!", "test")
	db, err := sql.Open(config.Conf.DB, psqlInfo)
	if err != nil {
		fmt.Println(err)
	}

	db.SetMaxIdleConns(50)
	db.SetMaxOpenConns(50)

	DB = db

}

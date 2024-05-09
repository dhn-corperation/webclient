package databasepool

import (
	"database/sql"

	//_ "github.com/alexbrainman/odbc"
	//_ "github.com/go-sql-driver/mysql"
	_ "github.com/godror/godror"

	//"log"
	"config"
	//"fmt"
	//iconv "github.com/djimenez/iconv-go"
)

var DB *sql.DB

func InitDatabase() {
	//fmt.Println(config.Conf.DBINFOR)
	//config.Stdlog.Println("DB INFOR : " + config.Conf.DBINFOR)
	db, err := sql.Open("godror", `user="`+config.Conf.USER+`" password="`+config.Conf.PASSWORD+`" connectString="`+config.Conf.CONNECTSTRING+`"`)
	if err != nil {
		config.Stdlog.Println("DB Open Error : " + err.Error())
		panic(err)
	}
	config.Stdlog.Println("DB Open OK !!")
	//fmt.Println(db)
	err = db.Ping()
	if err != nil {
		config.Stdlog.Println(config.Conf.CONNECTSTRING, "DB Ping Error : "+err.Error())
		//fmt.Println(config.Conf.DBINFOR, "DB Ping Error : " + err.Error())
		panic(err)
	}
	config.Stdlog.Println("DB Ping !!")
	db.SetMaxIdleConns(10)
	db.SetMaxOpenConns(10)

	DB = db

}

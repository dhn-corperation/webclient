package config

import (
	"fmt"
	"log"
	"os"
	"time"

	ini "github.com/BurntSushi/toml"
	"github.com/go-resty/resty"
	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
	"gopkg.in/tomb.v2"
)

type Config struct {
	USER          string
	PASSWORD      string
	CONNECTSTRING string
	USERID        string
	SERVER        string
	SC_TRAN       string
	SC_TRAN_IMD   string
	SC_LOG        string
	TRAN_FROM     string
	TRAN_TO       string
}

type Proc struct {
	Tomb tomb.Tomb
}

var Conf Config
var Stdlog *log.Logger
var IsRunning bool = true
var Client *resty.Client

func InitConfig() {
	homedir, _ := os.UserHomeDir();
	path := homedir + "/DHNClient/logs/DHNClient"
	//path := "./log/DHNClient"
	loc, _ := time.LoadLocation("Asia/Seoul")
	writer, err := rotatelogs.New(
		fmt.Sprintf("%s-%s.log", path, "%Y-%m-%d"),
		rotatelogs.WithLocation(loc),
		rotatelogs.WithMaxAge(-1),
		rotatelogs.WithRotationCount(-1),
	)

	if err != nil {
		log.Fatalf("Failed to Initialize Log File %s", err)
	}

	log.SetOutput(writer)
	stdlog := log.New(os.Stdout, "INFO -> ", log.Ldate|log.Ltime)
	stdlog.SetOutput(writer)
	Stdlog = stdlog

	Conf = readConfig()

	Client = resty.New()
}

func readConfig() Config {
	homedir, _ := os.UserHomeDir();
	var configfile = homedir + "/DHNClient/config.ini"
	//var configfile = "./config.ini"
	_, err := os.Stat(configfile)
	if err != nil {
		fmt.Println("Config file is missing : ", configfile)
	}

	var result Config
	_, err1 := ini.DecodeFile(configfile, &result)

	if err1 != nil {
		fmt.Println("Config file read error : ", err1)
	}

	return result
}

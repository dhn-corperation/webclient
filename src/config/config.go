package config

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	ini "github.com/BurntSushi/toml"
	"github.com/go-resty/resty/v2"
	rotatelogs "github.com/lestrrat/go-file-rotatelogs"
	"gopkg.in/tomb.v2"
)

type Config struct {
	DB          string
	DBURL       string
	USERID      string
	SERVER      string
	REQTABLE    string
	RESULTTABLE string
}

type Proc struct {
	Tomb tomb.Tomb
}

var Conf Config
var Stdlog *log.Logger
var IsRunning bool = true
var Client *resty.Client

func InitConfig() {
	realpath, _ := os.Executable()
	dir := filepath.Dir(realpath)
	logDir := filepath.Join(dir, "logs")
	err := createDir(logDir)
	if err != nil {
		log.Fatalf("Failed to ensure log directory: %s", err)
	}
	path := filepath.Join(logDir, "DHNClient")
	//path := "/root/DHNClient/log/DHNClient"
	//path := "./log/DHNClient"
	loc, _ := time.LoadLocation("Asia/Seoul")
	writer, err := rotatelogs.New(
		fmt.Sprintf("%s-%s.log", path, "%Y-%m-%d"),
		rotatelogs.WithLocation(loc),
		rotatelogs.WithMaxAge(-1),
		rotatelogs.WithRotationCount(7),
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
	/*
		var configfile = "/root/DHNClient/config.ini"
		_, err := os.Stat(configfile)
		if err != nil {
			fmt.Println("Config file is missing : ", configfile)
		}
	*/
	realpath, _ := os.Executable()
	dir := filepath.Dir(realpath)
	var configfile = filepath.Join(dir, "config.ini")
	//var configfile = "/root/DHNServer/config.ini"
	//var configfile = "./config.ini"
	_, err := os.Stat(configfile)
	if err != nil {

		err := createConfig(configfile)
		if err != nil {
			Stdlog.Println("Config file create fail")
		}
		Stdlog.Println("config.ini 생성완료 작성을 해주세요.")

		fmt.Println("Config file is missing : ", configfile)
	}

	var result Config
	_, err1 := ini.DecodeFile(configfile, &result)

	if err1 != nil {
		fmt.Println("Config file read error : ", err1)
	}

	return result
}

func InitGenieConfig() {
	realpath, _ := os.Executable()
	dir := filepath.Dir(realpath)
	logDir := filepath.Join(dir, "logs")
	err := createDir(logDir)
	if err != nil {
		log.Fatalf("Failed to ensure log directory: %s", err)
	}
	path := filepath.Join(logDir, "GClient")
	//path := "/root/GenieClient/log/GenieClient"
	//path := "./log/DHNClient"
	loc, _ := time.LoadLocation("Asia/Seoul")
	writer, err := rotatelogs.New(
		fmt.Sprintf("%s-%s.log", path, "%Y-%m-%d"),
		rotatelogs.WithLocation(loc),
		rotatelogs.WithMaxAge(-1),
		rotatelogs.WithRotationCount(7),
	)

	if err != nil {
		log.Fatalf("Failed to Initialize Log File %s", err)
	}

	log.SetOutput(writer)
	stdlog := log.New(os.Stdout, "INFO -> ", log.Ldate|log.Ltime)
	stdlog.SetOutput(writer)
	Stdlog = stdlog

	Conf = readGenieConfig()

	Client = resty.New()
}

func readGenieConfig() Config {
	/*
		var configfile = "/root/GenieClient/config.ini"
		_, err := os.Stat(configfile)
		if err != nil {
			fmt.Println("Config file is missing : ", configfile)
		}
	*/
	realpath, _ := os.Executable()
	dir := filepath.Dir(realpath)
	var configfile = filepath.Join(dir, "config.ini")
	//var configfile = "/root/DHNServer/config.ini"
	//var configfile = "./config.ini"
	_, err := os.Stat(configfile)
	if err != nil {

		err := createConfig(configfile)
		if err != nil {
			Stdlog.Println("Config file create fail")
		}
		Stdlog.Println("config.ini 생성완료 작성을 해주세요.")

		fmt.Println("Config file is missing : ", configfile)
	}

	var result Config
	_, err1 := ini.DecodeFile(configfile, &result)

	if err1 != nil {
		fmt.Println("Config file read error : ", err1)
	}

	return result
}

func createDir(dirName string) error {
	err := os.MkdirAll(dirName, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}
	return nil
}

func createConfig(dirName string) error {
	fo, err := os.Create(dirName)
	if err != nil {
		return fmt.Errorf("Config file create fail: %w", err)
	}
	configData := []string{
		`# DB 관련`,
		`DB = "DB종류"`,
		`DBURL = "사용자:패스워드@tcp(000.000.000.000:포트번호)/데이터베이스"`,
		``,
		`# DHN Server`,
		`UESRID = "ID명"`,
		`SERVER = "서버 주소"`,
		``,
		`# 카카오 발송용 Table`,
		`REQTABLE = "카카오 발송 테이블명"`,
		``,
		`# 결과 테이블`,
		`RESULTTABLE = "결과 테이블명"`,
		``,
		`#추가할 설정 내용 필요에 따라 작성`,
	}

	for _, line := range configData {
		fmt.Fprintln(fo, line)
	}

	return nil
}

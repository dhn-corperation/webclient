package main

import (
	"fmt"
	"os"
	"os/signal"
	"syscall"

	_ "github.com/go-sql-driver/mysql"

	"webclient/src/config"
	"webclient/src/databasepool"
	"webclient/src/resultreq"
	"webclient/src/sendrequest"

	//"time"

	"github.com/takama/daemon"
)

const (
	name        = "DHNClient_m"
	description = "마트톡 카카오 발송 프로그램"

	// name        = "DHNClient_g"
	// description = "올지니 카카오 발송 프로그램"

	// name        = "DHNClient_o"
	// description = "오투오 카카오 발송 프로그램"

	// name        = "DHNClient_p"
	// description = "스피드톡 카카오 발송 프로그램"

	// name        = "DHNClient_s"
	// description = "싸다고 카카오 발송 프로그램"
)

var dependencies = []string{name+".service"}

var resultTable string

type Service struct {
	daemon.Daemon
}

func (service *Service) Manage() (string, error) {

	usage := "Usage: "+name+" install | remove | start | stop | status"

	if len(os.Args) > 1 {
		command := os.Args[1]
		switch command {
		case "install":
			return service.Install()
		case "remove":
			return service.Remove()
		case "start":
			return service.Start()
		case "stop":
			return service.Stop()
		case "status":
			return service.Status()
		default:
			return usage, nil
		}
	}

	resultProc()
	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt, os.Kill, syscall.SIGTERM)

	for {
		select {
		case killSignal := <-interrupt:
			config.Stdlog.Println("Got signal:", killSignal)
			config.Stdlog.Println("Stoping DB Conntion : ", databasepool.DB.Stats())
			defer databasepool.DB.Close()
			if killSignal == os.Interrupt {
				return "Daemon was interrupted by system signal", nil
			}
			return "Daemon was killed", nil
		}
	}
}

func main() {

	config.InitConfig()

	databasepool.InitDatabase()

	var rLimit syscall.Rlimit

	rLimit.Max = 50000
	rLimit.Cur = 50000

	err := syscall.Setrlimit(syscall.RLIMIT_NOFILE, &rLimit)

	if err != nil {
		config.Stdlog.Println("Error Setting Rlimit ", err)
	}

	err = syscall.Getrlimit(syscall.RLIMIT_NOFILE, &rLimit)

	if err != nil {
		config.Stdlog.Println("Error Getting Rlimit ", err)
	}

	config.Stdlog.Println("Rlimit Final", rLimit)

	srv, err := daemon.New(name, description, daemon.SystemDaemon, dependencies...)
	if err != nil {
		config.Stdlog.Println("Error: ", err)
		os.Exit(1)
	}
	service := &Service{srv}
	status, err := service.Manage()
	if err != nil {
		config.Stdlog.Println(status, "\nError: ", err)
		os.Exit(1)
	}
	fmt.Println(status)

}

func resultProc() {
	config.Stdlog.Println(name, " 시작")

	go sendrequest.Process()

	go resultreq.ResultReqProc()

}

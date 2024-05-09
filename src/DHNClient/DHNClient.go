package main

import (
	"fmt"
	"os"
	"os/signal"
	"resultreq"
	"syscall"

	"config"
	"databasepool"
	"sendrequest"
	"sendrequestimd"

	//"time"

	"github.com/takama/daemon"
)

const (
	name        = "DHNClient"
	description = "대형네트웍스 발송 Client"
)

var dependencies = []string{"DHNClient.service"}

var resultTable string

type Service struct {
	daemon.Daemon
}

func (service *Service) Manage() (string, error) {

	usage := "Usage: DHNClient install | remove | start | stop | status"

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
	//time.Sleep(time.Millisecond * time.Duration(1000))
	
	databasepool.InitDatabase()
	//time.Sleep(time.Millisecond * time.Duration(1000))

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
	config.Stdlog.Println("DHN Client start !!")

	go sendrequest.Process()

	go sendrequestimd.ImdProcess()
	//return
	go resultreq.ResultReqProc()

	//go kfriendreq.FriendInfoReqProc()
	/*
		r := gin.New()
		r.Use(gin.Recovery())
		//r := gin.Default()

		r.GET("/", func(c *gin.Context) {
			//time.Sleep(30 * time.Second)
			c.String(200, "Server Alive ")
		})

		r.GET("/status", func(c *gin.Context) {
			sendrequest.DisplayStatus = true
			c.String(200, "Server Alive ")
		})

		r.POST("/ft/image", kaocenter.FT_Upload)

		r.POST("/ft/wide/image", kaocenter.FT_Wide_Upload)

		r.POST("/at/image", kaocenter.AT_Image)

		r.POST("/mms/image", kaocenter.MMS_Image)

		r.Run(":8484")
	*/
}

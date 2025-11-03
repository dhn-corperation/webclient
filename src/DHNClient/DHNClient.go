package main

import (
	"fmt"
	"os"
	"log"
	"context"
	"syscall"
	"reflect"
	"os/signal"

	"webclient/src/config"
	"webclient/src/databasepool"
	"webclient/src/resultreq"
	"webclient/src/sendrequest"

	"github.com/takama/daemon"
	"github.com/fasthttp/router"
	"github.com/valyala/fasthttp"
	_ "github.com/go-sql-driver/mysql"
)

const (
	name        = "DHNClient_m"
	description = "마트톡 카카오 발송 프로그램"
	port  		= ":3310"

	// name        = "DHNClient_g"
	// description = "올지니 카카오 발송 프로그램"
	// port  		= ":3320"

	// name        = "DHNClient_o"
	// description = "오투오 카카오 발송 프로그램"
	// port  		= ":3330"

	// name        = "DHNClient_p"
	// description = "스피드톡 카카오 발송 프로그램"
	// port  		= ":3340"

	// name        = "DHNClient_s"
	// description = "싸다고 카카오 발송 프로그램"
	// port  		= ":3350"
)

var dependencies = []string{name+".service"}

var resultTable string

var cc context.CancelFunc

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
	var conf = config.Conf

	val := reflect.ValueOf(conf)
	typ := reflect.TypeOf(conf)

	config.Stdlog.Println(name, " 시작")
	config.Stdlog.Println("------------------------------------------------------------------------------")

	for i := 0; i < val.NumField(); i++ {
		field := typ.Field(i)
		value := val.Field(i)
		config.Stdlog.Println(fmt.Sprintf("%s: %v", field.Name, value.Interface()))
	}

	config.Stdlog.Println("------------------------------------------------------------------------------")

	ctx, cancel := context.WithCancel(context.Background())
	cc = cancel

	go sendrequest.Process(ctx)

	go sendrequest.ProcessBroadcast(ctx)

	go resultreq.ResultReqProc(ctx)

	r := router.New()

	r.GET("/", func(c *fasthttp.RequestCtx) {
		c.SetStatusCode(fasthttp.StatusOK)
		c.SetBodyString("정상 수신 완료 - " + name + "\n")
	})

	r.GET("/allstop", func(c *fasthttp.RequestCtx){
		uid := string(c.QueryArgs().Peek("uid"))
		if uid == "dhn" {
			config.Stdlog.Println("전체 종료 시작")
			cc()
			cc = nil
			c.SetContentType("application/json")
			c.SetStatusCode(fasthttp.StatusOK)
			c.SetBody([]byte("전체 종료 수신 완료\n"))
		} else {
			c.SetContentType("application/json")
			c.SetStatusCode(fasthttp.StatusOK)
			c.SetBody([]byte("전체 종료 수신 실패\n"))
		}
	})

	if err := fasthttp.ListenAndServe(port, r.Handler); err != nil {
		config.Stdlog.Println("fasthttp 실행 실패")
		log.Fatal("fasthttp 실행 실패")
	}

}

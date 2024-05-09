package resultreq

import (
	"encoding/json"
	//"fmt"

	"config"

	"databasepool"
	"resulttable"

	s "strings"
	"sync"
	"time"
)

var Interval int32 = 10000
var PreMonth = ""

var RES_CODE = map[string]string{
		"00":"00",
		"01":"00",
		"03":"03",
		"04":"04",
		"05":"05",
		"06":"06",
		"07":"07",
		"08":"08",
		"09":"09",
		"10":"10",
		"11":"11",
		"13":"53",
		"14":"53",
		"15":"16",
		"16":"39",
		"20":"11",
		"21":"43",
		"22":"40",
		"23":"44",
		"28":"11",
		"29":"46",
		"36":"36",
		"37":"37",
		"38":"47",
		"50":"49",
		"51":"52",
		"52":"60",
		"53":"31",
		"54":"61",
		"59":"63",
		"60":"64",
		"61":"65",
		"69":"69",
		"73":"11",
		"74":"11",
		"75":"35",
		"76":"37",
		"77":"36",
		"78":"38",
		"79":"68",
		"83":"21",
		"90":"66",
		"91":"67",
		"92":"55",
		"93":"19",
		"94":"29",
		"95":"11",
		"96":"11",
		"97":"11",
		"98":"11",
		"99":"01",
}

func ResultReqProc() {
	var wg sync.WaitGroup

	for {

		wg.Add(1)

		go getResultProcess(&wg)

		wg.Wait()
		time.Sleep(time.Millisecond * time.Duration(Interval))

	}
	config.Stdlog.Println("Result Req Process End !!")
}

func getResultProcess(wg *sync.WaitGroup) {

	defer wg.Done()

	defer func() {
		if err := recover(); err != nil {
			config.Stdlog.Println("Send Process Error -", err)
		}
	}()

	var conf = config.Conf

	var errlog = config.Stdlog
	var procCnt int = 0

	resp, err := config.Client.R().
		SetHeaders(map[string]string{"userid": conf.USERID}).
		Post(conf.SERVER + "result")

	if err != nil {
		errlog.Println("Result process error : ", err)
	} else {

		if resp.StatusCode() == 200 {
			str := resp.Body()
			//fmt.Println(resp)
			var result []resulttable.ResultTable
			jerr := json.Unmarshal([]byte(str), &result)
			if jerr != nil {
				errlog.Println("Result Json process error : ", jerr)
			}
			//fmt.Println("RESULT",result, jerr)
			if len(result) >= 1 {
				Interval = 1
				errlog.Println("Result receive 완료 - 후처리 시작")

				for i, _ := range result {
					insertResult(result[i])
					procCnt++
				}
	
				if procCnt > 0 {
					errlog.Println("Result receive complete - Message count : ", procCnt)
				}

			} else {
				if Interval >= 10000 {
					Interval = 10000
				} else {
					Interval = Interval + 1000
				}
			}

		} else {
			errlog.Println("Result server error ", resp)
		}

	}
}

func insertResult(result resulttable.ResultTable) {
	tx, err := databasepool.DB.Begin()
	if err != nil {
		return
	}

	defer func() {
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	var resQuery string
	var resCode string
	var conf = config.Conf
	//config.Stdlog.Println("Result Process : ", result)

	if s.EqualFold(result.Code, "0000") || s.EqualFold(result.Code, "MS03") || s.EqualFold(result.Code, "K000") {
		resCode = "06"
	} else {
		resCode = result.Code[2:4]
		config.Stdlog.Println("Failed to send message - TR_NUM : " , result.Msgid, " / Result Code : ", resCode, " / result message : ", result.Message)
	}

	if s.EqualFold(result.P_com, "D") {
		var rcode =  RES_CODE[resCode]
		if len(rcode) <=0 {
			rcode = "11"
		}
		resQuery = "update " + conf.SC_TRAN + " set tr_rsltdate = sysdate, tr_modified = sysdate, tr_rsltstat = '" + rcode + "', tr_net = '" + result.Remark1 + "', tr_sendstat='2' where tr_num = '" + result.Msgid + "'"
	
		_, err = tx.Query(resQuery)
	
		_, err = tx.Query("insert into " + conf.SC_LOG + " select * from " + conf.SC_TRAN + " where tr_num = '" + result.Msgid + "'")
		if err != nil {
			config.Stdlog.Println("insert into " + conf.SC_LOG + " select * from " + conf.SC_TRAN + " where tr_num = '" + result.Msgid + "'")
		}
		_, err = tx.Query("delete from " + conf.SC_TRAN + " where tr_num = '" + result.Msgid + "'")
	} else if s.EqualFold(result.P_com, "I") {
	
		var rcode =  RES_CODE[resCode]
		if len(rcode) <=0 {
			rcode = "11"
		}
		resQuery = "update " + conf.SC_TRAN_IMD + " set tr_rsltdate = sysdate, tr_modified = sysdate, tr_rsltstat = '" + rcode + "', tr_net = '" + result.Remark1 + "', tr_sendstat='2' where tr_num = '" + result.Msgid + "'"
		
		_, err = tx.Query(resQuery)
		
		_, err = tx.Query("insert into " + conf.SC_LOG + " select * from " + conf.SC_TRAN_IMD + " where tr_num = '" + result.Msgid + "'")
		if err != nil {
			config.Stdlog.Println("insert into " + conf.SC_LOG + " select * from " + conf.SC_TRAN_IMD + " where tr_num = '" + result.Msgid + "'")
		}
		_, err = tx.Query("delete from " + conf.SC_TRAN_IMD + " where tr_num = '" + result.Msgid + "'")
	}
/*
	var rcode =  RES_CODE[resCode];
	if len(rcode) <=0 {
		rcode = "11"
	}
	resQuery = "update " + conf.SC_LOG + " set tr_rsltdate = sysdate, tr_modified = sysdate, tr_rsltstat = '" + rcode + "', tr_net = '" + result.Remark1 + "', tr_sendstat='2' where tr_num = '" + result.Msgid + "'"

	_, err = tx.Query(resQuery)

	if err != nil {
		config.Stdlog.Println("Result Table Insert error :"+err.Error(), " ( msgid : ", result.Msgid, ")")
		//config.Stdlog.Println(insQuery)
		//fmt.Println("Request Table - select 오류 : " + err.Error())
		return
	}
*/
	return
}

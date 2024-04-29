package sendrequest

import (
	//"bytes"
	"database/sql"
	"reflect"
	"webclient/src/common"
	"webclient/src/config"
	"webclient/src/databasepool"

	//"encoding/json"
	"fmt"
	//"io/ioutil"

	//"net/http"
	s "strings"

	//	"sync"
	"time"
)

var procCnt int

func Process() {

	for {
		if procCnt < 5 {

			var count int

			cnterr := databasepool.DB.QueryRow("SELECT COUNT(1) AS cnt FROM " + config.Conf.REQTABLE + " r WHERE group_no IS NULL AND (r.reserve_dt < TO_CHAR(NOW(), 'YYYYMMDDHH24MISS') OR r.reserve_dt = '00000000000000')").Scan(&count)

			if cnterr != nil {
				config.Stdlog.Println("Request Table - select 오류 : " + cnterr.Error())
			} else {

				if count > 0 {
					var startNow = time.Now()
					var group_no = fmt.Sprintf("%02d%02d%02d%09d", startNow.Hour(), startNow.Minute(), startNow.Second(), startNow.Nanosecond())

					updateRows, err := databasepool.DB.Exec("update " + config.Conf.REQTABLE + " r set group_no = '" + group_no + "' where msgid IN (SELECT msgid FROM dhn_request WHERE group_no IS NULL AND (reserve_dt < TO_CHAR(NOW(), 'YYYYMMDDHH24MISS') or reserve_dt = '00000000000000') LIMIT 1000)")
					if err != nil {
						config.Stdlog.Println("Request Table - Group No Update 오류 : " + err.Error())
					} else {
						rowcnt, err1 := updateRows.RowsAffected()
						if err1 != nil {
							rowcnt = 0
							config.Stdlog.Println("Request Table - RowsAffected 오류 : " + err.Error())
						}

						if rowcnt > 0 {
							go sendProcess(group_no)
						}
					}
				}
			}
		}
	}
	config.Stdlog.Println("Send Process End !!")
}

func sendProcess(group_no string) {

	procCnt++
	var db = databasepool.DB
	var conf = config.Conf
	var stdlog = config.Stdlog
	var errlog = config.Stdlog

	db2json := map[string]string{
		"msgid":         "msgid",
		"ad_flag":       "adflag",
		"button1":       "button1",
		"button2":       "button2",
		"button3":       "button3",
		"button4":       "button4",
		"button5":       "button5",
		"image_link":    "imagelink",
		"image_url":     "imageurl",
		"message_type":  "messagetype",
		"msg":           "msg",
		"msg_sms":       "msgsms",
		"only_sms":      "onlysms",
		"p_com":         "pcom",
		"p_invoice":     "pinvoice",
		"phn":           "phn",
		"profile":       "profile",
		"reg_dt":        "regdt",
		"remark1":       "remark1",
		"remark2":       "remark2",
		"remark3":       "remark3",
		"remark4":       "remark4",
		"remark5":       "remark5",
		"reserve_dt":    "reservedt",
		"s_code":        "scode",
		"sms_kind":      "smskind",
		"sms_lms_tit":   "smslmstit",
		"sms_sender":    "smssender",
		"tmpl_id":       "tmplid",
		"wide":          "wide",
		"supplement":    "supplement",
		"price":         "price",
		"currency_type": "currencytype",
		"title":         "title",
		"header":        "header",
		"carousel":      "carousel",
		"att_items":     "att_items",
		"att_coupon":    "att_coupon",
	}

	reqsql := "select * from " + conf.REQTABLE + " where group_no = '" + group_no + "'"

	reqrows, err := db.Query(reqsql)
	if err != nil {
		//errlog.Fatal(err)
		stdlog.Println("sendProcess 쿼리 에러 group_no : ", group_no)
		stdlog.Println("sendProcess 쿼리 에러 query : ", reqsql)
		stdlog.Println("sendProcess 쿼리 에러 : ", err)
		//stdlog.Println(conf.REQTABLE + " Table - Select 오류 : ( " + group_no + " ) : " + err.Error())
		return
	}

	columnTypes, err := reqrows.ColumnTypes()
	if err != nil {
		//errlog.Fatal(err)
		errlog.Println("sendProcess 컬럼 초기화 에러 group_no : ", group_no)
		errlog.Println("sendProcess 컬럼 초기화 에러 : ", err)
		//errlog.Println(conf.REQTABLE + " Table - ColumnType 조회 오류" + err.Error())
		return
	}
	count := len(columnTypes)
	initScanArgs := common.InitDatabaseColumn(columnTypes, count)

	finalRows := []interface{}{}

	var isContinue bool
	var procCount int
	procCount = 0
	var startNow = time.Now()
	var startTime = fmt.Sprintf("%02d:%02d:%02d", startNow.Hour(), startNow.Minute(), startNow.Second())

	stdlog.Printf(" ( %s ) 처리 시작 - %s ", startTime, group_no)

	for reqrows.Next() {
		scanArgs := initScanArgs

		err := reqrows.Scan(scanArgs...)
		if err != nil {
			//errlog.Fatal(err)
			errlog.Println("sendProcess 컬럼 스캔 에러 group_no : ", group_no)
			errlog.Println("sendProcess 컬럼 스캔 에러 : ", err)
			//errlog.Println(conf.REQTABLE + " Table - Scan 오류" + err.Error())
		}

		masterData := map[string]interface{}{}

		for i, v := range columnTypes {

			isContinue = false

			if z, ok := (scanArgs[i]).(*sql.NullBool); ok {
				masterData[db2json[s.ToLower(v.Name())]] = z.Bool
				isContinue = true
			}

			if z, ok := (scanArgs[i]).(*sql.NullString); ok {
				masterData[db2json[s.ToLower(v.Name())]] = z.String
				isContinue = true
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt64); ok {
				masterData[db2json[s.ToLower(v.Name())]] = z.Int64
				isContinue = true
			}

			if z, ok := (scanArgs[i]).(*sql.NullFloat64); ok {
				masterData[db2json[s.ToLower(v.Name())]] = z.Float64
				isContinue = true
			}

			if z, ok := (scanArgs[i]).(*sql.NullInt32); ok {
				masterData[db2json[s.ToLower(v.Name())]] = z.Int32
				isContinue = true
			}
			if !isContinue {
				masterData[db2json[s.ToLower(v.Name())]] = scanArgs[i]
			}

		}

		for key, value := range masterData {
			stdlog.Printf("Key: %s, Value: %v, Type: %s\n", key, value, reflect.TypeOf(value))
		}
		//masterData["price"] = fmt.Sprintf("%d", masterData["price"])
		finalRows = append(finalRows, masterData)
		procCount++
	}
	resp, err := config.Client.R().
		SetHeaders(map[string]string{"Content-Type": "application/json", "userid": conf.USERID}).
		SetBody(finalRows).
		Post(conf.SERVER + "testyyw")
		//Post(conf.SERVER + "req")

	if err != nil {
		errlog.Println("메시지 서버 호출 오류", err)
		databasepool.DB.Exec("update " + conf.REQTABLE + "set group_no = null where group_no = '" + group_no + "'")
	} else {

		if resp.StatusCode() == 200 {
			databasepool.DB.Exec("delete from " + conf.REQTABLE + " where group_no = '" + group_no + "'")

		} else {
			stdlog.Println("서버 처리 오류 !! ( ", resp, " )")
		}
	}
	stdlog.Printf(" ( %s ) 처리끝 : %d 건 처리 ( process cnt : %d ) - %s", startTime, procCount, procCnt, group_no)
	procCnt--
}

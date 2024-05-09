package resultreq

import (
	"database/sql"
	"encoding/json"
	"fmt"

	"config"

	"databasepool"
	"resulttable"

	s "strings"
	"sync"
	"time"
	iconv "github.com/djimenez/iconv-go"
)

var Interval int32 = 10000
var PreMonth = ""

func ResultReqProc() {
	var wg sync.WaitGroup
	
	var t = time.Now()
	PreMonth = fmt.Sprintf("%d%02d", t.Year(), t.Month())
	var query = " select * from " + config.Conf.RESULTTABLE + "_" + PreMonth +"  r where rownum = 1"
	_, err := databasepool.DB.Query(query)
	
	if err != nil {
		databasepool.DB.Query("create table " + config.Conf.RESULTTABLE + "_" + PreMonth +" as select * from dhn_request_result where 1 = 2")
		databasepool.DB.Query("create index " + config.Conf.RESULTTABLE + "_" + PreMonth +"_idx01 on " + config.Conf.RESULTTABLE + "_" + PreMonth +"(msgid)")
	}
	
	for {
		var t = time.Now()
		currMonth := fmt.Sprintf("%d%02d", t.Year(), t.Month())

		if PreMonth == currMonth {
			wg.Add(1)
			
			go getResultProcess(&wg)
	
			wg.Wait()
			time.Sleep(time.Millisecond * time.Duration(Interval))
		} else {
			var query = " select * from " + config.Conf.RESULTTABLE + "_" + currMonth +"  r where rownum = 1"
			_, err := databasepool.DB.Query(query)
			
			if err != nil {
				databasepool.DB.Query("create table " + config.Conf.RESULTTABLE + "_" + currMonth +" as select * from dhn_request_result where 1 = 2")
				databasepool.DB.Query("create index " + config.Conf.RESULTTABLE + "_" + currMonth +"_idx01 on " + config.Conf.RESULTTABLE + "_" + PreMonth +"(msgid)")
			}
			
			PreMonth = currMonth
		}
	}
	config.Stdlog.Println("Result Req Process End !!")
}

func getResultProcess(wg *sync.WaitGroup) {

	defer wg.Done()

	defer func() {
		if err := recover(); err != nil {
			config.Stdlog.Println("Send Process Error -" , err)
		}
	} ()
	
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
			} else {
				if Interval >= 10000 {
					Interval = 10000
				} else {
					Interval = Interval + 1000
				}
			}

			for i, _ := range result {
				insertResult(result[i]) 
				procCnt++
			}

			if procCnt > 0 {
				errlog.Println("Result receive complete - Message count : ", procCnt)
			}

		} else {
			errlog.Println("Result server error ", resp)
		}

	}
}


func insertResult(result resulttable.ResultTable) {


	insQuery := `INSERT INTO ` + config.Conf.RESULTTABLE + `_#{PreMonth}(
	  MSGID, AD_FLAG, BUTTON1, BUTTON2, 
	  BUTTON3, BUTTON4, BUTTON5, 
	  CODE, IMAGE_LINK, IMAGE_URL, 
	  KIND, MESSAGE, MESSAGE_TYPE, 
	  MSG, MSG_SMS, ONLY_SMS, P_COM, 
	  P_INVOICE, PHN, PROFILE, REG_DT, 
	  REMARK1, REMARK2, REMARK3, 
	  REMARK4, REMARK5, REMARK6, REMARK7, REMARK8, REMARK9, REMARK10, RES_DT, RESERVE_DT, 
	  RESULT, S_CODE, SMS_KIND, SMS_LMS_TIT, 
	  SMS_SENDER, SYNC, TMPL_ID, 
	  WIDE, SUPPLEMENT, PRICE, CURRENCY_TYPE, TITLE
	) values (`


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


	var remark6,remark7,remark8,remark9,remark10 sql.NullString
	var button1,button2,button3,button4,button5,msg,msg_sms,only_sms,remark1,remark2,remark3,remark4,remark5,sms_lms_tit,tmpl_id,supplement sql.NullString
	
	drQuery := `select 
	button1,
	button2,
	button3,
	button4,
	button5,
	msg,
	msg_sms,
	only_sms,
	remark1,
	remark2,
	remark3,
	remark4,
	remark5,
	remark6,
	remark7,
	remark8,
	remark9,
	remark10,
	sms_lms_tit,
	tmpl_id,
	supplement
	from DHN_REQUEST dr
where msgid = '` + result.Msgid + `'`

	rowErr := tx.QueryRow(drQuery).Scan(&button1,&button2,&button3,&button4,&button5,&msg,&msg_sms,&only_sms,&remark1,&remark2,&remark3,&remark4,&remark5,&remark6,&remark7,&remark8,&remark9,&remark10,&sms_lms_tit,&tmpl_id,&supplement)
	if rowErr != nil {
		config.Stdlog.Println("Result Process - Request select error :" + rowErr.Error(), " ( msgid : ", result.Msgid , ")")
		//fmt.Println("결과 처리 Request select  처리 중 오류 발생 :" + rowErr.Error())
		//fmt.Println(drQuery)
		return	
	}
	tx.Exec("delete from DHN_REQUEST where msgid = '" + result.Msgid + "'")
    
    instable := ""
    
    if s.EqualFold(result.Reserve_dt, "00000000000000") {
		instable = s.Replace(result.Reg_dt[:7],"-","",-1)    
    } else {
		instable = result.Reserve_dt[:6]    
    }
    
    insQuery = s.Replace(insQuery, "#{PreMonth}", instable, -1)
    
	insQuery = insQuery + "'" + result.Msgid+ "',"
	insQuery = insQuery + "'" + result.Ad_flag+ "',"
	insQuery = insQuery + "'" + s.Replace(button1.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(button2.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(button3.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(button4.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(button5.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + result.Code+ "',"
	insQuery = insQuery + "'" + result.Image_link+ "',"
	insQuery = insQuery + "'" + result.Image_url+ "',"
	insQuery = insQuery + "'" + result.Kind+ "',"
	//msgE, _ :=  iconv.ConvertString(result.Message, "utf-8", "euc-kr")
	insQuery = insQuery + "'" + s.Replace(result.Message,"'","''",-1) + "',"
	insQuery = insQuery + "'" + result.Message_type+ "',"
	insQuery = insQuery + "'" + s.Replace(msg.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(msg_sms.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + only_sms.String  + "',"
	insQuery = insQuery + "'" + result.P_com+ "',"
	insQuery = insQuery + "'" + result.P_invoice+ "',"
	insQuery = insQuery + "'" + result.Phn+ "',"
	insQuery = insQuery + "'" + result.Profile+ "',"
	insQuery = insQuery + "'" + result.Reg_dt[:10]+ "',"
	insQuery = insQuery + "'" + s.Replace(remark1.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(remark2.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(remark3.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(remark4.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(remark5.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(remark6.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(remark7.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(remark8.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(remark9.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + s.Replace(remark10.String,"'","''",-1) + "',"
	insQuery = insQuery + "sysdate,"
	insQuery = insQuery + "'" + result.Reserve_dt+ "',"
	if s.EqualFold(result.Code, "0000") || s.EqualFold(result.Code, "MS03") || s.EqualFold(result.Code, "K000") {
		insQuery = insQuery + "'Y',"
	} else {
		insQuery = insQuery + "'N',"
	}				
	
	insQuery = insQuery + "'" + result.S_code+ "',"
	insQuery = insQuery + "'" + result.Sms_kind+ "',"
	insQuery = insQuery + "'" + s.Replace(sms_lms_tit.String,"'","''",-1) + "',"
	insQuery = insQuery + "'" + result.Sms_sender+ "',"
	insQuery = insQuery + "'" + result.Sync+ "',"
	insQuery = insQuery + "'" + result.Tmpl_id+ "',"
	insQuery = insQuery + "'" + result.Wide+ "',"
	insQuery = insQuery + "'" + result.Supplement+ "',"
	insQuery = insQuery + "'" + result.Price+ "',"
	insQuery = insQuery + "'" + result.Currency_type+ "',"
	insQuery = insQuery + "'" + result.Title+ "')"
 	
	_, err = tx.Exec(insQuery)

	if err != nil {
		config.Stdlog.Println("Result Table Insert error :" + err.Error(), " ( msgid : ", result.Msgid , ")")
		//config.Stdlog.Println("Result Table Insert Query :")
		//config.Stdlog.Println(insQuery)
		//fmt.Println("Request Table - select 오류 : " + err.Error())
		return
	}

	return
}


func insertResult2(result resulttable.ResultTable) {


	insQuery := `INSERT INTO ` + config.Conf.RESULTTABLE + `2(
	  MSGID,CODE,MESSAGE, MESSAGE_TYPE,REG_DT,RES_DT,RESULT
	) values (`


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


	//var remark6,remark7,remark8,remark9,remark10 sql.NullString

	//tx.QueryRow("select remark6,remark7,remark8,remark9,remark10 from DHN_REQUEST r where msgid = '" + result.Msgid + "'").Scan(&remark6,&remark7,&remark8,&remark9,&remark10)
	tx.Exec("delete from DHN_REQUEST where msgid = '" + result.Msgid + "'")

	insQuery = insQuery + "'" + result.Msgid+ "',"
	insQuery = insQuery + "'" + result.Code+ "',"
	msgE, _ :=  iconv.ConvertString(result.Message, "utf-8", "euc-kr")
	insQuery = insQuery + "'" + msgE + "',"
	insQuery = insQuery + "'" + result.Message_type+ "',"
	insQuery = insQuery + "'" + result.Reg_dt[:10]+ "',"
	insQuery = insQuery + "sysdate,"
	if s.EqualFold(result.Code, "0000") || s.EqualFold(result.Code, "MS03") || s.EqualFold(result.Code, "K000") {
		insQuery = insQuery + "'Y'"
	} else {
		insQuery = insQuery + "'N'"
	}				
	insQuery = insQuery + ")"
 
	_, err = tx.Exec(insQuery)

	if err != nil {
		config.Stdlog.Println("Result Table Insert error :" + err.Error(), result.Msgid)
		//config.Stdlog.Println(insQuery)
		//fmt.Println("Request Table - select 오류 : " + err.Error())
		return
	}

	return
}



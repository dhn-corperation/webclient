package sendrequest

import (
	//"bytes"
	"config"
	"database/sql"
	"databasepool"

	//"encoding/json"
	"fmt"
	//"io/ioutil"

	//"net/http"
	//s "strings"

	//	"sync"
	"time"
	//iconv "github.com/djimenez/iconv-go"
)

var procCnt int = 0
var DisplayStatus bool = false

func Process() {
	
	defer func() {
		if err := recover(); err != nil {
			config.Stdlog.Println("Send Process Error - Proc CNT : ",procCnt, " , message : " , err)
		}
	} ()
	
	for {
		if procCnt < 5 {
	
			var count int
	
			cnterr := databasepool.DB.QueryRow(" select count(1) as cnt from DHN_REQUEST r where group_no is null and (r.reserve_dt < to_char(sysdate,'yyyymmddhh24miss') or r.reserve_dt = '00000000000000') and rownum = 1").Scan(&count)
	
			if cnterr != nil {
				config.Stdlog.Println("Request Table - select error : " + cnterr.Error())
			} else {
				if count > 0 {
					var startNow = time.Now()
					var group_no = fmt.Sprintf("%02d%02d%02d%09d", startNow.Hour(), startNow.Minute(), startNow.Second(), startNow.Nanosecond())
					
					updateReqeust(group_no)
					go sendProcess(group_no)
					
/*
					tx, err := databasepool.DB.Begin()
					if err != nil {
						config.Stdlog.Println("send Req Error DB Begin !!")
						panic("send Req Error DB Begin !!")
					}
					
					updateRows, uperr := tx.Exec("update DHN_REQUEST r set group_no = '" + group_no + "' where  group_no is null and (r.reserve_dt < to_char(sysdate,'yyyymmddhh24miss') or r.reserve_dt = '00000000000000')  and rownum <= 10")
					
					if uperr != nil {
						config.Stdlog.Println("Request Table - Group No Update error : " + uperr.Error())
					} else {
						rowcnt, err1 := updateRows.RowsAffected()
						if err1 != nil {
							rowcnt = 0
							config.Stdlog.Println("Request Table - RowsAffected error : " + err1.Error())
						}
	
						if rowcnt > 0 {
							//config.Stdlog.Println(group_no);
							//sendProcess(group_no)
							//fmt.Println(rowcnt, " 발송 시작")
						}
					}
					tx.Commit()
					*/
				}
			}
			
		}
		
		if DisplayStatus {
			config.Stdlog.Println("Send Process - Proc CNT : ",procCnt)
			DisplayStatus = false
		}

	}
	config.Stdlog.Println("Send Process End !!")
}

func updateReqeust(group_no string) {
	tx, err := databasepool.DB.Begin()
	if err != nil {
		return
	}

	defer func() {
		//config.Stdlog.Println("Group No Update End", group_no)
		if err != nil {
			tx.Rollback()
			return
		}
		err = tx.Commit()
	}()

	config.Stdlog.Println("Group No Update - ", group_no)	

	reqrows, err := tx.Query("select r.msgid from DHN_REQUEST r  where  group_no is null and (r.reserve_dt < to_char(sysdate,'yyyymmddhh24miss') or r.reserve_dt = '00000000000000')  and rownum <= 1000")
	if err != nil {
		config.Stdlog.Println(" Group NO Update - Select error : ( " + group_no + " ) : " + err.Error())
		return
	}
	
	for reqrows.Next() {
		var msgid sql.NullString
		reqrows.Scan(&msgid)
		if _, err = tx.Exec("update DHN_REQUEST r set group_no = '" + group_no + "' where  msgid = '" + msgid.String +"'"); err != nil {
			return
		} 
	}
	return
}

func sendProcess(group_no string) {

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

	procCnt++

	var conf = config.Conf
	var stdlog = config.Stdlog
	var errlog = config.Stdlog

	defer func() {
		if err := recover(); err != nil {
			procCnt--
			stdlog.Println(group_no, "Send Process error : ", err)
		}
	} ()
 
	reqsql := `select msgid
  ,ad_flag
  ,button1
  ,button2
  ,button3
  ,button4
  ,button5
  ,image_link
  ,image_url
  ,message_type
  ,msg
  ,msg_sms
  ,only_sms
  ,p_com
  ,p_invoice
  ,phn
  ,profile
  ,TO_CHAR(SYSDATE,'yyyy-mm-dd hh24:mi:ss') as reg_dt
  ,remark1
  ,remark2
  ,remark3
  ,remark4
  ,remark5
  ,reserve_dt
  ,s_code
  ,sms_kind
  ,sms_lms_tit
  ,sms_sender
  ,tmpl_id
  ,wide
  ,supplement
  ,price
  ,currency_type
  ,group_no
  ,title from ` + conf.REQTABLE + ` where group_no = '` + group_no + `'`

	
	reqrows, err := tx.Query(reqsql)
	if err != nil {
		//errlog.Fatal(err)
		config.Stdlog.Println(conf.REQTABLE + " Table - Select error : ( " + group_no + " ) : " + err.Error())
		return
	}
	//stdlog.Printf(group_no,3);
	finalRows := []interface{}{}

	//var isContinue bool
	var procCount int
	procCount = 0
	var startNow = time.Now()
	var startTime = fmt.Sprintf("%02d:%02d:%02d", startNow.Hour(), startNow.Minute(), startNow.Second())
	stdlog.Printf(" ( %s ) Send start - %s ", startTime, group_no)
	//stdlog.Printf(group_no,4);
	//stdlog.Printf(len(reqrows));
	for reqrows.Next() {
		//scanArgs := make([]interface{}, count)
		var msgid,ad_flag,button1,button2,button3,button4,button5,image_link,image_url,message_type,msg,msg_sms,only_sms,p_com,p_invoice,phn,profile,reg_dt,remark1,remark2,remark3,remark4,remark5,reserve_dt,s_code,sms_kind,sms_lms_tit,sms_sender,tmpl_id,wide,supplement,price,currency_type,group_no,title sql.NullString
		reqrows.Scan(&msgid,&ad_flag,&button1,&button2,&button3,&button4,&button5,&image_link,&image_url,&message_type,&msg,&msg_sms,&only_sms,&p_com,&p_invoice,&phn,&profile,&reg_dt,&remark1,&remark2,&remark3,&remark4,&remark5,&reserve_dt,&s_code,&sms_kind,&sms_lms_tit,&sms_sender,&tmpl_id,&wide,&supplement,&price,&currency_type,&group_no,&title)

		masterData := map[string]interface{}{}

		masterData["msgid"]=msgid.String
		masterData["adflag"]=ad_flag.String
		masterData["button1"]=button1.String 
		masterData["button2"]=button2.String 
		masterData["button3"]=button3.String 
		masterData["button4"]=button4.String 
		masterData["button5"]=button5.String 
		masterData["imagelink"]=image_link.String
		masterData["imageurl"]=image_url.String
		masterData["messagetype"]=message_type.String
		masterData["msg"]=msg.String 
		masterData["msgsms"] = msg_sms.String 
		masterData["onlysms"]=only_sms.String
		masterData["pcom"]=p_com.String
		masterData["pinvoice"]=p_invoice.String
		masterData["phn"]=phn.String
		masterData["profile"]=profile.String
		masterData["regdt"]=reg_dt.String
		masterData["remark1"]=remark1.String 
		masterData["remark2"]=remark2.String 
		masterData["remark3"]=remark3.String 
		masterData["remark4"]=remark4.String 
		masterData["remark5"]=remark5.String 
		masterData["reservedt"]=reserve_dt.String
		masterData["scode"]=s_code.String
		masterData["smskind"]=sms_kind.String
		masterData["smslmstit"]=sms_lms_tit.String 
		masterData["smssender"]=sms_sender.String
		masterData["tmplid"]=tmpl_id.String
		masterData["wide"]=wide.String
		masterData["supplement"]=supplement.String 
		masterData["price"]=price.String
		masterData["currencytype"]=currency_type.String
		masterData["title"]=title.String 

		//errlog.Println("Req ; ", reg_dt.String, message_type.String)
		finalRows = append(finalRows, masterData)
		procCount++
	}
	//b, _ := json.Marshal(finalRows)
	//fmt.Println(string(b))
	
	//return
	resp, err := config.Client.R().
		SetHeaders(map[string]string{"Content-Type": "application/json", "userid": conf.USERID}).
		SetBody(finalRows).
		Post(conf.SERVER + "req")

	if err != nil {
		errlog.Println("Message Server error !! - ", err)
		//tx, _ := databasepool.DB.Begin()
		tx.Exec("update " + conf.REQTABLE + "set group_no = null where group_no = '" + group_no + "'")
		//tx.Commit()
	} else {

		if resp.StatusCode() == 200 {
			//tx.Exec("delete from " + conf.REQTABLE + " where group_no = '" + group_no + "'")

		} else {
			stdlog.Println("Request Process error !! ( ", resp, " )")
		}
	}
	//stdlog.Printf(group_no,5);
	stdlog.Printf(" ( %s ) Send complete - Message count : %d ( process cnt : %d ) - %s", startTime, procCount, procCnt, group_no)
	//stdlog.Printf(group_no,6);
	procCnt--
	return
}

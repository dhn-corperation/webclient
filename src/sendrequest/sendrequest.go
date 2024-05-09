package sendrequest

import (
	//"bytes"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"database/sql"
	"goclient_knou/src/config"
	"goclient_knou/src/databasepool"

	"fmt"
	"io"

	//"io/ioutil"

	//"net/http"

	s "strings"

	//	"sync"
	"time"
	//iconv "github.com/djimenez/iconv-go"
)

var procCnt int = 0
var DisplayStatus bool = false
var SecretKey = "9b4dabe9d4fed126a58f8639846143c7"

func Process() {

	defer func() {
		if err := recover(); err != nil {
			config.Stdlog.Println("Send Process Error - Proc CNT : ", procCnt, " , message : ", err)
		}
	}()

	var conf = config.Conf

	for {
		if procCnt < 5 {

			var count int

			cnterr := databasepool.DB.QueryRow(" select count(1) as cnt from " + conf.SC_TRAN + " where TR_SENDSTAT = '0' and tr_senddate <= sysdate and tr_modified is null and to_number(to_char(sysdate, 'hh24')) BETWEEN " + conf.TRAN_FROM + " AND (" + conf.TRAN_TO + "-1) and rownum = 1").Scan(&count)
			//fmt.Println("select count(1) as cnt from " + conf.SC_TRAN + " where TR_SENDSTAT = '0' and tr_senddate <= sysdate and to_number(to_char(sysdate + 0.69, 'hh24')) BETWEEN " + conf.TRAN_FROM + " AND (" + conf.TRAN_TO + "-1) and rownum = 1")
			if cnterr != nil {
				config.Stdlog.Println("Request Table - select error : " + cnterr.Error())
			} else {
				if count > 0 {
					var startNow = time.Now()
					var group_no = fmt.Sprintf("%04d%02d%02d%02d%02d%02d", startNow.Year(), startNow.Month(), startNow.Day(), startNow.Hour(), startNow.Minute(), startNow.Second())

					updateReqeust(group_no)
					go sendProcess(group_no)
					time.Sleep(time.Millisecond * time.Duration(1000))
				}
			}

		}

		if DisplayStatus {
			config.Stdlog.Println("Send Process - Proc CNT : ", procCnt)
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
	var conf = config.Conf
	config.Stdlog.Println("Group No Update - ", group_no)
	gudQuery := `
	update ` + conf.SC_TRAN + `  set TR_SENDSTAT = '9', tr_modified = to_date('` + group_no + `', 'yyyymmddhh24miss')  where  TR_NUM in (
		select r.TR_NUM from ` + conf.SC_TRAN + ` r  where  TR_SENDSTAT = '0' and tr_senddate <= sysdate and rownum <= 1000 and tr_modified is null  
	)
	`
	_, err = tx.Query(gudQuery)
	if err != nil {
		config.Stdlog.Println(" Group NO Update - Select error : ( " + group_no + " ) : " + err.Error())
		config.Stdlog.Println(gudQuery)
		return
	}
	/*
		for reqrows.Next() {
			var tr_num sql.NullString
			reqrows.Scan(&tr_num)
			if _, err = tx.Exec("update " + conf.SC_TRAN + "  set TR_SENDSTAT = '9', tr_modified = to_date('" + group_no + "', 'yyyymmddhh24miss')  where  TR_NUM = '" + tr_num.String + "'"); err != nil {
				return
			}
		}
	*/
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
	}()

	reqsql := `SELECT st.*
, to_char(sysdate, 'yyyy-mm-dd hh24:mi:ss') as curr_date 
,RANK() OVER (PARTITION BY  st.TR_PHONE , st.TR_CALLBACK , st.TR_MSG ORDER BY st.TR_NUM) AS mst_cnt
FROM ` + conf.SC_TRAN + ` st 
WHERE TR_SENDSTAT  = '9'
and tr_modified = to_date('` + group_no + `', 'yyyymmddhh24miss')
`
	reqrows, err := tx.Query(reqsql)
	if err != nil {
		//errlog.Fatal(err)
		config.Stdlog.Println(conf.SC_TRAN + " Table - Select error : ( " + group_no + " ) : " + err.Error())
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
		var trNum, trSenddate, trId, trSendstat, trRsltstat, trMsgtype, trPhone, trCallback, trRsltdate, trModified, trMsg, trEtc1, trEtc2, trEtc3, trEtc4, trEtc5, trEtc6, trNet, trCurrendDate sql.NullString
		var MsgCnt sql.NullInt16
		var smsKind, smslmstit string
		var msg, msgT, nonce, phn, sender []byte

		reqrows.Scan(&trNum, &trSenddate, &trId, &trSendstat, &trRsltstat, &trMsgtype, &trPhone, &trCallback, &trRsltdate, &trModified, &trMsg, &trEtc1, &trEtc2, &trEtc3, &trEtc4, &trEtc5, &trEtc6, &trNet, &trCurrendDate, &MsgCnt)

		if MsgCnt.Int16 == 1 {
			masterData := map[string]interface{}{}

			if s.EqualFold(trMsgtype.String, "0") {
				smsKind = "S"
			} else if s.EqualFold(trMsgtype.String, "1") {
				smsKind = "L"
			} else {
				smsKind = ""
			}

			if len(trMsg.String) <= 30 {
				smslmstit = trMsg.String
			} else {
				smslmstit = trMsg.String[:30]
			}

			masterData["msgid"] = trNum.String
			masterData["messagetype"] = "ph"
			msg, nonce, _ = AES256GSMEncrypt([]byte(SecretKey), []byte(trMsg.String), nonce)
			masterData["msg"] = fmt.Sprintf("%x", msg)
			masterData["msgsms"] = fmt.Sprintf("%x", msg)
			phn, nonce, _ = AES256GSMEncrypt([]byte(SecretKey), []byte(trPhone.String), nonce)
			masterData["phn"] = fmt.Sprintf("%x", phn)
			masterData["regdt"] = trCurrendDate.String
			masterData["remark1"] = trEtc1.String
			masterData["remark2"] = trEtc2.String
			masterData["remark3"] = trEtc3.String
			masterData["remark4"] = trEtc4.String
			masterData["remark5"] = trEtc5.String
			masterData["pinvoice"] = trId.String
			masterData["pcom"] = "D"
			masterData["profile"] = fmt.Sprintf("%x", nonce)
			masterData["smskind"] = smsKind
			msgT, nonce, _ = AES256GSMEncrypt([]byte(SecretKey), []byte(smslmstit), nonce)
			masterData["smslmstit"] = fmt.Sprintf("%x", msgT)
			sender, nonce, _ = AES256GSMEncrypt([]byte(SecretKey), []byte(trCallback.String), nonce)
			masterData["smssender"] = fmt.Sprintf("%x", sender)
			masterData["crypto"] = "Y"

			//errlog.Println("Req ; ", reg_dt.String, message_type.String)
			finalRows = append(finalRows, masterData)
		} else {
			tx.Exec("update " + conf.SC_TRAN + " set tr_sendstat='2', tr_rsltdate = sysdate, tr_modified = sysdate, tr_rsltstat = '-1', tr_net = '0' where tr_num = '" + trNum.String + "'")
			tx.Exec("insert into " + conf.SC_LOG + " select * from " + conf.SC_TRAN + " st where st.tr_num = '" + trNum.String + "'")
			tx.Exec("delete from " + conf.SC_TRAN + " where tr_num = '" + trNum.String + "'")
		}
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
		//tx.Exec("update sc_tran set tr_modified = sysdate, tr_sendstat = '3' where tr_sendstat = '9' ")
		tx.Exec("update " + conf.SC_TRAN + " set tr_sendstat='2', tr_rsltdate = sysdate, tr_modified = sysdate, tr_rsltstat = '11' where tr_sendstat = '9' and tr_modified = to_date('" + group_no + "', 'yyyymmddhh24miss')")
		tx.Exec("insert into " + conf.SC_LOG + " select * from " + conf.SC_TRAN + " st where st.tr_sendstat = '9' and tr_modified = to_date('" + group_no + "', 'yyyymmddhh24miss')")
		tx.Exec("delete from " + conf.SC_TRAN + " st where st.tr_sendstat = '9' and tr_modified = to_date('" + group_no + "', 'yyyymmddhh24miss')")
		//tx.Commit()
	} else {

		if resp.StatusCode() == 200 {
			tx.Exec("update " + conf.SC_TRAN + " set tr_sendstat = '1' where tr_sendstat = '9' and tr_modified = to_date('" + group_no + "', 'yyyymmddhh24miss')")
			//tx.Exec("insert into sc_log select * from sc_tran where tr_sendstat = 'W' and tr_etc6 = '" + group_no + "'")
			//tx.Exec("delete from sc_tran where tr_sendstat = 'W' and tr_etc6 = '" + group_no + "'")
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

func AES256GSMEncrypt(secretKey []byte, plaintext []byte, nonce []byte) ([]byte, []byte, error) {

	if len(secretKey) != 32 {
		//fmt.Printf("secret key is not for AES-256: total %d bits, %d", 8*len(secretKey), len(secretKey))
		return nil, nil, fmt.Errorf("secret key is not for AES-256: total %d bits", 8*len(secretKey))
	}

	block, err := aes.NewCipher(secretKey)
	if err != nil {
		return nil, nil, err

	}

	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, nil, err
	}

	// make random nonce
	if nonce == nil {
		nonce = make([]byte, 12)
		if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
			return nil, nil, err
		}
	}
	// encrypt plaintext
	ciphertext := aesgcm.Seal(nil, nonce, plaintext, nil)

	//fmt.Printf("nonce: %x\n", nonce)
	//fmt.Println(nonce)

	return ciphertext, nonce, nil
}

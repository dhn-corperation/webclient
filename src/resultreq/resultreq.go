package resultreq

import (
	//	"bytes"
	//"database/sql"
	"encoding/json"
	"fmt"

	//kakao "kakaojson"
	"webclient/src/config"
	"webclient/src/resulttable"

	"webclient/src/databasepool"
	//"io/ioutil"
	//"net/http"

	//"strconv"
	s "strings"
	"sync"
	"time"
)

var Interval int32 = 10000

func ResultReqProc() {
	var wg sync.WaitGroup

	for config.IsRunning {
		wg.Add(1)

		go gerResultProcess(&wg)

		wg.Wait()
		time.Sleep(time.Millisecond * time.Duration(Interval))
	}
	config.Stdlog.Println("Result Req Process End !!")
}

func gerResultProcess(wg *sync.WaitGroup) {

	defer wg.Done()

	var db = databasepool.DB
	var conf = config.Conf

	var errlog = config.Stdlog
	var procCnt int = 0

	resStrs := []string{}
	resValues := []interface{}{}

	resIns := `INSERT IGNORE INTO ` + conf.RESULTTABLE + `(
  MSGID, AD_FLAG, BUTTON1, BUTTON2, 
  BUTTON3, BUTTON4, BUTTON5, 
  CODE, IMAGE_LINK, IMAGE_URL, 
  KIND, MESSAGE, MESSAGE_TYPE, 
  MSG, MSG_SMS, ONLY_SMS, P_COM, 
  P_INVOICE, PHN, PROFILE, REG_DT, 
  REMARK1, REMARK2, REMARK3, 
  REMARK4, REMARK5, RES_DT, RESERVE_DT, 
  RESULT, S_CODE, SMS_KIND, SMS_LMS_TIT, 
  SMS_SENDER, SYNC, TMPL_ID, 
  WIDE, SUPPLEMENT, PRICE, CURRENCY_TYPE, TITLE, HEADER, CAROUSEL, ATT_ITEMS
) values %s`

	resp, err := config.Client.R().
		SetHeaders(map[string]string{"userid": conf.USERID}).
		Post(conf.SERVER + "result")

	if err != nil {
		errlog.Println("메시지 결과 요청 오류 : ", err)
	} else {

		if resp.StatusCode() == 200 {
			str := resp.Body()
			var result []resulttable.ResultTable
			json.Unmarshal([]byte(str), &result)

			if len(result) >= 1 {
				Interval = 500
			} else {
				if Interval >= 10000 {
					Interval = 10000
				} else {
					Interval = Interval + 1000
				}
			}

			for i, _ := range result {
				resStrs = append(resStrs, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,now(),?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)")
				resValues = append(resValues, result[i].Msgid)
				resValues = append(resValues, result[i].Ad_flag)
				resValues = append(resValues, result[i].Button1)
				resValues = append(resValues, result[i].Button2)
				resValues = append(resValues, result[i].Button3)
				resValues = append(resValues, result[i].Button4)
				resValues = append(resValues, result[i].Button5)
				resValues = append(resValues, result[i].Code)
				resValues = append(resValues, result[i].Image_link)
				resValues = append(resValues, result[i].Image_url)
				resValues = append(resValues, result[i].Kind)
				resValues = append(resValues, result[i].Message)
				resValues = append(resValues, result[i].Message_type)
				resValues = append(resValues, result[i].Msg)
				resValues = append(resValues, result[i].Msg_sms)
				resValues = append(resValues, result[i].Only_sms)
				resValues = append(resValues, result[i].P_com)
				resValues = append(resValues, result[i].P_invoice)
				resValues = append(resValues, result[i].Phn)
				resValues = append(resValues, result[i].Profile)
				resValues = append(resValues, result[i].Reg_dt)
				resValues = append(resValues, result[i].Remark1)
				resValues = append(resValues, result[i].Remark2)
				resValues = append(resValues, result[i].Remark3)
				resValues = append(resValues, result[i].Remark4)
				resValues = append(resValues, result[i].Remark5)
				resValues = append(resValues, result[i].Reserve_dt)

				if s.EqualFold(result[i].Code, "0000") || s.EqualFold(result[i].Code, "MS03") || s.EqualFold(result[i].Code, "K000") {
					resValues = append(resValues, "Y")
				} else {
					resValues = append(resValues, "N")
				}

				resValues = append(resValues, result[i].S_code)
				resValues = append(resValues, result[i].Sms_kind)
				resValues = append(resValues, result[i].Sms_lms_tit)
				resValues = append(resValues, result[i].Sms_sender)
				resValues = append(resValues, result[i].Sync)
				resValues = append(resValues, result[i].Tmpl_id)
				resValues = append(resValues, result[i].Wide)
				resValues = append(resValues, result[i].Supplement)
				resValues = append(resValues, result[i].Price)
				resValues = append(resValues, result[i].Currency_type)
				resValues = append(resValues, result[i].Title)
				resValues = append(resValues, result[i].Header)
				resValues = append(resValues, result[i].Carousel)
				resValues = append(resValues, result[i].Att_items)

				if len(resStrs) >= 1000 {
					stmt := fmt.Sprintf(resIns, s.Join(resStrs, ","))
					_, err := db.Exec(stmt, resValues...)

					if err != nil {
						errlog.Println("Result Table Insert 처리 중 오류 발생 " + err.Error())
					}

					resStrs = nil
					resValues = nil
				}
				procCnt++
			}

			if len(resStrs) > 0 {
				stmt := fmt.Sprintf(resIns, s.Join(resStrs, ","))
				_, err := db.Exec(stmt, resValues...)

				if err != nil {
					errlog.Println("Result Table Insert 처리 중 오류 발생 " + err.Error())
				}

				resStrs = nil
				resValues = nil
			}
			if procCnt > 0 {
				errlog.Println("결과 수신 완료 : ", procCnt, " 건 처리")
			}

		} else {
			errlog.Println("결과 서버 요청 처리 중 오류 발생 ", resp)
		}

		//}
	}
	//}

}

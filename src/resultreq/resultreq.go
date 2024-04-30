package resultreq

import (
	//	"bytes"
	//"database/sql"
	"encoding/json"
	"io"
	"net/http"

	//kakao "kakaojson"
	"webclient/src/common"
	"webclient/src/config"
	"webclient/src/resulttable"

	"webclient/src/databasepool"
	//"io/ioutil"
	//"net/http"

	//"strconv"
	s "strings"
	"sync"
	"time"

	"github.com/lib/pq"
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

	//var db = databasepool.DB
	var conf = config.Conf

	var errlog = config.Stdlog
	var procCnt int = 0

	resValues := []common.ResColumn{}

	req, err := http.NewRequest("POST", conf.SERVER+"result", nil)
	if err != nil {
		errlog.Println("resultreq.go -> DHNCenter /result API 발송 request 만들기 실패 ", err.Error())
	}
	req.Header.Set("userid", conf.USERID)

	resp, err := config.GoClient.Do(req)
	if err != nil {
		errlog.Println("resultreq.go -> 메시지 결과 요청 오류 : ", err)
	} else {

		if resp.StatusCode == 200 {
			str, _ := io.ReadAll(resp.Body)
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
				resValue := common.ResColumn{}
				resValue.Msgid = result[i].Msgid
				resValue.Ad_flag = result[i].Ad_flag
				resValue.Button1 = result[i].Button1
				resValue.Button2 = result[i].Button2
				resValue.Button3 = result[i].Button3
				resValue.Button4 = result[i].Button4
				resValue.Button5 = result[i].Button5
				resValue.Code = result[i].Code
				resValue.Image_link = result[i].Image_link
				resValue.Image_url = result[i].Image_url
				resValue.Kind = result[i].Kind
				resValue.Message = result[i].Message
				resValue.Message_type = result[i].Message_type
				resValue.Msg = result[i].Msg
				resValue.Msg_sms = result[i].Msg_sms
				resValue.Only_sms = result[i].Only_sms
				resValue.P_com = result[i].P_com
				resValue.P_invoice = result[i].P_invoice
				resValue.Phn = result[i].Phn
				resValue.Profile = result[i].Profile
				resValue.Reg_dt = result[i].Reg_dt
				resValue.Remark1 = result[i].Remark1
				resValue.Remark2 = result[i].Remark2
				resValue.Remark3 = result[i].Remark3
				resValue.Remark4 = result[i].Remark4
				resValue.Remark5 = result[i].Remark5
				resValue.Res_dt = "now()"
				resValue.Reserve_dt = result[i].Reserve_dt
				if s.EqualFold(result[i].Code, "0000") || s.EqualFold(result[i].Code, "MS03") || s.EqualFold(result[i].Code, "K000") {
					resValue.Result = "Y"
				} else {
					resValue.Result = "N"
				}
				resValue.S_code = result[i].S_code
				resValue.Sms_kind = result[i].Sms_kind
				resValue.Sms_lms_tit = result[i].Sms_lms_tit
				resValue.Sms_sender = result[i].Sms_sender
				resValue.Sync = result[i].Sync
				resValue.Tmpl_id = result[i].Tmpl_id
				resValue.Wide = result[i].Wide
				resValue.Supplement = result[i].Supplement
				resValue.Price = result[i].Price
				resValue.Currency_type = result[i].Currency_type
				resValue.Title = result[i].Title
				resValue.Header = result[i].Header
				resValue.Carousel = result[i].Carousel
				resValue.Att_items = result[i].Att_items

				resValues = append(resValues, resValue)

				if len(resValues) >= 1000 {
					insertResData(resValues)
					resValues = []common.ResColumn{}
				}
				procCnt++

			}
			if len(resValues) > 0 {
				insertResData(resValues)
			}

			if procCnt > 0 {
				errlog.Println("결과 수신 완료 : ", procCnt, " 건 처리")
			}

		} else {
			errlog.Println("resultreq.go -> 결과 서버 요청 처리 중 오류 발생 ", resp)
		}

		//}
	}
	//}

}

func insertResData(resValues []common.ResColumn) {
	tx, err := databasepool.DB.Begin()
	if err != nil {
		config.Stdlog.Println("resultreq.go / insertResData / ", config.Conf.RESULTTABLE, " / 트랜젝션 초기화 실패 ", err)
	}
	defer tx.Rollback()
	resStmt, err := tx.Prepare(pq.CopyIn(config.Conf.RESULTTABLE, common.GetResColumnPq(common.ResColumn{})...))
	if err != nil {
		config.Stdlog.Println("resultreq.go / insertResData / ", config.Conf.RESULTTABLE, " / resStmt 초기화 실패 ", err)
		return
	}
	for _, data := range resValues {
		_, err := resStmt.Exec(data.Msgid, data.Ad_flag, data.Button1, data.Button2, data.Button3, data.Button4, data.Button5, data.Code, data.Image_link, data.Image_url, data.Kind, data.Message, data.Message_type, data.Msg, data.Msg_sms, data.Only_sms, data.P_com, data.P_invoice, data.Phn, data.Profile, data.Reg_dt, data.Remark1, data.Remark2, data.Remark3, data.Remark4, data.Remark5, data.Res_dt, data.Reserve_dt, data.Result, data.S_code, data.Sms_kind, data.Sms_lms_tit, data.Sms_sender, data.Sync, data.Tmpl_id, data.Wide, data.Supplement, data.Price, data.Currency_type, data.Title, data.Header, data.Carousel, data.Att_items)
		if err != nil {
			config.Stdlog.Println("resultreq.go / insertResData / ", config.Conf.RESULTTABLE, " / resStmt personal Exec ", err)
		}
	}

	_, err = resStmt.Exec()
	if err != nil {
		resStmt.Close()
		config.Stdlog.Println("resultreq.go / insertResData / ", config.Conf.RESULTTABLE, " / resStmt Exec ", err)
	}
	resStmt.Close()
	err = tx.Commit()
	if err != nil {
		config.Stdlog.Println("resultreq.go / insertResData / ", config.Conf.RESULTTABLE, " / resStmt commit ", err)
	}
}

package kfriendreq

import (
	"encoding/json"
	
	"config"

	"databasepool"
	"reqtable"

	s "strings"
	"sync"
	"time"
)

var Interval int32 = 10000
var PreMonth = ""

func FriendInfoReqProc() {
	var wg sync.WaitGroup
	
	var query = " select * from dhn_ft_info where rownum = 1"
	_, err := databasepool.DB.Query(query)
	
	if err != nil {
		createTable()
	}
	
	for {
			wg.Add(1)
			
			go getFriendInfo(&wg)
	
			wg.Wait()
			time.Sleep(time.Millisecond * time.Duration(Interval))
	}
	
}

func getFriendInfo(wg *sync.WaitGroup) {

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
		Post(conf.SERVER + "friendinfo")

	if err != nil {
		errlog.Println("Friend Infor receive process error : ", err)
	} else {

		if resp.StatusCode() == 200 {
			str := resp.Body()
			//fmt.Println(resp)
			var result []reqtable.FriendReqtable
			jerr := json.Unmarshal([]byte(str), &result)
			if jerr != nil {
				errlog.Println("Friend Infor Json process error : ", jerr)
			}
 
			for i, _ := range result {
				insertResult(result[i]) 
				procCnt++
			}

			if procCnt > 0 {
				errlog.Println("Friend Infor receive complete - Message count : ", procCnt)
			}

		} else {
			errlog.Println("server error ", resp)
		}

	}
}


func insertResult(result reqtable.FriendReqtable) {

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

	insQuery := `INSERT INTO dhn_ft_info(
	profile_key,
	linkcode,
	talk_type,
	at_template_code,
	at_msg_body,
	image_link,
	image_url,
	button1,
	button2,
	button3,
	button4,
	button5,
	approval_date,
	reg_date
	) values (`

 	insQuery = insQuery + "'" + s.Replace(result.Profilekey,"'","''",-1) + "',"
 	insQuery = insQuery + "'" + s.Replace(result.Linkcode,"'","''",-1) + "',"
 	insQuery = insQuery + "'" + s.Replace(result.TalkType,"'","''",-1) + "',"
 	insQuery = insQuery + "'" + s.Replace(result.AtTemplateCode,"'","''",-1) + "',"
 	insQuery = insQuery + "'" + s.Replace(result.AtMsgBody,"'","''",-1) + "',"
 	insQuery = insQuery + "'" + s.Replace(result.Imagelink,"'","''",-1) + "',"
 	insQuery = insQuery + "'" + s.Replace(result.Imageurl,"'","''",-1) + "',"
 	insQuery = insQuery + "'" + s.Replace(result.Button1,"'","''",-1) + "',"
 	insQuery = insQuery + "'" + s.Replace(result.Button2,"'","''",-1) + "',"
 	insQuery = insQuery + "'" + s.Replace(result.Button3,"'","''",-1) + "',"
 	insQuery = insQuery + "'" + s.Replace(result.Button4,"'","''",-1) + "',"
 	insQuery = insQuery + "'" + s.Replace(result.Button5,"'","''",-1) + "',"
	insQuery = insQuery + "to_date('" + result.ApprovalDate + "','yyyy-mm-dd hh24:mi:ss'),"
	insQuery = insQuery + "sysdate)"
 
	_, err = tx.Exec(insQuery)

	if err != nil {
		config.Stdlog.Println(insQuery)
		config.Stdlog.Println("Friend Infor Table Insert error :" + err.Error(), " ( Link Code : ", result.Linkcode , ")")
		return
	}
	return
}

func createTable() {
	cratequery := `CREATE TABLE dhn_ft_info (
	profile_key VARCHAR(40) not null,
	linkcode VARCHAR(40) not null,
	talk_type VARCHAR(1) not null,
	at_template_code VARCHAR(30),
	at_msg_body VARCHAR(4000),
	image_url VARCHAR(1000),
	image_link VARCHAR(1000),
	button1 VARCHAR(4000),
	button2 VARCHAR(4000),
	button3 VARCHAR(4000),
	button4 VARCHAR(4000),
	button5 VARCHAR(4000),
	approval_date date not null,
	reg_date date not null
)
`
databasepool.DB.Query(cratequery)

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.PROFILE_KEY IS '카카오톡 발신프로필키'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.LINKCODE IS '링크 코유 코드 서원지점코드+ _ + talk_type + _ + 년월일(20220101)'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.TALK_TYPE IS '톡 종류(F : 친구톡, A : 알림톡)'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.AT_TEMPLATE_CODE IS '알림톡 템플릿 코드'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.AT_MSG_BODY IS '알림톡 발송시 템플릿 및 문자 내용'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.IMAGE_URL IS '친구톡 이미지 URL'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.IMAGE_LINK IS '친구톡 이미지 클릭시 연결 URL'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.BUTTON1 IS '친구톡 Button 1 정보'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.BUTTON2 IS '친구톡 Button 2 정보'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.BUTTON3 IS '친구톡 Button 3 정보'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.BUTTON4 IS '친구톡 Button 4 정보'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.BUTTON5 IS '친구톡 Button 5 정보'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.APPROVAL_DATE IS '지점 승인일시'")

databasepool.DB.Query("COMMENT ON COLUMN DHN_FT_INFO.REG_DATE IS 'Interface 일시'")

databasepool.DB.Query("ALTER TABLE DHN_FT_INFO ADD PRIMARY KEY(LINKCODE)")


}
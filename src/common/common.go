package common

import (
	"database/sql"
	"reflect"
)

// 데이터베이스 default 값 초기화
func InitDatabaseColumn(columnTypes []*sql.ColumnType, length int) []interface{} {
	scanArgs := make([]interface{}, length)

	for i, v := range columnTypes {
		switch v.DatabaseTypeName() {
		case "VARCHAR", "TEXT", "UUID", "TIMESTAMP":
			scanArgs[i] = new(sql.NullString)
			break
		case "BOOL":
			scanArgs[i] = new(sql.NullBool)
			break
		case "INT4":
			scanArgs[i] = new(sql.NullInt64)
			break
		default:
			scanArgs[i] = new(sql.NullString)
		}
	}

	return scanArgs
}

func GetResColumn() []string {
	resColumn := []string{
		"msgid",
		"ad_flag",
		"button1",
		"button2",
		"button3",
		"button4",
		"button5",
		"code",
		"image_link",
		"image_url",
		"kind",
		"message",
		"message_type",
		"msg",
		"msg_sms",
		"only_sms",
		"p_com",
		"p_invoice",
		"phn",
		"profile",
		"reg_dt",
		"remark1",
		"remark2",
		"remark3",
		"remark4",
		"remark5",
		"res_dt",
		"reserve_dt",
		"result",
		"s_code",
		"sms_kind",
		"sms_lms_tit",
		"sms_sender",
		"sync",
		"tmpl_id",
		"wide",
		"supplement",
		"price",
		"currency_type",
		"title",
		"header",
		"carousel",
		"att_items",
	}

	return resColumn
}

func GetResColumnPq(mtype interface{}) []string {
	t := reflect.TypeOf(mtype)
	columns := make([]string, t.NumField())
	for i := 0; i < t.NumField(); i++ {
		columns[i] = t.Field(i).Tag.Get("db")
	}
	return columns
}

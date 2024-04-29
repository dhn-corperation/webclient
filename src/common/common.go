package common

import "database/sql"

//데이터베이스 default 값 초기화
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

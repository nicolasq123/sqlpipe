package sqlpipe

import (
	"bytes"
	"errors"
	"html/template"
	"net/url"
	"strings"

	"database/sql"

	_ "github.com/ClickHouse/clickhouse-go"
	_ "github.com/go-sql-driver/mysql"
	"github.com/jmoiron/sqlx"
)

var ErrDupMapKey = errors.New("duplicate map key error")

func StrListHas(l []string, v string) bool {
	for _, d := range l {
		if d == v {
			return true
		}
	}
	return false
}

func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

// func (db *DB) Select(dest interface{}, query string, args ...interface{}) error {
func Open(dsn string) (*sqlx.DB, error) {
	u, err := url.Parse(dsn)
	if err != nil {
		return nil, err
	}

	if u.Scheme == "postgres" || u.Scheme == "postgresql" {
		db, err := sqlx.Connect("postgres", dsn)
		if err != nil {
			return nil, err
		}
		db = db.Unsafe()
		return db, nil
	}

	if u.Scheme == "mysql" {
		dsn = strings.TrimPrefix(dsn, "mysql://")

		db, err := sqlx.Connect("mysql", dsn)
		if err != nil {
			return nil, err
		}
		db = db.Unsafe()
		return db, nil
	}

	if u.Scheme == "clickhouse" {
		q := u.Query()
		if database := strings.Trim(u.Path, "/"); database != "" {
			q.Add("database", database)
		}
		dsn = "tcp://" + u.Host + "?" + q.Encode()
		db, err := sqlx.Connect("clickhouse", dsn)
		if err != nil {
			return nil, err
		}
		db = db.Unsafe()
		return db, nil
	}

	return nil, errors.New("unsurpport Scheme")
}

// Rows函数解析sql.Rows，返回列名、map[string]string的切片,其中key为列名，value为对应的值
func Rows(sqlrows *sql.Rows) ([]string, []map[string]string, error) {
	defer sqlrows.Close()
	cols, err := sqlrows.Columns()
	if err != nil {
		return nil, nil, err
	}
	n := len(cols)
	row, err := getRows(sqlrows, n)
	if err != nil {
		return nil, nil, err
	}
	ret := make([]map[string]string, 0, len(row))
	for _, row := range row {
		m := make(map[string]string, n)
		for i, s := range row {
			m[cols[i]] = s
		}
		ret = append(ret, m)
	}

	return cols, ret, nil
}

func GetRows(rows *sql.Rows) ([][]string, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	n := len(columns)
	return getRows(rows, n)
}

func GetRowsInterface(rows *sql.Rows) ([][]interface{}, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, err
	}
	n := len(columns)
	return getRowsInterface(rows, n)
}

func getRows(rows *sql.Rows, n int) ([][]string, error) {
	var l [][]string
	for rows.Next() {
		r, err := scanRow(rows, n)
		if err != nil {
			return l, err
		}
		l = append(l, r)
	}
	return l, nil
}

func getRowsInterface(rows *sql.Rows, n int) ([][]interface{}, error) {
	var l [][]interface{}
	for rows.Next() {
		r, err := scanRowInterface(rows, n)
		if err != nil {
			return l, err
		}
		l = append(l, r)
	}
	return l, nil
}

func scanRow(rows *sql.Rows, n int) ([]string, error) {
	row := make([]string, n)
	ref := make([]interface{}, n)
	for i := 0; i < n; i++ {
		ref[i] = &row[i]
	}
	err := rows.Scan(ref...)
	return row, err
}

func scanRowInterface(rows *sql.Rows, n int) ([]interface{}, error) {
	row := make([]interface{}, n)
	ref := make([]interface{}, n)
	for i := 0; i < n; i++ {
		ref[i] = &row[i]
	}
	err := rows.Scan(ref...)
	return row, err
}

// 仅取一列
func getOneColFromRows(col string, rows []map[string]string) []interface{} {
	rs := []interface{}{}
	for _, row := range rows {
		rs = append(rs, row[col])
	}
	return rs
}

// func rows2List(col string, rows []map[string]string) []interface{} {
// 	ls := []interface{}{}
// 	for _, r := range rows {
// 		ls = append(ls, r[col])
// 	}
// 	return ls
// }

func genPlaceholders(n int) string {
	buf := new(bytes.Buffer)
	for i := 0; i < n; i++ {
		if i != 0 {
			buf.WriteString(",")
		}
		buf.WriteString("?")
	}
	return buf.String()
}

func Tpl(s string, d interface{}) (string, error) {
	t, err := template.New("").Parse(s)
	if err != nil {
		return "", err
	}
	buf := new(bytes.Buffer)
	err = t.Execute(buf, d)
	return buf.String(), err
}

func MergeMapWithNoDupKey(ms ...map[string]string) (res map[string]string, err error) {
	res = map[string]string{}
	for _, m := range ms {
		for k, v := range m {
			_, ok := res[k]
			if ok {
				err = ErrDupMapKey
				return
			}
			res[k] = v
		}
	}
	return
}

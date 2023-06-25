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

// func sqlxRows2Rows(rows *sqlx.Rows) ([]map[string]interface{}, error) {
// 	res := make([]map[string]interface{}, 0)
// 	var err error
// 	for rows.Next() {
// 		r := make(map[string]interface{})
// 		err = rows.MapScan(r)
// 		if err != nil {
// 			return nil, err
// 		}
// 		res = append(res, r)
// 	}
// 	return res, nil
// }

// func transferMyRows(cols []string, rows []map[string]interface{}) [][]string {
// 	res := make([][]string, 0)
// 	for i := range rows {
// 		m := transferMap(rows[i])
// 		records := []string{}
// 		for _, col := range cols {
// 			records = append(records, m[col])
// 		}
// 		res = append(res, records)
// 	}

// 	return res
// }

// func transferMap(m map[string]interface{}) map[string]string {
// 	res := make(map[string]string)
// 	for k, v := range m {
// 		res[k] = string(inter2String(v))
// 	}
// 	return res
// }

// func inter2String(v interface{}) []byte {
// 	w := []byte{}
// 	switch f := v.(type) {
// 	case bool:
// 		return strconv.AppendBool(w, f)
// 	case float32:
// 		return strconv.AppendFloat(w, float64(f), 'f', -1, 32)
// 	case float64:
// 		return strconv.AppendFloat(w, f, 'f', -1, 32)
// 	case int:
// 		return strconv.AppendInt(w, int64(f), 10)
// 	case int8:
// 		return strconv.AppendInt(w, int64(f), 10)
// 	case int16:
// 		return strconv.AppendInt(w, int64(f), 10)
// 	case int32:
// 		return strconv.AppendInt(w, int64(f), 10)
// 	case int64:
// 		return strconv.AppendInt(w, f, 10)
// 	case uint:
// 		return strconv.AppendUint(w, uint64(f), 10)
// 	case uint8:
// 		return strconv.AppendUint(w, uint64(f), 10)
// 	case uint16:
// 		return strconv.AppendUint(w, uint64(f), 10)
// 	case uint32:
// 		return strconv.AppendUint(w, uint64(f), 10)
// 	case uint64:
// 		return strconv.AppendUint(w, f, 10)
// 	case string:
// 		return append(w, f...)
// 	case []byte:
// 		return append(w, f...)
// 	case fmt.Stringer:
// 		return append(w, f.String()...)
// 	default: // TODO
// 		return w
// 	}
// }

// func transferMyRows2(cols []string, rows []map[string]interface{}) []map[string]string {
// 	res := make([]map[string]string, 0)
// 	for i := range rows {
// 		m := transferMap(rows[i])
// 		res = append(res, m)
// 	}

// 	return res
// }

// Rows函数解析sql.Rows，返回列名、map[string]string的切片,其中key为列名，value为对应的值
func Rows(rows *sql.Rows) ([]string, []map[string]string, error) {
	defer rows.Close()
	columns, err := rows.Columns()
	if err != nil {
		return nil, nil, err
	}
	n := len(columns)
	row, err := getRows(rows, n)
	if err != nil {
		return nil, nil, err
	}
	ret := make([]map[string]string, 0, len(row))
	for _, row := range row {
		m := make(map[string]string, n)
		for i, s := range row {
			m[columns[i]] = s
		}
		ret = append(ret, m)
	}

	return columns, ret, nil
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
func rows2list(col string, rows []map[string]string) []interface{} {
	rs := []interface{}{}
	for _, row := range rows {
		rs = append(rs, row[col])
	}
	return rs
}

func rows2List(col string, rows []map[string]string) []interface{} {
	ls := []interface{}{}
	for _, r := range rows {
		ls = append(ls, r[col])
	}
	return ls
}

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

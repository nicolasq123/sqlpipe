package sqlpipe

import (
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
)

type Job struct {
	Name    string
	DBName  string
	Query   string
	SubJobs []*Job
	Next    *Job
}

func (j *Job) Run(dims []map[string]string, dbs map[string]*sqlx.DB) (cols []string, data []map[string]string, err error) {
	query := j.Query
	placeholders := map[string]string{}
	args := []interface{}{}
	for _, sub := range j.SubJobs {
		col, onecolrows, placeholder, err1 := sub.runSubJob(dbs)
		if err1 != nil {
			return nil, nil, err1
		}
		args = append(args, onecolrows...)
		placeholders[col] = placeholder
	}
	query, err = Tpl(query, placeholders)

	db := dbs[j.DBName]
	var n int
	if len(dims) == 0 {
		cols, data, n, err = execute(db, query, args...)
		if err != nil {
			return
		}
	} else {
		for i, dim := range dims {
			querytmp, err := Tpl(query, dim)
			if err != nil {
				return nil, nil, err
			}
			colstmp, datatmp, ntmp, err := execute(db, querytmp, args...)
			if err != nil {
				return nil, nil, err
			}
			if i == 0 {
				cols = colstmp
			}
			data = append(data, datatmp...)
			n += ntmp
		}
	}

	if j.Next != nil {
		cols, data, err = j.Next.Run(data, dbs)
		if err != nil {
			return
		}
	} else {
		if n != 0 {
			log.Printf("query: %s, %d rows affected", j.Query, n)
		} else {
			log.Printf("query: %s, data is %v", j.Query, data)
		}
	}
	return
}

func (j *Job) runSubJob(dbs map[string]*sqlx.DB) (col string, args []any, placeholder string, err error) {
	query := j.Query
	db := dbs[j.DBName]
	cols := []string{}
	data := []map[string]string{}
	cols, data, _, err = execute(db, query)
	if err != nil {
		return
	}
	col = cols[0]
	args = rows2list(col, data)
	placeholder = genPlaceholders(len(args))
	return
}

func execute(db *sqlx.DB, query string, args ...any) (cols []string, data []map[string]string, n int, err error) {
	query2 := strings.TrimSpace(query)
	query2 = strings.ToLower(query2)
	if strings.HasPrefix(query2, "select") {
		sqlrows, err := db.Query(query, args...)
		if err != nil {
			return nil, nil, 0, err
		}
		cols, data, err = Rows(sqlrows)
		return cols, data, 0, err
	}

	affected, err := db.Exec(query, args...)
	if err != nil {
		return nil, nil, 0, err
	}
	n64, err := affected.RowsAffected()
	return nil, nil, int(n64), err
}

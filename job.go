package sqlpipe

import (
	"errors"
	"fmt"
	"log"
	"strings"

	"github.com/jmoiron/sqlx"
)

var ErrDimEmpty = errors.New("dim empty error")

type Job struct {
	Name    string
	DBName  string
	Query   string
	SubJobs []*Job
	Next    *Job

	affected int
}

func (j *Job) Run(dims []map[string]string, dbs map[string]*sqlx.DB) (cols []string, data []map[string]string, err error) {
	defer func() {
		if err != nil {
			err = fmt.Errorf("job name: %s, %v", j.Name, err)
		}
	}()
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

	db := dbs[j.DBName]
	var n int
	if len(dims) == 0 {
		query, err = Tpl(query, placeholders)
		log.Println("query is: ", query, args)
		cols, data, n, err = execute(db, query, args...)
		if err != nil {
			return
		}
	} else {
		for i, dim := range dims {
			newDim, err := MergeMapWithNoDupKey(dim, placeholders)
			if err != nil {
				return nil, nil, err
			}

			querytmp, err := Tpl(query, newDim)
			if err != nil {
				return nil, nil, err
			}
			log.Println("query is: ", querytmp, args, dim)
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

	if !j.isLastJob() {
		if len(data) == 0 {
			err = ErrDimEmpty
			return
		}

		cols, data, err = j.Next.Run(data, dbs)
		if err != nil {
			return
		}
	}

	j.affected = n
	return
}

func (j *Job) runSubJob(dbs map[string]*sqlx.DB) (col string, args []any, placeholder string, err error) {
	query := j.Query
	db := dbs[j.DBName]
	var cols []string
	var data []map[string]string
	cols, data, _, err = execute(db, query)
	if err != nil {
		err = fmt.Errorf("job name: %s, %v", j.Name, err)
		return
	}
	col = cols[0]
	args = getOneColFromRows(col, data)
	placeholder = genPlaceholders(len(args))
	return
}

func (j *Job) isLastJob() bool {
	return j.Next == nil
}

func (j *Job) isLastJobSelect() bool {
	last := j.lastJob()
	query := strings.TrimSpace(last.Query)
	query = strings.ToLower(query)
	return strings.HasPrefix(query, "select")
}

func (j *Job) getAffected() int {
	return j.lastJob().affected
}

func (j *Job) lastJob() *Job {
	if j.isLastJob() {
		return j
	}
	return j.Next.lastJob()
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

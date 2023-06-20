package sqlpipe

import (
	"bytes"
	"database/sql"
	"fmt"
	"strings"

	"github.com/jmoiron/sqlx"
)

type Job struct {
	Name    string
	DBName  string
	Query   string
	SubJobs []*Job
	Pre     *Job
	db      *sqlx.DB
}

func (j *Job) Run(dims []map[string]interface{}) (cols []string, data []map[string]string, err error) {
	query := j.Query
	placehs := map[string]string{}
	args := []interface{}{}
	for _, sub := range j.SubJobs {
		cols1, data1, err1 := sub.Run(nil)
		if err1 != nil {
			return nil, nil, err1
		}
		ls := rows2list(cols1[0], data1)
		args = append(args, ls...)
		query, err = Tpl(query, placehs)
	}

	if len(dims) == 0 && j.Pre == nil {

	} else if len(dims) == 0 {
		//log.Fatalf("job: %s, len dims is zero.", j.Name)
		return nil, nil, fmt.Errorf("job: %s, len dims is zero.", j.Name)
	}

	if len(dim) != 0 {
		query, err = Tpl(query, dim)
		if err != nil {
			return nil, nil, err
		}
	}


}

func (j *Job) execute(query string, args []interface{}) (cols []string, data []map[string]string, err error) {
	if strings.HasPrefix(query, "select") {
		sqlrows, err := j.db.Query(query, args...)
		if err != nil {
			return nil, nil, err
		}
		cols, data, err = Rows(sqlrows)
		return cols, data, err
	}

	
}

func Rows(sqlrows *sql.Rows) ([]string, []map[string]string, error) {
	return nil, nil, nil
}

func rows2list(col string, data []map[string]string) []interface{} {
	return []interface{}{}
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

func Tpl(q string, dim interface{}) (s string, err error) {
	return "", nil
}

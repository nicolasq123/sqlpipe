package sqlpipe

import (
	"os"
	"testing"
)

func TestNewSqlPipe(t *testing.T) {
	nextJob := &Job{
		Name:   "myjob2",
		DBName: "mysql2",
		Query:  "delete from user where id= {{.id}}",
	}

	c := &Conf{
		DBConfs: map[string]string{
			"mysql1": os.Getenv("TEST_MYSQL_DSN"),
			"mysql2": os.Getenv("TEST_MYSQL_DSN"),
		},
		Jobs: []*Job{
			{
				Name:   "myjob",
				DBName: "mysql1",
				Query:  "select id from user limit 1",
				Next:   nextJob,
			},
		},
	}

	err := c.Validate()
	if err != nil {
		t.Errorf("Validate err: %v", err)
	}
}

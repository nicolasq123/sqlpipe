package sqlpipe

import "testing"

func TestJob(t *testing.T) {
	nextJob := &Job{
		Name:   "myjob2",
		DBName: "mysql2",
		Query:  "delete from user where id= {{.id}}",
	}
	_ = nextJob
	// todo
}

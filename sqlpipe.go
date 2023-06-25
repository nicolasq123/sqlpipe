package sqlpipe

import (
	"errors"
	"fmt"

	"github.com/jmoiron/sqlx"
	"github.com/xwb1989/sqlparser"
)

// type SqlPipe struct {
// 	*Conf
// 	DBs map[string]*sql.DB //name-db

// 	tplRead  string // SELECT * FROM table WHERE xx GROUP BY dims
// 	tplWrite string // INSERT INTO table (a, b, c) VALUES %s
// 	writer   *bw.SQLWriter
// }

type Conf struct {
	DBConfs map[string]string // name-db
	Jobs    []*Job
}

type SqlPipe struct {
	conf *Conf
	dbs  map[string]*sqlx.DB
}

func (c *Conf) New() (*SqlPipe, error) {
	Panic(c.Validate())
	err := c.Validate()
	if err != nil {
		return nil, err
	}
	dbs := make(map[string]*sqlx.DB)
	for name, dsn := range c.DBConfs {
		db, err := Open(dsn)
		if err != nil {
			return nil, err
		}
		dbs[name] = db
	}
	return &SqlPipe{
		conf: c,
		dbs:  dbs,
	}, nil
}

func (c *Conf) Validate() error {
	if len(c.DBConfs) == 0 {
		return errors.New("empty dbconfs")
	}
	if len(c.Jobs) == 0 {
		return errors.New("empty Jobs")
	}
	for _, job := range c.Jobs {
		err := c.walk(job, c.checkDbName, c.checkDbName, c.checkQuery)
		if err != nil {
			return err
		}
	}
	return nil
}

type handler func(j *Job) error

// dfs
func (c *Conf) walk(j *Job, hs ...handler) error {
	if j == nil {
		return nil
	}

	for _, h := range hs {
		err := h(j)
		if err != nil {
			return err
		}
	}

	for _, job := range j.SubJobs {
		err := c.walk(job, hs...)
		if err != nil {
			return err
		}
	}

	return c.walk(j.Next, hs...)
}

func (c *Conf) checkDbName(j *Job) error {
	_, ok := c.DBConfs[j.DBName]
	if !ok {
		return fmt.Errorf("db name not found: %s", j.DBName)
	}
	return nil
}

func (c *Conf) checkQuery(j *Job) error {
	_, err := sqlparser.Parse(j.Query)
	return err
}

func (s *SqlPipe) Run() error {
	jobs := s.conf.Jobs
	for _, job := range jobs {
		_, _, err := s.runOneJob(job)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SqlPipe) runOneJob(job *Job) (cols []string, res []map[string]string, err error) {
	if job == nil {
		return
	}
	return job.Run(nil, s.dbs)
}

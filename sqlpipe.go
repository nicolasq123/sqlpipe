package sqlpipe

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/jmoiron/sqlx"
	"github.com/xwb1989/sqlparser"
)

var ErrJobNonSelect = errors.New("only last job can be insert/update/delete")

type Conf struct {
	DBConfs    map[string]string // name-db
	Jobs       []*Job
	WriterType string
	Debug      bool
}

type SqlPipe struct {
	conf   *Conf
	dbs    map[string]*sqlx.DB
	writer MyWriter
}

func (c *Conf) New() (*SqlPipe, error) {
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
		conf:   c,
		dbs:    dbs,
		writer: NewMyWriter(c.WriterType),
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
		// c.checkQueryValid todo
		err := c.walk(job, c.checkDbName, c.checkQuery)
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
	job := j
	for {
		if job.isLastJob() {
			break
		}
		query := strings.TrimSpace(job.Query)
		query = strings.ToLower(query)
		if !strings.HasPrefix(query, "select") {
			return ErrJobNonSelect
		}
		job = job.Next
	}

	return nil
}

func (c *Conf) checkQueryValid(j *Job) error {
	_, err := sqlparser.Parse(j.Query)
	return err
}

func (s *SqlPipe) Run() error {
	defer s.Close()
	jobs := s.conf.Jobs
	for _, job := range jobs {
		cols, res, err := s.runOneJob(job)
		if err != nil {
			return err
		}
		if job.isLastJobSelect() {
			err = s.writer.WriteRecords(cols, res)
		} else {
			tmp := map[string]string{}
			tmp["affected"] = strconv.Itoa(job.getAffected())
			err = s.writer.WriteRecords([]string{"affected"}, []map[string]string{tmp})
		}
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *SqlPipe) Close() {
	s.writer.Close()
	for _, db := range s.dbs {
		db.Close()
	}
}

func (s *SqlPipe) runOneJob(job *Job) (cols []string, res []map[string]string, err error) {
	if job == nil {
		return
	}
	return job.Run(nil, s.dbs)
}

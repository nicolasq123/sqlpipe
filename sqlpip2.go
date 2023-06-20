package sqlpipe

import "github.com/jmoiron/sqlx"

type SQLPipe struct {
	c   *Conf
	dbs map[string]*sqlx.DB
}

type Conf struct {
	DBConfs map[string]string
	Jobs    []*Job
}



func (s *SQLPipe) Run() error {
	
	
	return nil
}

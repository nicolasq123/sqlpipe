package sqlpipe

import "database/sql"

type SqlPipe struct {
	*Conf
	DBs      map[string]*sql.DB //name-db

	tplRead  string             // SELECT * FROM table WHERE xx GROUP BY dims
	tplWrite string             // INSERT INTO table (a, b, c) VALUES %s
	writer   *bw.SQLWriter
}

type Conf struct {
	DBConfs map[string]string // name-db

}


type Job struct {
	Input []*Job
	DbName string
	
}
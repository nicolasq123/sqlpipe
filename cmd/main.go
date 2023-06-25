package main

import (
	"io/ioutil"
	"log"

	"github.com/nicolasq123/sqlpipe"
	yaml "gopkg.in/yaml.v3"
)

func main() {
	c := &sqlpipe.Conf{
		// DBConfs: map[string]string{
		// 	"mysql1": "mysql://root:123456@(127.0.0.1)/test1?charset=utf8&sql_mode=TRADITIONAL&parseTime=true",
		// 	"mysql2": "mysql://root:123456@(127.0.0.1)/test1?charset=utf8&sql_mode=TRADITIONAL&parseTime=true",
		// },
		// Jobs: []*Job{
		// 	&Job{},
		// },
	}

	b, err := ioutil.ReadFile("./conf.yml")
	if err != nil {
		panic(err)
	}
	err = yaml.Unmarshal(b, c)
	if err != nil {
		panic(err)
	}

	log.Println("conf is: %v", c)

	s, err := c.New()
	sqlpipe.Panic(err)
	s.Run()
}

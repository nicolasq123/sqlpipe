package main

import (
	"flag"
	"io/ioutil"
	"log"
	"os"

	"github.com/nicolasq123/sqlpipe"
	yaml "gopkg.in/yaml.v3"
)

func main() {
	// c := &sqlpipe.Conf{
	// 	DBConfs: map[string]string{
	// 		"mysql1": "mysql://root:123456@(127.0.0.1)/test1?charset=utf8&sql_mode=TRADITIONAL&parseTime=true",
	// 		"mysql2": "mysql://root:123456@(127.0.0.1)/test1?charset=utf8&sql_mode=TRADITIONAL&parseTime=true",
	// 	},
	// 	Jobs: []*Job{
	// 		&Job{},
	// 	},
	// }
	c := parseConf()
	s, err := c.New()
	sqlpipe.Panic(err)
	err = s.Run()
	sqlpipe.Panic(err)
}

func parseConf() *sqlpipe.Conf {
	c := &sqlpipe.Conf{}
	confPath := flag.String("conf", "./conf.yml", "path to config")
	flag.Parse()

	b, err := ioutil.ReadFile(*confPath)
	sqlpipe.Panic(err)

	err = yaml.Unmarshal(b, c)
	sqlpipe.Panic(err)

	log.Printf("conf is: %v \n", c)

	out, _ := yaml.Marshal(c)
	os.Stdout.Write(out)
	return c
}

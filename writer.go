package sqlpipe

import (
	"encoding/csv"
	"errors"
	"os"
)

type MyWriter interface {
	WriteRecords(cols []string, records []map[string]interface{}) (err error)
	Close()
}

type csvwriter struct {
	path string
	w    *csv.Writer
	f    *os.File
}

func NewCsvWriter(path string) *csvwriter {
	if path == "" {
		path = "./output.csv"
	}
	file, err := os.OpenFile(path, os.O_RDWR|os.O_APPEND|os.O_CREATE, 0666)
	if err != nil {
		panic(errors.New("open failed"))
	}
	w := csv.NewWriter(file)

	return &csvwriter{path: path, f: file, w: w}
}
func (w *csvwriter) Write(record []string) (err error) {
	return w.w.Write(record)
}

func (w *csvwriter) Close() {
	w.w.Flush() //刷新，不刷新是无法写入的
	w.f.Close()
}

func (w *csvwriter) WriteRecords(cols []string, records [][]string) (err error) {
	w.w.Write(cols)
	for i := range records {
		w.w.Write(records[i])
	}
	return nil
}

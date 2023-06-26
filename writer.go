package sqlpipe

import (
	"encoding/csv"
	"errors"
	"fmt"
	"os"
)

type MyWriter interface {
	WriteRecords(cols []string, records []map[string]string) (err error)
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

func (w *csvwriter) WriteRecords(cols []string, records []map[string]string) (err error) {
	w.w.Write(cols)
	for _, m := range records {
		rows := make([]string, 0, len(cols))
		for _, col := range cols {
			rows = append(rows, m[col])
		}
		w.w.Write(rows)
	}
	return nil
}

type stdwriter struct {
}

func NewStdWriter() *stdwriter {
	return &stdwriter{}
}

func (w *stdwriter) Close() {
}

func (w *stdwriter) WriteRecords(cols []string, records []map[string]string) (err error) {
	fmt.Println("cols is: ", cols)
	fmt.Println("records is: ", records)
	return nil
}

type allwriter struct {
	c *csvwriter
	s *stdwriter
}

func NewAllWriter() *allwriter {
	return &allwriter{
		c: NewCsvWriter(""),
		s: NewStdWriter(),
	}
}

func (w *allwriter) Close() {
	w.c.Close()
	w.s.Close()
}

func (w *allwriter) WriteRecords(cols []string, records []map[string]string) (err error) {
	err = w.c.WriteRecords(cols, records)
	if err != nil {
		return nil
	}
	err = w.s.WriteRecords(cols, records)
	return err
}

func NewMyWriter(typ string) MyWriter {
	if typ == "csv" {
		return NewCsvWriter("")
	}
	if typ == "std" {
		return NewCsvWriter("")
	}
	return NewAllWriter()
}

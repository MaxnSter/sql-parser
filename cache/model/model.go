package model

import (
	"bytes"
	"github.com/MaxnSter/sql-parser/cache"
	"github.com/objectbox/objectbox-go/objectbox"
	"io"
)

var box *TableBox

//go:generate objectbox-gogen
type Table struct {
	Id   uint64
	Name string
	data []byte
}

func init() {
	b, err := objectbox.NewBuilder().Model(ObjectBoxModel()).Build()
	if err != nil {
		panic(err)
	}

	box = BoxForTable(b)
}

func CreateTableCache(name string, rc io.ReadCloser) (Id uint64, err error) {
	t := &Table{
		Name: name,
	}

	b := cache.Get()
	defer cache.Put(b)
	io.Copy(b, rc)
	rc.Close()

	t.data = append(t.data, b.Bytes()...)
	return box.Put(t)
}

type rc struct {
	b *bytes.Buffer
}

func (r *rc) Read(p []byte) (n int, err error) {
	return r.b.Read(p)
}

func (r *rc) Close() error {
	cache.Put(r.b)
	return nil
}

func GetTableCache(id uint64) (io.ReadCloser, error) {
	table, err := box.Get(id)
	if err != nil {
		return nil, err
	}

	b := cache.Get()
	b.Write(table.data)
	return &rc{b: b}, nil
}

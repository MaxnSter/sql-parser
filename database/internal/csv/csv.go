package csv

import (
	"encoding/csv"
	"github.com/MaxnSter/sql-parser/database"
	"github.com/go-restful/log"
	"github.com/pkg/errors"
	"github.com/schollz/progressbar"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"io"
	"time"
)

type db struct {
	tables map[string]sql.Table
	name   string
}

func init() {
	database.RegisterDatabaseBuilder("csv", Build)
}

func (d *db) Name() string                 { return d.name }
func (d *db) Tables() map[string]sql.Table { return d.tables }

func Build(dbName string, tb ...database.TableBuilder) (sql.Database, error) {
	d := &db{
		tables: map[string]sql.Table{},
		name:   dbName,
	}

	log.Printf("building db:%s", dbName)
	bar := progressbar.New(len(tb))
	for _, t := range tb {
		table, err := buildTable(t)
		if err != nil {
			return nil, err
		}

		d.tables[table.Name()] = table
		bar.Add(1)
	}
	bar.Clear()
	log.Printf("build db:%s ok", dbName)
	return d, nil
}

type table struct {
	name string
	s    sql.Schema

	builder database.TableBuilder
}

func buildTable(builder database.TableBuilder) (sql.Table, error) {
	time.Sleep(time.Second)
	name, r, err := builder()
	if err != nil {
		return nil, err
	}
	defer r.Close()

	t := &table{
		name:    name,
		builder: builder,
	}

	nr := csv.NewReader(r)
	headers, err := nr.Read()
	if err != nil {
		return nil, errors.Wrapf(err, "reader header error for table:%s", name)
	}
	for _, h := range headers {
		t.s = append(t.s, &sql.Column{
			Name:   h,
			Type:   sql.Text,
			Source: h,
		})
	}
	return t, nil
}

func (t *table) Name() string       { return t.name }
func (t *table) String() string     { return t.name }
func (t *table) Schema() sql.Schema { return t.s }

func (t *table) Partitions(*sql.Context) (sql.PartitionIter, error) {
	return &partitionIter{}, nil
}

func (t *table) PartitionRows(*sql.Context, sql.Partition) (sql.RowIter, error) {
	_, r, err := t.builder()
	if err != nil {
		return nil, err
	}

	nr := csv.NewReader(r)
	nr.Read()
	return &rowIter{
		Reader: nr,
		Closer: r,
	}, nil
}

type rowIter struct {
	*csv.Reader
	io.Closer
}

func (r *rowIter) Next() (sql.Row, error) {
	cols, err := r.Read()
	if err == io.EOF {
		return nil, err
	}
	if err != nil {
		return nil, errors.Wrap(err, "read row error")
	}

	var row sql.Row
	for _, c := range cols {
		row = append(row, c)
	}
	return row, nil
}

func (r *rowIter) Close() error {
	return r.Closer.Close()
}

type partitionIter struct{ done bool }

func (p *partitionIter) Close() error { return nil }

type partition struct{}

func (partition) Key() []byte { return []byte{'@'} }

func (p *partitionIter) Next() (sql.Partition, error) {
	if p.done {
		return nil, io.EOF
	}
	p.done = true
	return &partition{}, nil
}

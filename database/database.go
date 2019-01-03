package database

import (
	"github.com/pkg/errors"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"io"
)

type Builder func(database string, tb ...TableBuilder) (sql.Database, error)
type TableBuilder func() (table string, r io.ReadCloser, err error)

var builderRegister = map[string]Builder{}

func RegisterDatabaseBuilder(name string, builder Builder) {
	if _, ok := builderRegister[name]; ok {
		panic("duplicate builder:" + name)
	}

	builderRegister[name] = builder
}

func Build(builder, database string, tb ...TableBuilder) (sql.Database, error) {
	if _, ok := builderRegister[builder]; !ok {
		return nil, errors.Errorf("builder:%s, not register", builder)
	}

	b, err := builderRegister[builder](database, tb...)
	if err != nil {
		return nil, err
	}

	return b, nil
}

package sql_server

import (
	"bytes"
	"fmt"
	"github.com/MaxnSter/sql-parser/database"
	_ "github.com/MaxnSter/sql-parser/database/impl"
	"github.com/pkg/errors"
	"github.com/xenolf/lego/log"
	"gopkg.in/src-d/go-mysql-server.v0"
	"gopkg.in/src-d/go-mysql-server.v0/auth"
	"gopkg.in/src-d/go-mysql-server.v0/server"
	"gopkg.in/src-d/go-mysql-server.v0/sql"
	"gopkg.in/yaml.v2"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"
)

type Config struct {
	Dir      string `yaml:"dir"`
	FileType string `yaml:"fileType"`
	Database string `yaml:"database"`
}

type Configs struct {
	Addr string   `yaml:"addr"`
	Cs   []Config `yaml:"configs,flow"`
}

func Load(conf string) *Configs {
	cs := &Configs{}
	buf := &bytes.Buffer{}
	f, err := os.Open(conf)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	io.Copy(buf, f)

	if err := yaml.Unmarshal(buf.Bytes(), cs); err != nil {
		panic(err)
	}

	return cs
}

func Build(addr string, cs ...Config) {
	engine := sqle.NewDefault()
	for _, c := range cs {
		ts, err := parseDir(c.Dir, c.FileType)
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			continue
		}

		d, err := database.Build(c.FileType, c.Database, ts...)
		if err != nil {
			fmt.Fprint(os.Stderr, err)
			continue
		}

		engine.AddDatabase(d)
	}

	engine.AddDatabase(sql.NewInformationSchemaDatabase(engine.Catalog))
	if err := engine.Init(); err != nil {
		panic(err)
	}

	config := server.Config{
		Protocol: "tcp",
		Address:  addr,
		Auth:     &auth.None{},
	}
	s, err := server.NewDefaultServer(config, engine)
	if err != nil {
		panic(err)
	}
	log.Printf("listening at %s\n", addr)
	s.Start()
}

func parseDir(dir, fileType string) ([]database.TableBuilder, error) {
	var ts []database.TableBuilder

	fs, err := ioutil.ReadDir(dir)
	if err != nil {
		return nil, errors.Wrapf(err, "open dir:%s", dir)
	}

	for _, f := range fs {
		f := f
		if f.IsDir() || !strings.Contains(f.Name(), "."+fileType) {
			continue
		}

		ts = append(ts, func() (table string, r io.ReadCloser, err error) {
			table = strings.TrimSuffix(f.Name(), "."+fileType)
			file, err := os.Open(path.Join(dir, f.Name()))
			if err != nil {
				return "", nil, errors.Wrapf(err, "open file:%s", path.Join(dir, f.Name()))
			}

			r = file
			return
		})
	}

	return ts, nil
}

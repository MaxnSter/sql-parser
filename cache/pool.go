package cache

import (
	"bytes"
	"sync"
)

var p = sync.Pool{
	New: func() interface{} {
		return &bytes.Buffer{}
	},
}

func Get() *bytes.Buffer {
	b := p.Get().(*bytes.Buffer)
	b.Reset()
	return b
}

func Put(b *bytes.Buffer) {
	p.Put(b)
}

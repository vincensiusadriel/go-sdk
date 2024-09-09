package csvPartition

import (
	"encoding/csv"
	"os"
	"sync"
)

type CSVPartitionWriter struct {
	count  uint64
	maxRow uint64
	shard  uint64

	file   *os.File
	writer *csv.Writer

	fPath []string

	mut *sync.RWMutex
}

func New(fPath []string, maxRow uint64) (*CSVPartitionWriter, error) {
	c := &CSVPartitionWriter{
		maxRow: maxRow,
		count:  0,
		shard:  0,
		fPath:  fPath,
		mut:    &sync.RWMutex{},
	}
	if err := c.makefile(); err != nil {
		return nil, err
	}

	return c, nil

}

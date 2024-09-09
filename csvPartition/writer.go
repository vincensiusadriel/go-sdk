package csvPartition

import (
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
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

func NewWriter(fPath []string, maxRow uint64) (*CSVPartitionWriter, error) {
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

func (c *CSVPartitionWriter) Write(record []string) error {
	if c == nil {
		return errors.New("empty instance")
	}

	c.mut.Lock()
	defer c.mut.Unlock()

	if c.count >= c.maxRow && c.maxRow != 0 {
		if err := c.Close(); err != nil {
			return err
		}
		c.shard++
		if err := c.makefile(); err != nil {
			return err
		}
		c.count = 0
	}
	if c.writer == nil {
		return errors.New("empty writer")
	}

	c.writer.Write(record)
	c.count++

	return nil
}

func (c *CSVPartitionWriter) Close() error {
	if c == nil {
		return errors.New("empty instance")
	}

	if c.writer != nil {
		c.writer.Flush()
	}

	if c.file != nil {
		if err := c.file.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (c *CSVPartitionWriter) makefile() error {
	localPath, err := localpathGenerator(c.fPath, c.shard)
	if err != nil {
		return err
	}

	if err := os.MkdirAll(filepath.Dir(localPath), os.ModePerm); err != nil {
		return err
	}

	// Create the CSV file
	file, err := os.Create(localPath)
	if err != nil {
		return err
	}

	writer := csv.NewWriter(file)

	c.file = file
	c.writer = writer

	return nil
}

package csvPartition

import (
	"encoding/csv"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

func (c *CSVPartitionWriter) Write(record []string) error {
	if c == nil {
		return errors.New("empty instance")
	}

	c.mut.Lock()
	defer c.mut.Unlock()

	if c.count >= c.maxRow && c.maxRow != 0 {
		c.shard++
		if err := c.Close(); err != nil {
			return err
		}
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
	pathfile := c.fPath
	shard := c.shard

	if len(pathfile) == 0 {
		return errors.New("error makefile : empty path file")
	}

	filename := pathfile[len(pathfile)-1]
	if shard > 0 {
		filename = strings.TrimSuffix(filename, ".csv")
		filename = csvfilename(filename, shard)
	}

	pathfile[len(pathfile)-1] = filename

	localPath := filepath.Join(pathfile...)
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

package csvPartition

import (
	"encoding/csv"
	"errors"
	"os"
)

type CSVPatitionReader struct {
	fPath []string
	shard uint64

	file   *os.File
	reader *csv.Reader
}

func NewReader(fPath []string) (*CSVPatitionReader, error) {
	c := &CSVPatitionReader{
		fPath: fPath,
	}

	err := c.openfile()
	if err != nil {
		return nil, err
	}

	return c, nil
}

func (c *CSVPatitionReader) Read() ([]string, error) {
	if c == nil {
		return nil, errors.New("empty instance")
	}

	if c.reader == nil {
		return nil, errors.New("empty reader")
	}

	record, err := c.reader.Read()
	if err != nil && err.Error() == "EOF" {
		if err := c.Close(); err != nil {
			return nil, err
		}
		c.shard++
		if err := c.openfile(); err != nil {
			if os.IsNotExist(err) {
				return nil, errors.New("EOF")
			}
			return nil, err
		}
		record, err = c.reader.Read()
	} else if err != nil {
		return nil, err
	}

	return record, err
}

func (c *CSVPatitionReader) Close() error {
	if c == nil {
		return errors.New("empty instance")
	}

	if c.file != nil {
		if err := c.file.Close(); err != nil {
			return err
		}
	}

	return nil
}

func (c *CSVPatitionReader) openfile() error {
	localPath, err := localpathGenerator(c.fPath, c.shard)
	if err != nil {
		return err
	}

	file, err := os.Open(localPath)
	if err != nil {
		return err
	}

	reader := csv.NewReader(file)

	c.file = file
	c.reader = reader

	return nil
}

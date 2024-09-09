package csvPartition

import (
	"errors"
	"path/filepath"
	"strconv"
	"strings"
)

func csvfilename(filename string, shard uint64) string {
	if shard == 0 {
		return filename + ".csv"
	}
	return filename + "_" + strconv.FormatUint(shard, 10) + ".csv"
}

func localpathGenerator(fpath []string, shard uint64) (string, error) {

	pathfile := make([]string, len(fpath))
	copy(pathfile, fpath)

	if len(pathfile) == 0 {
		return "", errors.New("empty path file")
	}

	filename := pathfile[len(pathfile)-1]
	if shard > 0 {
		filename = strings.TrimSuffix(filename, ".csv")
		filename = csvfilename(filename, shard)
	}

	pathfile[len(pathfile)-1] = filename

	localPath := filepath.Join(pathfile...)

	return localPath, nil
}

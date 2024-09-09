package csvPartition

import (
	"strconv"
)

func csvfilename(filename string, shard uint64) string {
	if shard == 0 {
		return filename + ".csv"
	}
	return filename + "_" + strconv.FormatUint(shard, 10) + ".csv"
}

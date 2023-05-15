package io_test

import (
	"testing"

	"github.com/kstremick/mango/io"
)

func TestCsvToParquet(t *testing.T) {
	df, _ := io.ReadCsvFile("testdata/titanic.csv")
	io.WriteParquetFile(df, "testdata/titanic.parquet")
}

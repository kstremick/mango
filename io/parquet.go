package io

import (
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
	"github.com/kstremick/mango/core/dataframe"
	"github.com/kstremick/mango/core/series"
)

// BOUNDED_LEN is the maximum number of rows to read from a parquet file
// TODO: make this configurable, or have smarter defaults by looking into the file metadata first
const BOUNDED_LEN = 1_000_000_000

// ReadParquetFile reads a parquet file and returns a DataFrame
// using the arrow parquet reader
func ReadParquetFile(path string) (*dataframe.DataFrame, error) {
	rdr, err := file.OpenParquetFile(path, true)
	if err != nil {
		return nil, err
	}
	defer rdr.Close()

	metadata := rdr.MetaData()
	numColumns := metadata.Schema.NumColumns()
	columns := make([]series.Series, numColumns)
	for i := 0; i < numColumns; i++ {
		ser, err := readParquetColumn(*rdr, i)
		if err != nil {
			return nil, err
		}
		columns[i] = *ser
	}

	return dataframe.NewDataFrame(columns), nil
}

// Reads the column at colIdx from the parquet file
func readParquetColumn(rdr file.Reader, colIdx int) (*series.Series, error) {
	newColSchema := rdr.MetaData().Schema.Column(colIdx)
	colReader := pqarrow.ColumnReader{}
	chunked, err := colReader.BuildArray(BOUNDED_LEN)
	if err != nil {
		return nil, err
	}
	ret := series.NewSeriesFromChunked(newColSchema.Name(), chunked)
	return &ret, nil
}

// WriteParquetFile writes a DataFrame to a parquet file
// using the arrow parquet writer
func WriteParquetFile(df *dataframe.DataFrame, path string) error {
	panic("not implemented")
}

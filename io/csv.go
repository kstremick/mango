package io

import (
	rawcsv "encoding/csv"
	"io"
	"os"
	"strings"

	"github.com/kstremick/mango/core/dataframe"
	"github.com/kstremick/mango/core/series"
)

// ReadCsv reads a CSV file from an io.Reader and returns a DataFrame
func ReadCsv(input io.Reader) (*dataframe.DataFrame, error) {
	var df dataframe.DataFrame

	lines, err := rawcsv.NewReader(input).ReadAll()
	if err != nil {
		return &df, err
	}
	names := lines[0]
	var data [][]interface{} = make([][]interface{}, len(names))
	for _, line := range lines[1:] {
		for i, v := range line {
			data[i] = append(data[i], v)
		}
	}
	seriesSlice := make([]series.Series, len(names))
	for i, name := range names {
		seriesSlice[i] = series.NewSeriesFromSlice(name, data[i], nil, true)
	}
	return dataframe.NewDataFrame(seriesSlice), nil
}

// ReadCsvFile reads a CSV file from a path and returns a DataFrame
func ReadCsvFile(path string) (*dataframe.DataFrame, error) {
	file, err := os.Open(path)
	if err != nil {
		return &dataframe.DataFrame{}, err
	}
	defer file.Close()
	return ReadCsv(file)
}

// ReadCsvString reads a CSV string and returns a DataFrame
func ReadCsvString(s string) (*dataframe.DataFrame, error) {
	return ReadCsv(strings.NewReader(s))
}

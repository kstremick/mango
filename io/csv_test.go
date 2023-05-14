package io_test

import (
	"testing"

	"github.com/kstremick/mango/core/dataframe"
	"github.com/kstremick/mango/core/series"
	"github.com/kstremick/mango/io"

	"github.com/zeebo/assert"
)

func TestCsvTypeInference(t *testing.T) {
	type testCase struct {
		name     string
		csvData  string
		expected []series.Series
	}
	testCases := []testCase{
		{
			name: "int",
			csvData: `col1,col2,col3
1,2,3
4,5,6
7,8,9`,
			expected: []series.Series{
				series.NewSeries("col1", []int64{1, 4, 7}),
				series.NewSeries("col2", []int64{2, 5, 8}),
				series.NewSeries("col3", []int64{3, 6, 9}),
			},
		},
		{
			name: "float",
			csvData: `col1,col2,col3
1.1,2.2,3.3
4.4,5.5,6.6
7.7,8.8,9.9`,
			expected: []series.Series{
				series.NewSeries("col1", []float64{1.1, 4.4, 7.7}),
				series.NewSeries("col2", []float64{2.2, 5.5, 8.8}),
				series.NewSeries("col3", []float64{3.3, 6.6, 9.9}),
			},
		},
		{
			name: "string",
			csvData: `col1,col2,col3
1,2,3
a,b,c
true,false,8.4`,
			expected: []series.Series{
				series.NewSeries("col1", []string{"1", "a", "true"}),
				series.NewSeries("col2", []string{"2", "b", "false"}),
				series.NewSeries("col3", []string{"3", "c", "8.4"}),
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			df, err := io.ReadCsvString(tc.csvData)
			if err != nil {
				t.Error(err)
			}
			expected := dataframe.NewDataFrame(tc.expected)
			assert.Equal(t, df.String(), expected.String())
		})
	}
}

func TestParseSeries(t *testing.T) {
	type testCase struct {
		name     string
		data     []string
		expected series.Series
	}
	testCases := []testCase{
		{
			name:     "int",
			data:     []string{"1", "2", "3"},
			expected: series.NewSeries("col1", []int64{1, 2, 3}),
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			s := series.NewSeriesFromSlice("col1", tc.data, nil, true)
			df := dataframe.NewDataFrame([]series.Series{s})
			dfExpected := dataframe.NewDataFrame([]series.Series{tc.expected})
			assert.Equal(t, df.String(), dfExpected.String())
		})
	}
}

func TestReadCsv(t *testing.T) {
	csvData := `col1,col2,col3
1,a,1.1
2,b,2.2
3,c,3.3`

	df, err := io.ReadCsvString(csvData)
	if err != nil {
		t.Error(err)
	}
	expectedSeries := []series.Series{
		series.NewSeries("col1", []int64{1, 2, 3}),
		series.NewSeries("col2", []string{"a", "b", "c"}),
		series.NewSeries("col3", []float64{1.1, 2.2, 3.3}),
	}
	expected := dataframe.NewDataFrame(expectedSeries)

	assert.DeepEqual(t, df.String(), expected.String())
}

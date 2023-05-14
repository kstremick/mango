package dataframe

import (
	"errors"
	"mango/core/primitive"
	"mango/core/series"
)

// DataFrame is a collection of Series
type DataFrame struct {
	Series []series.Series
}

// NewDataFrame creates a new DataFrame from a slice of Series
func NewDataFrame(s []series.Series) *DataFrame {
	return &DataFrame{s}
}

// GetColumns returns the columns of the DataFrame
func (df *DataFrame) GetColumns() []series.Series {
	return df.Series
}

// GetColumnNames returns the column names of the DataFrame
func (df *DataFrame) GetColumnNames() []string {
	names := make([]string, len(df.Series))
	for i, s := range df.Series {
		names[i] = s.Name
	}
	return names
}

// Select columns from this DataFrame.
func (df *DataFrame) Select(colNames ...string) (*DataFrame, error) {
	series := make([]series.Series, len(colNames))
	for i, name := range colNames {
		found := false
		for _, s := range df.Series {
			if s.Name == name {
				series[i] = s
				found = true
				break
			}
		}
		if !found {
			return nil, errors.New("column not found: " + name)
		}
	}
	return NewDataFrame(series), nil
}

type ApplyFunc func(row map[string]primitive.Optional[interface{}]) interface{}
type ApplyFuncErr func(row map[string]primitive.Optional[interface{}]) (interface{}, error)

// Height returns the number of rows in the DataFrame
func (df *DataFrame) Height() int {
	if len(df.Series) == 0 {
		return 0
	}
	return df.Series[0].Len()
}

// RowSlice returns a row of the DataFrame as a slice of interface{}
func (df *DataFrame) RowSlice(i int) []interface{} {
	row := make([]interface{}, len(df.Series))
	for j, s := range df.Series {
		row[j] = s.ValueExn(i)
	}
	return row
}

// Row returns the row as a map from column name to value
func (df *DataFrame) Row(i int) map[string]primitive.Optional[interface{}] {
	row := make(map[string]primitive.Optional[interface{}])
	for _, s := range df.Series {
		row[s.Name] = s.ValueExn(i)
	}
	return row
}

// ApplyErr applies a custom/user-defined function (UDF) over the rows of the DataFrame.
// If any row returns an error, the overall function will return an error
func (df *DataFrame) ApplyErr(fn ApplyFuncErr) (*series.Series, error) {
	nRows := df.Height()
	resultData := make([]primitive.Optional[interface{}], nRows)

	for i := 0; i < nRows; i++ {
		val, err := fn(df.Row(i))
		if err != nil {
			return nil, err
		}
		resultData[i] = primitive.Some(val)
	}
	ser := series.NewSeries("", resultData)
	return &ser, nil
}

// Apply applies a custom/user-defined function (UDF) over the rows of the DataFrame.m
func (df *DataFrame) Apply(fn ApplyFunc) *series.Series {
	wrappedFunc := func(row map[string]primitive.Optional[interface{}]) (interface{}, error) {
		return fn(row), nil
	}

	ser, err := df.ApplyErr(wrappedFunc)
	if err != nil {
		panic(err)
	}

	return ser
}

// With_columns adds clumns to the dataframe.
// Added columns will replace existing columns with the same name.
func (df *DataFrame) WithColumns(s ...*series.Series) *DataFrame {
	for _, ser := range s {
		found := false
		for i, s := range df.Series {
			if s.Name == ser.Name {
				df.Series[i] = *ser
				found = true
				break
			}
		}
		if !found {
			df.Series = append(df.Series, *ser)
		}
	}
	return df
}

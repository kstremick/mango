package dataframe_test

import (
	"testing"

	"github.com/kstremick/mango/core/dataframe"
	"github.com/kstremick/mango/core/primitive"
	"github.com/kstremick/mango/core/series"

	"github.com/zeebo/assert"
)

func TestSelect(t *testing.T) {
	series := []series.Series{
		series.NewSeries("PassengerId", []int64{1, 2, 3}),
		series.NewSeries("Survived", []bool{false, true, true}),
		series.NewSeries("Pclass", []int64{3, 1, 3}),
		series.NewSeries("Name", []string{"John", "James", "Hannnah"}),
	}
	df := &dataframe.DataFrame{
		Series: series,
	}

	cols := []string{
		"PassengerId",
		"Survived",
		"Pclass",
		"Name",
	}

	df, err := df.Select(cols...)
	if err != nil {
		t.Error(err)
	}
	if df == nil {
		t.Errorf("Expected dataframe to not be nil")
	}
	assert.Equal(t, df.GetColumnNames(), cols)

	// Test that an error is returned if a column is not found
	nilDf, err := df.Select("PassengerId", "Survived", "Pclass", "Name", "Age")
	if err == nil {
		t.Errorf("Expected error to be returned")
	}

	if nilDf != nil {
		t.Errorf("Expected dataframe to be nil")
	}
	doubleFunc := func(row map[string]primitive.Optional[interface{}]) interface{} {
		return row["PassengerId"].Value.(int64) * 2
	}
	doubleSeries := df.Apply(doubleFunc)
	doubleSeries.Rename("PassengerIdDoubled")
	df = df.WithColumns(doubleSeries)

	assert.Equal(t, df.GetColumnNames(), []string{"PassengerId", "Survived", "Pclass", "Name", "PassengerIdDoubled"})
}

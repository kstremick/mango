package series_test

import (
	"fmt"
	"mango/core/primitive"
	"mango/core/series"
	"testing"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/zeebo/assert"
)

// helper function to create a float64 array with nulls

func TestArray(t *testing.T) {
	pool := memory.NewGoAllocator()

	b := array.NewFloat64Builder(pool)
	defer b.Release()

	b.AppendValues(
		[]float64{1, 2, 3, 1, 4, 5},
		[]bool{true, true, true, false, true, true},
	)
	b.AppendNull()

	arr := b.NewFloat64Array()
	defer arr.Release()

	fmt.Printf("array = %v\n", arr)

	for i := 0; i < 7; i++ {
		fmt.Printf("array[%d] = %v\n", i, arr.Value(i))
	}

	sli := array.NewSlice(arr, 2, 5).(*array.Float64)
	defer sli.Release()

	fmt.Printf("slice = %v\n", sli)

}

func TestNewSeriesFromArray(t *testing.T) {
	pool := memory.NewGoAllocator()
	builder := array.NewFloat64Builder(pool)
	defer builder.Release()

	values := []float64{1.0, 2.0, 3.0, 0.0, 4.0}
	valids := []bool{true, true, true, false, true}
	builder.AppendValues(values, valids)
	arr := builder.NewFloat64Array()

	ser := series.NewSeriesFromArray("test", arr)

	assert.Equal(t, "test", ser.Name)
	assert.Equal(t, 5, ser.Len())
	assert.Equal(t, 1, ser.NumChunks())
	assert.Equal(t, 1, ser.NullN())
	assert.Equal(t, arrow.FLOAT64, ser.DataType().ID())

	v := ser.ValueExn(2)
	assert.Equal(t, 3.0, v.Value)
	assert.Equal(t, true, v.Valid)

	v = ser.ValueExn(3)
	assert.Equal(t, nil, v.Value)
	assert.Equal(t, false, v.Valid)

	seriesFromSeries := series.NewSeries("test", arr)
	assert.DeepEqual(t, ser, seriesFromSeries)
}

func TestNewSeriesFromSlice(t *testing.T) {
	values := []interface{}{1.0, 2.0, 3.0}
	valid := []bool{true, true, true}

	ser := series.NewSeriesFromSlice("test", values, valid, false)

	assert.Equal(t, "test", ser.Name)
	assert.Equal(t, arrow.FLOAT64, ser.DataType().ID())
	assert.Equal(t, 1, ser.NumChunks())
	assert.Equal(t, 3, ser.Len())

	seriesFromSeries := series.NewSeries("test", values)
	assert.DeepEqual(t, ser, seriesFromSeries)
}

func TestRename(t *testing.T) {
	ser := series.NewSeries("test", []interface{}{1.0, 2.0, 3.0})
	ser.Rename("test2")
	assert.Equal(t, "test2", ser.Name)
}

func TestAlias(t *testing.T) {
	ser := series.NewSeries("test", []interface{}{1, 2, 3})
	expected := series.NewSeries("test2", []interface{}{1, 2, 3})
	assert.Equal(t, expected, ser.Alias("test2"))
}

func TestNulls(t *testing.T) {
	ser := series.NewSeries("test", []interface{}{1.0, primitive.Null{}, 3.0, primitive.Null{}})
	assert.Equal(t, 2, ser.NullN())
	assert.Equal(t, []bool{false, true, false, true}, ser.IsNull())
	assert.Equal(t, []bool{true, false, true, false}, ser.IsNotNull())
	assert.Equal(t, true, ser.IsValidExn(0))
	assert.Equal(t, false, ser.IsValidExn(1))
}

func TestSeriesFromValue(t *testing.T) {
	ser := series.NewSeries("test", "hello")
	assert.Equal(t, "test", ser.Name)
	assert.Equal(t, 1, ser.Len())
	assert.Equal(t, "hello", ser.ValueExn(0).Value)
}

func TestSlice(t *testing.T) {
	ser := series.NewSeries("test", []interface{}{1.0, 2.0, 3.0, 4.0, 5.0})
	sli, err := ser.Slice(1, 3)
	if err != nil {
		t.Error(err)
	}

	assert.Equal(t, 3, sli.Len())
	assert.Equal(t, 2.0, sli.ValueExn(0).Value)
	assert.Equal(t, 3.0, sli.ValueExn(1).Value)
	assert.Equal(t, 4.0, sli.ValueExn(2).Value)

	negSli, _ := ser.Slice(-1, 1)
	assert.Equal(t, 1, negSli.Len())
	assert.Equal(t, 5.0, negSli.ValueExn(0).Value)

	_, err = ser.Slice(0, 10)
	assert.Error(t, err)

	_, err = ser.Slice(10, 2)
	assert.Error(t, err)

	_, err = ser.Slice(-2, 3)
	assert.Error(t, err)
}

func TestAllAndAny(t *testing.T) {
	ser := series.NewSeries("test", []interface{}{true, false, true})
	res, err := ser.All()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, res, false)

	res, err = ser.Any()
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, res, true)

	ser = series.NewSeries("test", []interface{}{1.0, 0.0})
	res, err = ser.All()
	assert.Equal(t, res, false)
	if err == nil {
		t.Error("expected error for All on non-bool series")
	}

	ser = series.NewSeries("test", []interface{}{1.0, 0.0})
	res, err = ser.Any()
	if err == nil {
		t.Error("expected error for Any on non-bool series")
	}

	assert.Equal(t, res, false)
}

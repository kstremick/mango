package series

// Inspired by https://pola-rs.github.io/polars/polars/series/trait.SeriesTrait.html

import (
	"fmt"
	"mango/core/chunked"
	"mango/core/primitive"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/memory"
)

type InterfaceBase struct {
	Name   string
	Rename (string)
}

type Series struct {
	Name string
	ca   *arrow.Chunked
}

// NewSeries creates a new Series from a chunked array.
func NewSeriesFromArray(name string, arr arrow.Array) Series {
	return Series{
		Name: name,
		ca:   arrow.NewChunked(arr.DataType(), []arrow.Array{arr}),
	}
}

// NewSeriesFromSlice creates a new Series from a slice of interface{}.
// The valid slice determines which values in v are valid (not null).
// The valid slice must either be empty or be equal in length to v.
// If empty, all values in v are appended and considered valid.
// If inferTypes is true, the type of the series will be inferred from the values in v.
// Mango will attempt to parse your values -- for example, ["1", "2", "3"] will
// be parsed into a series of type int64 if inferTypes is true.
func NewSeriesFromSlice[T any](name string, valsAsT []T, valid []bool, inferTypes bool) Series {
	memory := memory.NewGoAllocator()
	var ret arrow.Array
	var typeToConvertTo arrow.DataType
	var err error

	vals := make([]interface{}, len(valsAsT))
	for i, v := range valsAsT {
		vals[i] = v
	}

	if !inferTypes {
		typeToConvertTo, err = primitive.ExtractDatatype(vals)
	} else {
		typeToConvertTo, err = primitive.InferDatatype(vals)
	}
	if err != nil {
		panic(err)
	}

	switch typeToConvertTo.ID() {
	case arrow.STRING:
		b := array.NewStringBuilder(memory)
		defer b.Release()
		b.AppendValues(primitive.CastListT[string](vals, valid))
		ret = b.NewStringArray()
	case arrow.FLOAT64:
		b := array.NewFloat64Builder(memory)
		defer b.Release()
		b.AppendValues(primitive.CastListT[float64](vals, valid))
		ret = b.NewFloat64Array()
	case arrow.BOOL:
		b := array.NewBooleanBuilder(memory)
		defer b.Release()
		b.AppendValues(primitive.CastListT[bool](vals, valid))
		ret = b.NewBooleanArray()
	case arrow.INT64:
		b := array.NewInt64Builder(memory)
		defer b.Release()
		b.AppendValues(primitive.CastListT[int64](vals, valid))
		ret = b.NewInt64Array()

	}
	return NewSeriesFromArray(name, ret)
}

// NewSeriesFromValue creates a new Series from a single value.
func NewSeriesFromValue(name string, val interface{}) Series {
	return NewSeriesFromSlice(name, []interface{}{val}, nil, false)
}

func NewSeriesFromArrayT[T any](name string, arr []T) Series {
	genericArr := make([]interface{}, len(arr))
	for i, v := range arr {
		genericArr[i] = v
	}
	return NewSeriesFromSlice(name, genericArr, nil, false)
}

// NewSeries creates a new series from arbitrary data, matching on the structure of that data.
// Matches slice, array, and single value.
func NewSeries(name string, data interface{}) Series {
	switch data := data.(type) {
	case []primitive.Optional[interface{}]:
		return NewSeriesFromSlice(name, data, nil, true)
	case []interface{}:
		return NewSeriesFromSlice(name, data, nil, true)
	case []int64:
		return NewSeriesFromArrayT(name, data)
	case []float64:
		return NewSeriesFromArrayT(name, data)
	case []bool:
		return NewSeriesFromArrayT(name, data)
	case []string:
		return NewSeriesFromArrayT(name, data)
	case arrow.Array:
		return NewSeriesFromArray(name, data)
	default:
		return NewSeriesFromValue(name, data)
	}
}

// Rename renames the series.
func (s *Series) Rename(newName string) {
	s.Name = newName
}

// ChunkLengths returns the lengths of the underlying chunks.
func (s *Series) ChunkLengths() []int {
	chunks := s.ca.Chunks()

	ret := make([]int, len(chunks))
	for i, chunk := range chunks {
		ret[i] = chunk.Len()
	}
	return ret
}

// Chunks returns the underlying chunks of the Series.
func (s *Series) Chunks() []arrow.Array {
	return s.ca.Chunks()
}

// Alias returns a new Series with the same data, but a different name.
func (s Series) Alias(name string) Series {
	return Series{
		Name: name,
		ca:   s.ca,
	}
}

// Copy returns a cheap copy of the Series.
func (s *Series) Copy() Series {
	return Series{
		Name: s.Name,
		ca:   s.ca,
	}
}

// Slice returns a zero-copy slice of the Series.
// When offset is negative the offset is counted from
// The end of the Series.
func (s *Series) Slice(offset, length int64) (Series, error) {
	if offset < 0 {
		offset = offset + int64(s.ca.Len())
	}
	if length > int64(s.ca.Len()) {
		return Series{}, fmt.Errorf("length %d is greater than the length of the Series %d", length, s.ca.Len())
	}
	if length+offset > int64(s.ca.Len()) {
		return Series{}, fmt.Errorf("length + offset %d is greater than the length of the Series %d", length+offset, s.ca.Len())
	}
	chunkSlice := array.NewChunkedSlice(s.ca, offset, offset+length)
	return Series{Name: s.Name, ca: (chunkSlice)}, nil
}

// Filter filters by boolean mask. This operation clones data.
func (s *Series) Filter(mask *SeriesT[bool]) (Series, error) {
	if mask.Len() != s.ca.Len() {
		return Series{}, fmt.Errorf("length of mask %d is not equal to the length of the Series %d", mask.Len(), s.ca.Len())
	}
	if mask.DataType().ID() != arrow.BOOL {
		return Series{}, fmt.Errorf("mask is not of type bool")
	}
	rets := make([]interface{}, mask.Len())
	retsI := 0
	for i := 0; i < mask.Len(); i++ {
		isValid, err := mask.Value(i)
		if err != nil {
			return Series{}, err
		}
		// if v is Nill, value should be the default value, which is false.
		if isValid.Value {
			v, e := s.Value(i)
			if e != nil {
				return Series{}, err
			}
			rets[retsI] = v.Value
			retsI++
		}
	}
	return NewSeriesFromSlice(s.Name, rets, nil, false), nil
}

// Take by index. This operation copies the data.
func (s *Series) Take(indices *SeriesT[int64]) (Series, error) {
	if indices.Len() > s.ca.Len() {
		return Series{}, fmt.Errorf("length of indices %d is greater than the length of the Series %d", indices.Len(), s.ca.Len())
	}

	rets := make([]interface{}, indices.Len())
	valids := make([]bool, indices.Len())
	for i := 0; i < indices.Len(); i++ {
		optIndex, _ := indices.Value(i)
		if optIndex.Valid {
			value, isValid, err := s.ValueUnpacked(int(optIndex.Value))
			if err != nil {
				return Series{}, err
			}
			rets[i] = value
			valids[i] = isValid
		}
	}
	return NewSeriesFromSlice(s.Name, rets, valids, false), nil
}

// Len returns the length of the Series.
func (s *Series) Len() int {
	return s.ca.Len()
}

// NumChunks returns the number of chunks.
func (s *Series) NumChunks() int {
	return len(s.ca.Chunks())
}

// DataType returns the arrow data type of the Series.
func (s *Series) DataType() arrow.DataType {
	return s.ca.DataType()
}

// Type returns the arrow type of the Series.
func (s *Series) Type() arrow.Type {
	return s.ca.DataType().ID()
}

// Rechunk aggregates all chunks to a contiguous array of memory.
func (s *Series) Rechunk() {
	if s.NumChunks() <= 1 {
		return
	}
	rets := make([]interface{}, 0, s.ca.Len())
	valids := make([]bool, 0, s.ca.Len())
	for _, chunk := range s.Chunks() {
		chunkVals, chunkValids, err := chunked.ExtractChunk[string](chunk)
		if err != nil {
			panic(err)
		}
		rets = append(rets, any(chunkVals).([]interface{})...)
		valids = append(valids, chunkValids...)
	}
	newSeries := NewSeriesFromSlice(s.Name, rets, valids, false)
	s.ca.Release()
	s.ca = newSeries.ca
}

// TakeEvery takes every nth element as a new Series.
func (s *Series) TakeEvery(n int) Series {
	if n == 1 {
		return *s
	}
	bools := make([]bool, s.Len())
	for i := 0; i < s.Len(); i += n {
		bools[i] = true
	}
	ser := NewSeriesTFromTSlice("", bools, nil)
	ret, err := s.Filter(&ser)
	if err != nil {
		panic(err)
	}
	return ret
}

// ResolveIndex returns the index of the chunk, and the index within that chunk.
func (s *Series) ResolveIndex(i int) (int, int) {
	chunkLengths := s.ChunkLengths()
	chunkIndex := 0
	for i >= chunkLengths[chunkIndex] {
		if i-chunkLengths[chunkIndex] < 0 {
			break
		}
		i -= chunkLengths[chunkIndex]
		chunkIndex++
	}
	return chunkIndex, i
}

// valueExnHelper returns the value at index i, wrapped in a primitive.Optional
// It does not returns errors -- it panics on them
// See ValueExn for the function that returns the same thing, typed as primitive.Optional.
func (s *Series) valueExnHelper(i int) interface{} {
	chunkIndex, i := s.ResolveIndex(i)

	chunk := s.Chunks()[chunkIndex]
	if chunk.IsNull(i) {
		return primitive.None[interface{}]()
	}
	extractValueFn, err := chunked.ExtractValueFn(chunk)
	if err != nil {
		panic(err)
	}
	return primitive.Some(extractValueFn(i))
}

func (s *Series) ValueExn(i int) primitive.Optional[interface{}] {
	v := s.valueExnHelper(i)
	return v.(primitive.Optional[interface{}])
}

// Value returns the value at index i, wrapped in a primitive.Optional
// If the value is null, the second return value is false.
func (s *Series) Value(i int) (primitive.Optional[interface{}], error) {
	if i >= s.Len() {
		return primitive.None[interface{}](), fmt.Errorf("index out of range")
	}
	return s.ValueExn(i), nil
}

// ValueUnpacked returns the value at index i, without wrapping it in a primitive.Optional
// The second return value is true if the value is null.
// The third return value represents errors
func (s *Series) ValueUnpacked(i int) (interface{}, bool, error) {
	v, err := s.Value(i)
	if err != nil {
		return nil, false, err
	}
	if !v.Valid {
		return nil, true, nil
	}
	return v.Value, false, nil
}

// Head returns the first n elements of the Series, wrapped in a primitive.Optional.
// The third element is an error if the user's request is invalid or misformatted.
func (s *Series) Head(n int) ([]interface{}, error) {
	if n > s.ca.Len() {
		return nil, fmt.Errorf("n %d is greater than the length of the Series %d", n, s.ca.Len())
	}
	ret := make([]interface{}, n)
	for i := 0; i < n; i++ {
		// Don't check errors, because we already checked the length
		val, _ := s.Value(i)
		ret[i] = val
	}
	return ret, nil
}

// Tail returns the last n elements of the Series.
func (s *Series) Tail(n int) ([]interface{}, []bool, error) {
	if n > s.ca.Len() {
		return nil, nil, fmt.Errorf("n %d is greater than the length of the Series %d", n, s.ca.Len())
	}
	ret := make([]interface{}, n)
	nulls := make([]bool, n)
	for i := 0; i < n; i++ {
		// Don't check errors, because we already checked the length
		val, _ := s.Value(i)
		ret[i] = val
	}
	return ret, nulls, nil
}

// IsValid returns true if the value at index i is not null.
func (s *Series) IsValid(i int) (bool, error) {
	if i > s.Len() {
		return false, fmt.Errorf("index out of range")
	}
	chunkIndex, i := s.ResolveIndex(i)
	return s.Chunks()[chunkIndex].IsValid(i), nil
}

// IsValidExn returns true if the value at index i is not null.
// It panics on errors.
func (s *Series) IsValidExn(i int) bool {
	val, err := s.IsValid(i)
	if err != nil {
		panic(err)
	}
	return val
}

// IsNull returns a bool array indicating if the value at index i is null.
func (s *Series) IsNull() []bool {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = !s.IsValidExn(i)
	}
	return ret
}

// IsNotNull returns a bool array indicating if the value at index i is not null.
func (s *Series) IsNotNull() []bool {
	ret := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		ret[i] = s.IsValidExn(i)
	}
	return ret
}

// NewSeriesFromBools creates a new Series from a bool slice.
// func NewSeriesFromBool(name string, data []bool) Series {
// 	pool := memory.NewGoAllocator()
// 	b := array.NewBooleanBuilder(pool)
// 	defer b.Release()

// 	b.AppendValues(data, nil)
// 	arr := b.NewArray()
// 	return NewSeriesFromArray(name, arr)
// }

// NewSeriesFromBools creates a new SeriesT from a bool slice.
// func NewSeriesTFromBool(name string, data []bool) SeriesT[bool] {
// 	ser := NewSeriesFromBool(name, data)
// 	return SeriesT[bool]{Series: ser}
// }

// NewSeriesFromFloat64 creates a new Series from a float64 slice.
// func NewSeriesFromFloat64(name string, data []float64) Series {
// 	pool := memory.NewGoAllocator()
// 	b := array.NewFloat64Builder(pool)
// 	defer b.Release()

// 	b.AppendValues(data, nil)
// 	arr := b.NewArray()
// 	return Series{Name: name, ca: array.NewChunked(arr.DataType(), []array.Interface{arr})}
// }

// NullN returns the number of nulls.
func (s Series) NullN() int {
	return s.ca.NullN()
}

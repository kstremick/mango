package chunked

import (
	"fmt"

	"github.com/kstremick/mango/core/primitive"

	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
)

// import (
// 	"github.com/apache/arrow/go/arrow"
// 	"github.com/apache/arrow/go/arrow/array"
// )

// // Chunked manages a collection of primitives arrays as one logical large array.
// type Chunked struct {
// 	array.Chunked
// }

// func NewChunked(chunked array.Chunked) *Chunked {
// 	return &Chunked{chunked}
// }

// func (c *Chunked) NewSlice(i, j int64) *Chunked {
// 	slice := c.Chunked.NewSlice(i, j)
// 	return NewChunked(*slice)
// }

// func (ca Chunked) Value(i int) interface{} {
// 	chunks := ca.Chunks()
// 	switch ca.DataType().ID() {
// 	case arrow.BinaryTypes.String.ID():
// 		return chunk.(*array.String).Value(i)
// 	case arrow.PrimitiveTypes.Float64.ID():
// 		return chunk.(*array.Float64).Value(i)
// 	}
// 	return nil
// }

// StringChunk converts the array to string,
// Returning the validity array
func StringChunk(s arrow.Array) ([]string, []bool, error) {
	if !arrow.TypeEqual(s.DataType(), arrow.BinaryTypes.String) {
		return nil, nil, fmt.Errorf("series is not of type string")
	}
	ret := make([]string, s.Len())
	valids := make([]bool, s.Len())
	for i := 0; i < s.Len(); i++ {
		if s.IsValid(i) {
			ret[i] = s.(*array.String).Value(i)
			valids[i] = true
		} else {
			valids[i] = false
		}
	}
	return ret, valids, nil
}

// ExtractVaueFn returns a function that extracts the value at index i.
func ExtractValueFnT[T primitive.Primitive](s arrow.Array) (func(int) T, error) {
	desiredType := primitive.ToArrowDatatypeT[T]()
	if !arrow.TypeEqual(s.DataType(), desiredType, nil) {
		return nil, fmt.Errorf("series is not of type %T", primitive.ToArrowDatatypeT[T]())
	}
	switch s.DataType().ID() {
	case arrow.STRING:
		return func(i int) T {
			return any(s.(*array.String).Value(i)).(T)
		}, nil
	case arrow.FLOAT64:
		return func(i int) T {
			return any(s.(*array.Float64).Value(i)).(T)
		}, nil
	case arrow.BOOL:
		return func(i int) T {
			return any(s.(*array.Boolean).Value(i)).(T)
		}, nil
	case arrow.INT64:
		return func(i int) T {
			return any(s.(*array.Int64).Value(i)).(T)
		}, nil
	}
	return nil, fmt.Errorf("series is not of type %T", primitive.ToArrowDatatypeT[T]())
}

// ExtractValueFn returns a function that extracts the value at index i.
func ExtractValueFn(s arrow.Array) (func(int) interface{}, error) {
	switch s.DataType().ID() {
	case arrow.STRING:
		return func(i int) interface{} {
			return s.(*array.String).Value(i)
		}, nil
	case arrow.FLOAT64:
		return func(i int) interface{} {
			return s.(*array.Float64).Value(i)
		}, nil
	case arrow.BOOL:
		return func(i int) interface{} {
			return s.(*array.Boolean).Value(i)
		}, nil
	case arrow.INT64:
		return func(i int) interface{} {
			return s.(*array.Int64).Value(i)
		}, nil
	}
	return nil, fmt.Errorf("unknown series type %T", s.DataType())
}

// ExtractChunk converts the chunk to an array of type T.
// It also returns a validity array, and errors.
func ExtractChunk[T primitive.Primitive](s arrow.Array) ([]T, []bool, error) {
	ret := make([]T, s.Len())
	valids := make([]bool, s.Len())
	extractValue, err := ExtractValueFnT[T](s)
	if err != nil {
		return nil, nil, err
	}
	for i := 0; i < s.Len(); i++ {
		if s.IsValid(i) {
			ret[i] = extractValue(i)
			valids[i] = true
		} else {
			valids[i] = false
		}
	}
	return ret, valids, nil
}

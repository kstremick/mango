package series

import (
	"fmt"
	"mango/core/primitive"

	"github.com/apache/arrow/go/v11/arrow"
)

type SeriesT[T primitive.Primitive] struct {
	Series
}

func NewSeriesTFromArray[T primitive.Primitive](name string, arr arrow.Array) SeriesT[T] {
	return SeriesT[T]{
		Series: NewSeriesFromArray(name, arr),
	}
}

// NewSeriesTFromTSlice creates a new Series from a slice of interface{}.
// The valid slice determines which values in v are valid (not null).
// The valid slice must either be empty or be equal in length to v.
// If empty, all values in v are appended and considered valid.
func NewSeriesTFromTSlice[T primitive.Primitive](name string, vals []T, valid []bool) SeriesT[T] {
	series := NewSeriesFromSlice(name, any(vals).([]interface{}), valid, false)
	seriesT := SeriesT[T]{Series: series}
	err := seriesT.Validate()
	if err != nil {
		// We passed a []T so this should be impossible
		panic(err)
	}
	return seriesT
}

func NewSeriesTFromT[T primitive.Primitive](name string, val T) SeriesT[T] {
	return NewSeriesTFromTSlice(name, []T{val}, nil)
}

// Validate checks that the type T corresponds to the underying data.
// Returns an error if this is not true.
// Make sure to call this when you instantiate a new SeriesT.
func (s *SeriesT[T]) Validate() error {
	datatype := primitive.ToArrowDatatypeT[T]()
	if s.ca.DataType().ID() != datatype.ID() {
		return fmt.Errorf("the underlying datatype is not %s", datatype.Name())
	}
	return nil
}

// Value returns the value at index i as type T, wrapped in a primitive.Optional.
// Returns an error if this is not possible.
func (s *SeriesT[T]) Value(i int) (primitive.Optional[T], error) {
	v, err := s.Series.Value(i)
	if err != nil {
		return primitive.None[T](), err
	}
	return any(v).(primitive.Optional[T]), nil
}

// Len returns the length of the Series.
func (s *SeriesT[T]) Len() int {
	return s.Series.Len()
}

// Filter returns a new SeriesT[T] with only the values where the mask is true.
// func (s *SeriesT[T]) Filter(mask *SeriesT[bool]) (SeriesT[T], error) {
// 	rets := make([]T, mask.Len())
// 	retsI := 0
// 	for i := 0; i < mask.Len(); i++ {
// 		v, err := mask.Value(i)
// 		if err != nil {
// 			return SeriesT[T]{}, err
// 		}
// 		// if v is Nill, value should be the default value, which is false.
// 		if v.Value {
// 			v, e := s.Value(i)
// 			if e != nil {
// 				return SeriesT[T]{}, err
// 			}
// 			rets[retsI] = v.Value
// 			retsI++
// 		}
// 	}
// 	return NewSeriesTFromTSlice[T](s.Name, rets, nil), nil
// }

// AsFloat64 returns the Series as a typed Series of type float64.
// Returns an error if this is not possible.
// func (s *Series) AsFloat64() (SeriesT[float64], error) {
// 	ret := SeriesT[float64]{Name: s.Name, ca: s.ca}
// 	err := ret.Validate()
// 	if err != nil {
// 		return SeriesT[float64]{}, err
// 	}
// 	return ret, nil
// }

// AsString returns the Series as a typed Series of type string.
// Returns an error if this is not possible.
// func (s *Series) AsString() (SeriesT[string], error) {
// 	ret := SeriesT[string]{Name: s.Name, ca: s.ca}
// 	err := ret.Validate()
// 	if err != nil {
// 		return SeriesT[string]{}, err
// 	}
// 	return ret, nil
// }

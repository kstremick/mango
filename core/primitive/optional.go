package primitive

import "fmt"

type Optional[T any] struct {
	Value T
	Valid bool
}

func Some[T any](val T) Optional[T] {
	return Optional[T]{Value: val, Valid: true}
}

func None[T any]() Optional[T] {
	return Optional[T]{Valid: false}
}

func (o Optional[T]) String() string {
	if o.Valid {
		return fmt.Sprintf("%v", o.Value)
	}
	return "Null"
}

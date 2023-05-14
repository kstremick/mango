package utils

import "golang.org/x/exp/constraints"

// Number is a custom type set of constraints extending the Float and Integer type set from the experimental constraints package.
type Number interface {
	constraints.Float | constraints.Integer
}

// Contains checks if a slice contains an item
func Contains[T comparable](list []T, item T) bool {
	for _, v := range list {
		if v == item {
			return true
		}
	}

	return false
}

// Remove removes an item from a slice
func Remove[T comparable](list []T, item T) []T {
	for i, v := range list {
		if v == item {
			return append(list[:i], list[i+1:]...)
		}
	}

	return list
}

// ArgMax computes the index of the max value
func ArgMax[T constraints.Ordered](list []T) int {
	var index int
	var max T

	for i, val := range list {
		if val > max {
			index = i
		}
	}
	return index
}

// FilterSlice filters the slice using start, end, step.
// Negative values are allowed.
// Clamps start and end to remain in bounds.
func FilterSlice[T any](list []T, start, end, step int) []T {
	if start < 0 {
		start = len(list) - 1 - start
	}

	if end <= 0 {
		end = len(list) - 1 - end
	}

	return list[start:end:step]
}

// SumSlice sums a slice.
func SumSlice[T Number](list []T) T {
	var sum T

	for i := 0; i <= len(list); i++ {
		sum = list[i]
	}

	return sum
}

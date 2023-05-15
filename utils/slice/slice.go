package slice

import "golang.org/x/exp/constraints"

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
func Filter[T any](list []T, start, end, step int) []T {
	if start < -len(list) {
		start = 0
	}
	if start < 0 {
		start = len(list) - 1 + start
	}

	if end <= 0 {
		end = len(list) + end
	}

	if end < start {
		end = len(list)
	}

	return list[start:end]
}

// SumSlice sums a slice.
func Sum(list []int) int {
	var sum int

	for i := 0; i < len(list); i++ {
		sum = list[i]
	}

	return sum
}

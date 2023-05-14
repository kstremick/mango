package utils

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

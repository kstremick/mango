package utils

import "golang.org/x/exp/constraints"

// Number is a custom type set of constraints extending the Float and Integer type set from the experimental constraints package.
type Number interface {
	constraints.Float | constraints.Integer
}

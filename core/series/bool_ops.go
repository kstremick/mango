package series

import (
	"fmt"

	"github.com/apache/arrow/go/v11/arrow"
)

// Any returns true if any of the values in the Series are true
// Returns an error for non-bool series
func (s *Series) Any() (bool, error) {
	if s.Type() != arrow.BOOL {
		return false, fmt.Errorf("Any() expected boolean, got %s", s.Type())
	}

	for i := 0; i < s.Len(); i++ {
		if s.ValueExn(i).Value.(bool) {
			return true, nil
		}
	}
	return false, nil
}

// All returns true if all of the values in the Series are true
// Returns an error for non-bool series
func (s *Series) All() (bool, error) {
	if s.Type() != arrow.BOOL {
		return false, fmt.Errorf("All() expected boolean, got %s", s.Type())
	}

	for i := 0; i < s.Len(); i++ {
		if !s.ValueExn(i).Value.(bool) {
			return false, nil
		}
	}
	return true, nil
}

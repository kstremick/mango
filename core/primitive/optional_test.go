package primitive_test

import (
	"testing"

	"github.com/kstremick/mango/core/primitive"

	"github.com/zeebo/assert"
)

func TestSome(t *testing.T) {
	type testData struct {
		input    interface{}
		expected primitive.Optional[interface{}]
	}
	tests := []testData{
		{input: "test", expected: primitive.Some[interface{}]("test")},
		{input: float64(3.14), expected: primitive.Some[interface{}](float64(3.14))},
		{input: true, expected: primitive.Some[interface{}](true)},
		{input: int64(42), expected: primitive.Some[interface{}](int64(42))},
	}

	for _, test := range tests {
		opt := primitive.Some(test.input)
		if opt != test.expected {
			t.Errorf("Expected Optional value %v, got %v for input %v", test.expected, opt, test.input)
		}
	}
}

func TestNone(t *testing.T) {
	none := primitive.None[interface{}]()
	assert.Equal(t, false, none.Valid)
	assert.Equal(t, nil, none.Value)
}

package primitive_test

import (
	"testing"

	"github.com/kstremick/mango/core/primitive"

	"github.com/apache/arrow/go/v11/arrow"
)

func TestToArrowDatatype(t *testing.T) {
	type testData struct {
		input    interface{}
		expected arrow.DataType
	}
	tests := []testData{
		{input: "", expected: arrow.BinaryTypes.String},
		{input: float64(0), expected: arrow.PrimitiveTypes.Float64},
		{input: false, expected: arrow.FixedWidthTypes.Boolean},
		{input: int64(0), expected: arrow.PrimitiveTypes.Int64},
	}

	for _, test := range tests {
		dataType, _ := primitive.ToArrowDatatype(test.input)
		if dataType.ID() != test.expected.ID() {
			t.Errorf("expected %v, got %v", test.expected, dataType)
		}
	}

}

func TestInvalidDataType(t *testing.T) {
	val, err := primitive.ToArrowDatatype([]int{1})
	if val != nil {
		t.Errorf("expected nil, got %v", val)
	}
	if err == nil {
		t.Errorf("expected error, got nil")
	}
}

func TestToArrowDatatypeT(t *testing.T) {
	type testData struct {
		dataTypeFunc func() arrow.DataType
		expected     arrow.DataType
	}
	tests := []testData{
		{dataTypeFunc: primitive.ToArrowDatatypeT[string], expected: arrow.BinaryTypes.String},
		{dataTypeFunc: primitive.ToArrowDatatypeT[float64], expected: arrow.PrimitiveTypes.Float64},
		{dataTypeFunc: primitive.ToArrowDatatypeT[bool], expected: arrow.FixedWidthTypes.Boolean},
		{dataTypeFunc: primitive.ToArrowDatatypeT[int64], expected: arrow.PrimitiveTypes.Int64},
	}

	for _, test := range tests {
		dataType := test.dataTypeFunc()
		if dataType.ID() != test.expected.ID() {
			t.Errorf("expected %v, got %v", test.expected, dataType)
		}
	}
}

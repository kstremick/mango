// Primitive defines the primitive types that are supported by mango.
package primitive

import (
	"fmt"
	"strconv"
	"strings"

	utils "github.com/kstremick/mango/utils/slice"

	"github.com/apache/arrow/go/v12/arrow"
)

type Primitive interface {
	~string | ~float64 | ~bool | ~int64
}

type Null struct{}

// ToArrowDatatype converts a primitive type to an arrow datatype.
func ToArrowDatatype(p interface{}) (arrow.DataType, error) {
	switch p := p.(type) {
	case string, Optional[string]:
		return arrow.BinaryTypes.String, nil
	case float64, Optional[float64]:
		return arrow.PrimitiveTypes.Float64, nil
	case bool, Optional[bool]:
		return arrow.FixedWidthTypes.Boolean, nil
	case int64, Optional[int64]:
		return arrow.PrimitiveTypes.Int64, nil
	case Optional[interface{}]:
		if p.Valid {
			return ToArrowDatatype(p.Value)
		} else {
			return nil, fmt.Errorf("cannot convert None to arrow datatype")
		}
	default:
		return nil, fmt.Errorf("unsupported type %T", p)
	}
}

var trueStrings = []string{"true", "t", "1", "1.0", "yes", "y"}
var falseStrings = []string{"false", "f", "0", "0.0", "no", "n"}
var boolStrings = append(trueStrings, falseStrings...)

// A list of all types in order of preference
var inferredTypeOrdering = []arrow.DataType{
	arrow.FixedWidthTypes.Boolean,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Float64,
	arrow.BinaryTypes.String,
}

func inferOrExtractDatatype(data []interface{}, inferOrExtract func(interface{}) []arrow.DataType) (arrow.DataType, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("cannot infer datatype from empty list")
	}
	possibleDatatypes := make([]arrow.DataType, 0)
	for _, d := range data {
		datatypes := inferOrExtract(d)
		if len(datatypes) == 0 {
			continue
		}
		if len(possibleDatatypes) == 0 {
			// If we haven't found any possible datatypes yet, set them to the first one
			possibleDatatypes = datatypes
		} else {
			// Otherwise, remove so that we're only left with datatypes that
			// Everything can satisfy
			for _, existingType := range possibleDatatypes {
				if !utils.Contains(datatypes, existingType) {
					possibleDatatypes = utils.Remove(possibleDatatypes, existingType)
				}
			}
		}
	}
	if len(possibleDatatypes) == 0 {
		return nil, fmt.Errorf("could not infer datatype from list")
	}
	// Now we have a list of possible datatypes, we need to find the best one
	// We do this by finding the first datatype in the ordering that is in the list
	for _, datatype := range inferredTypeOrdering {
		if utils.Contains(possibleDatatypes, datatype) {
			return datatype, nil
		}
	}
	return nil, fmt.Errorf("could not infer datatype from list")
}

// inferDatatypeFn converts a primitive type to a list of possible arrow datatypes.
func inferDatatypeFn(p interface{}) []arrow.DataType {
	var ret []arrow.DataType = []arrow.DataType{arrow.BinaryTypes.String}
	switch p := p.(type) {
	case string:
		_, err := strconv.Atoi(p)
		if err == nil {
			ret = append(ret, arrow.PrimitiveTypes.Int64)
		}
		_, err = strconv.ParseFloat(p, 64)
		if err == nil {
			ret = append(ret, arrow.PrimitiveTypes.Float64)
		}
		if utils.Contains(boolStrings, strings.TrimSpace(strings.ToLower(p))) {
			ret = append(ret, arrow.FixedWidthTypes.Boolean)
		}
	case float64:
		ret = append(ret, arrow.PrimitiveTypes.Float64)
	case bool:
		ret = append(ret, arrow.FixedWidthTypes.Boolean)
		ret = append(ret, arrow.PrimitiveTypes.Int64)
		ret = append(ret, arrow.PrimitiveTypes.Float64)
	case int64:
		ret = append(ret, arrow.PrimitiveTypes.Int64)
		ret = append(ret, arrow.PrimitiveTypes.Float64)
	case Null:
		return inferredTypeOrdering
	case Optional[interface{}]:
		return inferDatatypeFn(p.Value)
	default:
		return ret
	}
	return ret
}

// InferDatatype infers the arrow datatype from a list of interface{}.
func InferDatatype(data []interface{}) (arrow.DataType, error) {
	return inferOrExtractDatatype(data, inferDatatypeFn)
}

// extractDatatypeFn is a helper function for ExtractDatatype.
func extractDatatypeFn(x interface{}) []arrow.DataType {
	if _, isNull := x.(Null); isNull {
		return inferredTypeOrdering
	}
	datatype, err := ToArrowDatatype(x)
	if err != nil {
		return []arrow.DataType{}
	}
	return []arrow.DataType{datatype}
}

// ExtractDatatype extracts the datatype from a list of interface{}.
// Unlike InferDatatype, it doesn't do aggressive type coercion.
func ExtractDatatype(data []interface{}) (arrow.DataType, error) {
	return inferOrExtractDatatype(data, extractDatatypeFn)
}

func getNil[T any]() T {
	var nil T
	return nil
}

func ToArrowDatatypeT[T Primitive]() arrow.DataType {
	// We know T is a Primitive so it has a valid arrow DataType
	ret, _ := ToArrowDatatype(getNil[T]())
	return ret
}

func AttemptConversionT[T Primitive](val interface{}) (T, bool) {
	// Check if no conversion is needed
	var converted interface{}
	var ok bool
	value, ok := val.(T)
	if ok {
		return value, ok
	}

	// Attempt conversion
	nilT := getNil[T]()
	switch val := val.(type) {
	case bool:
		if _, isInt := any(nilT).(int64); isInt {
			if val {
				converted = 1
			} else {
				converted = 0
			}
			ok = true
		} else if _, isFloat := any(nilT).(float64); isFloat {
			if val {
				converted = 1.0
			} else {
				converted = 0.0
			}
			ok = true
		} else if _, isString := any(nilT).(string); isString {
			converted = strconv.FormatBool(val)
			ok = true
		}
	case int64:
		if _, isFloat := any(nilT).(float64); isFloat {
			converted = any(float64(val)).(T)
			ok = true
		} else if _, isBool := any(nilT).(bool); isBool {
			converted = any(val != 0).(T)
			ok = true
		} else if _, isString := any(nilT).(string); isString {
			converted = any(strconv.FormatInt(val, 10)).(T)
			ok = true
		}
	case float64:
		if _, isString := any(nilT).(string); isString {
			converted = any(strconv.FormatFloat(val, 'f', -1, 64)).(T)
			ok = true
		}
	case string:
		str := strings.TrimSpace(strings.ToLower(val))
		if _, isBool := any(nilT).(bool); isBool {
			if utils.Contains(trueStrings, str) {
				converted = true
				ok = true
			} else if utils.Contains(falseStrings, str) {
				converted = false
				ok = true
			}
		} else if _, isInt := any(nilT).(int64); isInt {
			c, err := strconv.ParseInt(val, 10, 64)
			if err == nil {
				converted = c
				ok = true
			}
		} else if _, isFloat64 := any(nilT).(float64); isFloat64 {
			c, err := strconv.ParseFloat(val, 64)
			if err == nil {
				converted = c
				ok = true
			}
		}
		if str == "" {
		}
	}
	if ok {
		return any(converted).(T), ok
	} else {
		return nilT, ok
	}
}

// CastListT casts a list of interface{} to a list of T.
// The second argument is the validity array, though there can be
// primitive.Null or uncastable types in data
// It also returns a list of bools indicating whether the cast was successful.
func CastListT[T Primitive](data []interface{}, valid []bool) ([]T, []bool) {
	casted := make([]T, len(data))
	valids := make([]bool, len(data))
	for i, val := range data {
		if valid != nil && !valid[i] {
			casted[i] = getNil[T]()
			continue
		}
		value, ok := AttemptConversionT[T](val)
		casted[i] = value
		valids[i] = ok
	}
	return casted, valids
}

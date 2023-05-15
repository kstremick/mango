package dataframe

import (
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v12/arrow"
)

func formatRow(values []string, maxLengths []int) string {
	formatted := "|"
	for _, value := range values {
		formatted += fmt.Sprintf(" %-*v |", maxLengths[0], value)
	}
	return formatted
}

func formatSeparator(lengths []int) string {
	separator := "+"
	for _, length := range lengths {
		separator += strings.Repeat("-", length+2) + "+"
	}
	return separator
}

func shortType(input arrow.DataType) string {
	switch input.Name() {
	case "utf8":
		return "str"
	case "int64":
		return "i64"
	case "float64":
		return "f64"
	default:
		return input.Name()
	}
}

func (df *DataFrame) String() string {
	columns := df.GetColumns()
	if len(columns) == 0 {
		return "Empty Dataframe"
	}

	var maxLengths []int
	var header, types []string

	for _, col := range columns {
		maxLength := len(col.Name)
		header = append(header, col.Name)
		types = append(types, shortType(col.DataType()))

		for i := 0; i < col.Len(); i++ {
			length := len(fmt.Sprintf("%v", col.ValueExn(i).String()))
			if length > maxLength {
				maxLength = length
			}
		}

		maxLengths = append(maxLengths, maxLength)
	}
	var sb strings.Builder
	sepRow := formatSeparator(maxLengths) + "\n"

	sb.WriteString("\n")
	sb.WriteString(sepRow)
	sb.WriteString(formatRow(header, maxLengths) + "\n")
	sb.WriteString(formatRow(types, maxLengths) + "\n")
	sb.WriteString(sepRow)

	nRows := columns[0].Len()
	for i := 0; i < nRows; i++ {
		var rowData []string
		for colI, col := range columns {
			value := col.ValueExn(i)
			rowData = append(rowData, fmt.Sprintf("%-*v", maxLengths[colI], value.String()))
		}
		sb.WriteString(formatRow(rowData, maxLengths) + "\n")
		sb.WriteString(sepRow)
	}
	return sb.String()
}

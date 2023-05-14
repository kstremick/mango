package dataframe_test

import (
	"mango/io"
	"testing"

	"github.com/zeebo/assert"
)

func TestPrint(t *testing.T) {
	csvData := `col1,col2,col3
1,a,1.1
2,b,2.2
3,c,3.3`

	df, err := io.ReadCsvString(csvData)
	output := df.String()

	expectedOutput := `
+------+------+------+
| col1 | col2 | col3 |
| i64  | str  | f64  |
+------+------+------+
| 1    | a    | 1.1  |
+------+------+------+
| 2    | b    | 2.2  |
+------+------+------+
| 3    | c    | 3.3  |
+------+------+------+
`
	if err != nil {
		t.Error(err)
	}
	assert.Equal(t, output, expectedOutput)
}

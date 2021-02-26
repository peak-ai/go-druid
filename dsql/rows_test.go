package dsql

import (
	"database/sql/driver"
	"reflect"
	"testing"
	"time"
)

func TestCanParseDateTimeField(t *testing.T) {
	ro := make([][]field, 0)
	cols := []string{"created_at"}

	ro = append(ro, []field{{
		Value: reflect.ValueOf("2013-01-01T00:00:00.000Z"),
		Type:  reflect.TypeOf("string"),
	}})

	rs := resultSet{
		rows:        ro,
		columnNames: cols,
		dateField:   "created_at",
		dateFormat:  "iso",
	}

	r := &rows{
		conn:      nil,
		resultSet: rs,
	}

	values := []driver.Value{
		"2013-01-01T00:00:00.000Z",
	}

	if err := r.Next(values); err != nil {
		t.Fatal(err)
	}

	resultType := reflect.TypeOf(values[0])
	isTime := resultType.AssignableTo(reflect.TypeOf(time.Now()))
	if !isTime {
		t.Fatal("not the time")
	}
}

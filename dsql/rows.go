package dsql

import (
	"database/sql/driver"
	"errors"
	"io"
	"log"
	"time"
)

type resultSet struct {
	rows        [][]field
	columnNames []string
	currentRow  int
	cancelled   bool
	dateField   string
	dateFormat  string
}

type rows struct {
	conn      *connection
	resultSet resultSet
}

func (r *rows) Columns() (cols []string) {
	cols = r.resultSet.columnNames
	return
}

// Close connection to druid - this doesn't do anything as it's
// not a long lived TCP connection, uses the http/api under the hood
func (r *rows) Close() (err error) {
	return
}

func (r *rows) Next(dest []driver.Value) error {
	if !r.HasNextResultSet() {
		return io.EOF
	}

	data := r.resultSet.rows[r.resultSet.currentRow]
	if len(data) != len(dest) {
		return errors.New("druid: number of refs passed to scan does not match column count")
	}

	for i := range dest {
		// Parse pre-defined timestamp field
		if r.resultSet.columnNames[i] == r.resultSet.dateField {

			log.Println("col: ", r.resultSet.columnNames[i])
			log.Println("date field: ", r.resultSet.dateField)

			// This refers to ISO8601
			if r.resultSet.dateFormat == "iso" {
				t, err := time.Parse(time.RFC3339Nano, data[i].Value.Interface().(string))
				if err != nil {
					log.Fatal("druid: failed to parse given datetime field: ", err.Error())
				}
				dest[i] = t
				continue
			}

			// @todo(e.v) - add more time formats here...
		}

		switch data[i].Type.Name() {
		// TODO: []byte
		case "bool":
			dest[i] = data[i].Value.Interface().(bool)
		case "string":
			dest[i] = data[i].Value.Interface().(string)
		case "int":
			dest[i] = data[i].Value.Interface().(int)
		case "int64":
			dest[i] = data[i].Value.Interface().(int64)
		case "float64":
			dest[i] = data[i].Value.Interface().(float64)
		default:
			log.Fatal("druid: can't scan type ", data[i].Type.Name())
		}
	}

	_ = r.NextResultSet()

	return nil
}

// HasNextResultSet implements driver.RowsNextResultSet
func (r *rows) HasNextResultSet() bool {
	return r.resultSet.currentRow != len(r.resultSet.rows)
}

// NextResultSet implements driver.RowsNextResultSet
func (r *rows) NextResultSet() error {
	r.resultSet.currentRow++
	if !r.HasNextResultSet() {
		return io.EOF
	}

	return nil
}

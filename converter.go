package dawn

import (
	"database/sql"
	"encoding/csv"
	"fmt"
	"io"
)

type Converter struct {
	rows *sql.Rows
}

func newConverter(rows *sql.Rows) *Converter {
	return &Converter{
		rows: rows,
	}
}

func (c Converter) Write(writer io.Writer) error {
	rows := c.rows
	csvWriter := csv.NewWriter(writer)

	columnNames, err := rows.Columns()
	if err != nil {
		return err
	}

	count := len(columnNames)
	values := make([]interface{}, count)
	valuePtrs := make([]interface{}, count)

	for rows.Next() {
		row := make([]string, count)

		for i, _ := range columnNames {
			valuePtrs[i] = &values[i]
		}

		if err = rows.Scan(valuePtrs...); err != nil {
			return err
		}

		for i, _ := range columnNames {
			var value interface{}
			rawValue := values[i]

			byteArray, ok := rawValue.([]byte)
			if ok {
				value = string(byteArray)
			} else {
				value = rawValue
			}

			if value == nil {
				row[i] = "`N"
			} else {
				row[i] = fmt.Sprintf("%v", value)
			}
		}
		err = csvWriter.Write(row)
		if err != nil {
			return err
		}
	}
	err = rows.Err()
	csvWriter.Flush()
	return err
}

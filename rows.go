package redshiftdatasqldriver

import (
	"context"
	"database/sql/driver"
	"io"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

type redshiftDataRows struct {
	id          string
	p           *redshiftdata.GetStatementResultPaginator
	resultSet   *redshiftdata.GetStatementResultOutput
	columns     []types.ColumnMetadata
	columnNames []string
	index       int
}

func newRows(id string, p *redshiftdata.GetStatementResultPaginator) *redshiftDataRows {
	debugLogger.Printf("[%s] create rows", id)
	return &redshiftDataRows{
		id: id,
		p:  p,
	}
}

func (rows *redshiftDataRows) Close() (err error) {
	debugLogger.Printf("[%s] rows close called", rows.id)
	return nil
}

func (rows *redshiftDataRows) Columns() []string {
	debugLogger.Printf("[%s] rows columns called", rows.id)
	if rows.columnNames != nil {
		return rows.columnNames
	}
	if err := rows.getStatementResult(); err != nil {
		return []string{}
	}
	return rows.columnNames
}

func (rows *redshiftDataRows) getStatementResult() error {
	if rows.p == nil {
		return io.EOF
	}
	var err error
	rows.resultSet, err = rows.p.NextPage(context.Background())
	if err != nil {
		return err
	}
	rows.columns = rows.resultSet.ColumnMetadata
	rows.columnNames = make([]string, 0, len(rows.columns))
	for _, meta := range rows.columns {
		rows.columnNames = append(rows.columnNames, *meta.Name)
	}
	rows.index = 0
	return nil
}

func (rows *redshiftDataRows) Next(dest []driver.Value) error {
	debugLogger.Printf("[%s] rows next called", rows.id)
	if rows.resultSet == nil || rows.index >= len(rows.resultSet.Records) {
		if !rows.p.HasMorePages() {
			return io.EOF
		}
		if err := rows.getStatementResult(); err != nil {
			return err
		}
		rows.index = 0
		if len(rows.resultSet.Records) == 0 {
			return io.EOF
		}
	}
	record := rows.resultSet.Records[rows.index]
	for i := range dest {
		if i < len(record) {
			switch field := record[i].(type) {
			case *types.FieldMemberIsNull:
				dest[i] = nil
			case *types.FieldMemberStringValue:
				switch {
				case strings.EqualFold(*rows.resultSet.ColumnMetadata[i].TypeName, "timestamp"):
					t, err := time.Parse("2006-01-02 15:04:05", field.Value)
					if err != nil {
						errLogger.Printf(`time.Parse("2006-01-02 15:04:05", "%s"): %v`, field.Value, err)
						dest[i] = nil
					} else {
						dest[i] = t
					}
				case strings.EqualFold(*rows.resultSet.ColumnMetadata[i].TypeName, "timestamptz"):
					t, err := time.Parse("2006-01-02 15:04:05-07", field.Value)
					if err != nil {
						errLogger.Printf(`time.Parse("2006-01-02 15:04:05-07", "%s"): %v`, field.Value, err)
						dest[i] = nil
					} else {
						dest[i] = t
					}
				default:
					dest[i] = field.Value
				}
			case *types.FieldMemberLongValue:
				dest[i] = field.Value
			case *types.FieldMemberBooleanValue:
				dest[i] = field.Value
			case *types.FieldMemberDoubleValue:
				dest[i] = field.Value
			case *types.FieldMemberBlobValue:
				dest[i] = field.Value
			}
		}
	}
	rows.index++
	return nil
}

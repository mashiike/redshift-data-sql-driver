package redshiftdatasqldriver

import (
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

type redshiftDataResult struct {
	affectedRows int64
}

func newResult(output *redshiftdata.DescribeStatementOutput) *redshiftDataResult {
	debugLogger.Printf("[%s] create result", coalesce(output.Id))
	return &redshiftDataResult{
		affectedRows: output.ResultRows,
	}
}
func (r *redshiftDataResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("LastInsertId %w", ErrNotSupported)
}

func (r *redshiftDataResult) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}

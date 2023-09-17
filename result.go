package redshiftdatasqldriver

import (
	"database/sql/driver"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
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

func newResultWithSubStatementData(st types.SubStatementData) *redshiftDataResult {
	debugLogger.Printf("[%s] create result", coalesce(st.Id))
	return &redshiftDataResult{
		affectedRows: st.ResultRows,
	}
}

func (r *redshiftDataResult) LastInsertId() (int64, error) {
	return 0, fmt.Errorf("LastInsertId %w", ErrNotSupported)
}

func (r *redshiftDataResult) RowsAffected() (int64, error) {
	return r.affectedRows, nil
}

type redshiftDataDelayedResult struct {
	driver.Result
}

func (r *redshiftDataDelayedResult) LastInsertId() (int64, error) {
	debugLogger.Printf("delayed result LastInsertId called")
	if r.Result != nil {
		return r.Result.LastInsertId()
	}
	return 0, ErrBeforeCommit
}

func (r *redshiftDataDelayedResult) RowsAffected() (int64, error) {
	debugLogger.Printf("delayed result RowsAffected called")
	if r.Result != nil {
		return r.Result.RowsAffected()
	}
	return 0, ErrBeforeCommit
}

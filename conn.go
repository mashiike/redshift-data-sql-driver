package redshiftdatasqldriver

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
)

type redshiftDataConn struct {
	client   RedshiftDataClient
	cfg      *RedshiftDataConfig
	aliveCh  chan struct{}
	isClosed bool

	inTx          bool
	txOpts        driver.TxOptions
	sqls          []string
	delayedResult []*redshiftDataDelayedResult
}

func newConn(client RedshiftDataClient, cfg *RedshiftDataConfig) *redshiftDataConn {
	return &redshiftDataConn{
		client:  client,
		cfg:     cfg,
		aliveCh: make(chan struct{}),
	}
}

func (conn *redshiftDataConn) PrepareContext(ctx context.Context, query string) (driver.Stmt, error) {
	return nil, fmt.Errorf("prepared statment %w", ErrNotSupported)
}

func (conn *redshiftDataConn) Prepare(query string) (driver.Stmt, error) {
	return conn.PrepareContext(context.Background(), query)
}

func (conn *redshiftDataConn) Close() error {
	if conn.isClosed {
		return nil
	}
	conn.isClosed = true
	close(conn.aliveCh)
	return nil
}

func (conn *redshiftDataConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	if conn.inTx {
		return nil, ErrInTx
	}
	if opts.Isolation != driver.IsolationLevel(sql.LevelDefault) {
		return nil, fmt.Errorf("transaction isolation level change: %w", ErrNotSupported)
	}
	conn.inTx = true
	conn.txOpts = opts
	cleanup := func() error {
		conn.inTx = false
		conn.sqls = nil
		conn.delayedResult = nil
		return nil
	}
	tx := &redshiftDataTx{
		onRollback: func() error {
			if !conn.inTx {
				return ErrNotInTx
			}
			return cleanup()
		},
		onCommit: func() error {
			if !conn.inTx {
				return ErrNotInTx
			}
			if len(conn.sqls) == 0 {
				return cleanup()
			}
			if len(conn.sqls) != len(conn.delayedResult) {
				panic(fmt.Sprintf("sqls and delayedResult length is not match: sqls=%d delayedResult=%d", len(conn.sqls), len(conn.delayedResult)))
			}
			if len(conn.sqls) == 1 {
				result, err := conn.ExecContext(ctx, conn.sqls[0], []driver.NamedValue{})
				if err != nil {
					return err
				}
				if conn.delayedResult[0] != nil {
					conn.delayedResult[0].Result = result
				}
				return nil
			}
			input := &redshiftdata.BatchExecuteStatementInput{
				Sqls: append(make([]string, 0, len(conn.sqls)), conn.sqls...),
			}
			_, desc, err := conn.batchExecuteStatement(ctx, input)
			if err != nil {
				return err
			}
			for i := range input.Sqls {
				if i >= len(desc.SubStatements) {
					return fmt.Errorf("sub statement not found: %d", i)
				}
				if conn.delayedResult[i] != nil {
					conn.delayedResult[i].Result = newResultWithSubStatementData(desc.SubStatements[i])
				}
			}
			return cleanup()
		},
	}

	return tx, nil
}

func (conn *redshiftDataConn) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}

func (conn *redshiftDataConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if conn.inTx {
		return nil, fmt.Errorf("query in transaction: %w", ErrNotSupported)
	}

	params := &redshiftdata.ExecuteStatementInput{
		Sql:        nullif(rewriteQuery(query, len(args))),
		Parameters: convertArgsToParameters(args),
	}
	p, output, err := conn.executeStatement(ctx, params)
	if err != nil {
		return nil, err
	}
	rows := newRows(coalesce(output.Id), p)
	return rows, nil
}

func (conn *redshiftDataConn) ExecContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Result, error) {
	if conn.inTx {
		if len(args) > 0 {
			return nil, fmt.Errorf("exec with args in transaction: %w", ErrNotSupported)
		}
		if conn.txOpts.ReadOnly {
			return nil, fmt.Errorf("exec in read only transaction: %w", ErrNotSupported)
		}
		conn.sqls = append(conn.sqls, query)
		result := &redshiftDataDelayedResult{}
		conn.delayedResult = append(conn.delayedResult, result)
		debugLogger.Printf("delayedResult[%d] creaed for %q", len(conn.delayedResult)-1, query)
		return result, nil
	}

	params := &redshiftdata.ExecuteStatementInput{
		Sql:        nullif(rewriteQuery(query, len(args))),
		Parameters: convertArgsToParameters(args),
	}
	_, output, err := conn.executeStatement(ctx, params)
	if err != nil {
		return nil, err
	}
	return newResult(output), nil
}

func rewriteQuery(query string, paramsCount int) string {
	if paramsCount == 0 {
		return query
	}
	runes := make([]rune, 0, len(query))
	stack := make([]rune, 0)
	var exclamationCount int
	for _, r := range query {
		if len(stack) > 0 {
			if r == stack[len(stack)-1] {
				stack = stack[:len(stack)-1]
				runes = append(runes, r)
				continue
			}
		} else {
			switch r {
			case '?':
				exclamationCount++
				runes = append(runes, []rune(fmt.Sprintf(":%d", exclamationCount))...)
				continue
			case '$':
				runes = append(runes, ':')
				continue
			}
		}
		switch r {
		case '"', '\'':
			stack = append(stack, r)
		}
		runes = append(runes, r)
	}
	return string(runes)
}

func convertArgsToParameters(args []driver.NamedValue) []types.SqlParameter {
	if len(args) == 0 {
		return nil
	}
	params := make([]types.SqlParameter, 0, len(args))
	for _, arg := range args {
		params = append(params, types.SqlParameter{
			Name:  aws.String(coalesce(nullif(arg.Name), aws.String(fmt.Sprintf("%d", arg.Ordinal)))),
			Value: aws.String(fmt.Sprintf("%v", arg.Value)),
		})
	}
	return params
}

func (conn *redshiftDataConn) executeStatement(ctx context.Context, params *redshiftdata.ExecuteStatementInput) (*redshiftdata.GetStatementResultPaginator, *redshiftdata.DescribeStatementOutput, error) {
	debugLogger.Printf("query: %s", coalesce(params.Sql))
	params.ClusterIdentifier = conn.cfg.ClusterIdentifier
	params.Database = conn.cfg.Database
	params.DbUser = conn.cfg.DbUser
	params.WorkgroupName = conn.cfg.WorkgroupName
	params.SecretArn = conn.cfg.SecretsARN

	executeOutput, err := conn.client.ExecuteStatement(ctx, params)
	if err != nil {
		return nil, nil, fmt.Errorf("execute statement:%w", err)
	}
	queryStart := time.Now()
	debugLogger.Printf("[%s] success execute statement: %s", *executeOutput.Id, coalesce(params.Sql))
	describeOutput, err := conn.waitWithCancel(ctx, executeOutput.Id, queryStart)
	if err != nil {
		return nil, nil, err
	}
	if describeOutput.Status == types.StatusStringAborted {
		return nil, nil, fmt.Errorf("query aborted: %s", *describeOutput.Error)
	}
	if describeOutput.Status == types.StatusStringFailed {
		return nil, nil, fmt.Errorf("query failed: %s", *describeOutput.Error)
	}
	if describeOutput.Status != types.StatusStringFinished {
		return nil, nil, fmt.Errorf("query status is not finished: %s", describeOutput.Status)
	}
	debugLogger.Printf("[%s] success query: elapsed_time=%s", *executeOutput.Id, time.Since(queryStart))
	if !*describeOutput.HasResultSet {
		return nil, describeOutput, nil
	}
	debugLogger.Printf("[%s] query has result set: result_rows=%d", *executeOutput.Id, describeOutput.ResultRows)
	p := redshiftdata.NewGetStatementResultPaginator(conn.client, &redshiftdata.GetStatementResultInput{
		Id: executeOutput.Id,
	})
	return p, describeOutput, nil
}

func (conn *redshiftDataConn) batchExecuteStatement(ctx context.Context, params *redshiftdata.BatchExecuteStatementInput) ([]*redshiftdata.GetStatementResultPaginator, *redshiftdata.DescribeStatementOutput, error) {
	params.ClusterIdentifier = conn.cfg.ClusterIdentifier
	params.Database = conn.cfg.Database
	params.DbUser = conn.cfg.DbUser
	params.WorkgroupName = conn.cfg.WorkgroupName
	params.SecretArn = conn.cfg.SecretsARN

	batchExecuteOutput, err := conn.client.BatchExecuteStatement(ctx, params)
	if err != nil {
		return nil, nil, fmt.Errorf("execute statement:%w", err)
	}
	queryStart := time.Now()
	debugLogger.Printf("[%s] success execute statement: %d sqls", *batchExecuteOutput.Id, len(params.Sqls))
	describeOutput, err := conn.waitWithCancel(ctx, batchExecuteOutput.Id, queryStart)
	if err != nil {
		return nil, nil, err
	}
	if describeOutput.Status == types.StatusStringAborted {
		return nil, nil, fmt.Errorf("query aborted: %s", *describeOutput.Error)
	}
	if describeOutput.Status == types.StatusStringFailed {
		return nil, nil, fmt.Errorf("query failed: %s", *describeOutput.Error)
	}
	if describeOutput.Status != types.StatusStringFinished {
		return nil, nil, fmt.Errorf("query status is not finished: %s", describeOutput.Status)
	}
	debugLogger.Printf("[%s] success query: elapsed_time=%s", *batchExecuteOutput.Id, time.Since(queryStart))
	ps := make([]*redshiftdata.GetStatementResultPaginator, len(params.Sqls))
	for i, st := range describeOutput.SubStatements {
		if *st.HasResultSet {
			continue
		}
		ps[i] = redshiftdata.NewGetStatementResultPaginator(conn.client, &redshiftdata.GetStatementResultInput{
			Id: st.Id,
		})
	}
	return ps, describeOutput, nil
}

func isFinishedStatus(status types.StatusString) bool {
	return status == types.StatusStringFinished || status == types.StatusStringFailed || status == types.StatusStringAborted
}

func (conn *redshiftDataConn) wait(ctx context.Context, id *string, queryStart time.Time) (*redshiftdata.DescribeStatementOutput, error) {
	timeout := conn.cfg.Timeout
	if timeout == 0 {
		timeout = 15 * time.Minute
	}
	polling := conn.cfg.Polling
	if polling == 0 {
		polling = 10 * time.Millisecond
	}
	ectx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	debugLogger.Printf("[%s] wating finsih query: elapsed_time=%s", *id, time.Since(queryStart))
	describeOutput, err := conn.client.DescribeStatement(ctx, &redshiftdata.DescribeStatementInput{
		Id: id,
	})
	if err != nil {
		return nil, fmt.Errorf("describe statement:%w", err)
	}
	debugLogger.Printf("[%s] describe statement: status=%s pid=%d query_id=%d", *id, describeOutput.Status, describeOutput.RedshiftPid, describeOutput.RedshiftQueryId)
	if isFinishedStatus(describeOutput.Status) {
		return describeOutput, nil
	}
	delay := time.NewTimer(polling)
	for {
		select {
		case <-ectx.Done():
			if !delay.Stop() {
				<-delay.C
			}
			return nil, ectx.Err()
		case <-delay.C:
		case <-conn.aliveCh:
			if !delay.Stop() {
				<-delay.C
			}
			return nil, ErrConnClosed
		}
		debugLogger.Printf("[%s] wating finsih query: elapsed_time=%s", *id, time.Since(queryStart))
		describeOutput, err = conn.client.DescribeStatement(ctx, &redshiftdata.DescribeStatementInput{
			Id: id,
		})
		if err != nil {
			return nil, fmt.Errorf("describe statement:%w", err)
		}
		if isFinishedStatus(describeOutput.Status) {
			return describeOutput, nil
		}
		delay.Reset(polling)
	}
}

func (conn *redshiftDataConn) waitWithCancel(ctx context.Context, id *string, queryStart time.Time) (*redshiftdata.DescribeStatementOutput, error) {
	desc, err := conn.wait(ctx, id, queryStart)
	cctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if desc == nil {
		var rErr error
		desc, rErr = conn.client.DescribeStatement(cctx, &redshiftdata.DescribeStatementInput{
			Id: id,
		})
		if rErr != nil {
			return nil, err
		}
	}
	if isFinishedStatus(desc.Status) {
		return desc, err
	}
	debugLogger.Printf("[%s] try cancel statement", *id)
	output, cErr := conn.client.CancelStatement(cctx, &redshiftdata.CancelStatementInput{
		Id: id,
	})
	if cErr != nil {
		errLogger.Printf("[%s] failed cancel statement: %v", *id, err)
		return desc, err
	}
	if !*output.Status {
		debugLogger.Printf("[%s] cancel statement status is false", *id)
	}
	return desc, err
}

package redshiftdatasqldriver

import (
	"context"
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
	return nil, fmt.Errorf("transaction %w", ErrNotSupported)
}

func (conn *redshiftDataConn) Begin() (driver.Tx, error) {
	return conn.BeginTx(context.Background(), driver.TxOptions{})
}

func (conn *redshiftDataConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	params := &redshiftdata.ExecuteStatementInput{
		Sql:        nullif(query),
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

	if conn.cfg.Timeout == 0 {
		conn.cfg.Timeout = 15 * time.Minute
	}
	if conn.cfg.Polling == 0 {
		conn.cfg.Polling = 10 * time.Millisecond
	}
	ectx, cancel := context.WithTimeout(ctx, conn.cfg.Timeout)
	defer cancel()
	executeOutput, err := conn.client.ExecuteStatement(ectx, params)
	if err != nil {
		return nil, nil, fmt.Errorf("execute statement:%w", err)
	}
	queryStart := time.Now()
	debugLogger.Printf("[%s] sucess execute statement: %s", *executeOutput.Id, coalesce(params.Sql))
	describeOutput, err := conn.client.DescribeStatement(ectx, &redshiftdata.DescribeStatementInput{
		Id: executeOutput.Id,
	})
	if err != nil {
		return nil, nil, fmt.Errorf("describe statement:%w", err)
	}
	debugLogger.Printf("[%s] describe statement: status=%s pid=%d query_id=%d", *executeOutput.Id, describeOutput.Status, describeOutput.RedshiftPid, describeOutput.RedshiftQueryId)

	var isFinished bool
	defer func() {
		if !isFinished {
			describeOutput, err := conn.client.DescribeStatement(ctx, &redshiftdata.DescribeStatementInput{
				Id: executeOutput.Id,
			})
			if err != nil {
				errLogger.Printf("[%s] failed describe statement: %v", *executeOutput.Id, err)
				return
			}
			if describeOutput.Status == types.StatusStringFinished ||
				describeOutput.Status == types.StatusStringFailed ||
				describeOutput.Status == types.StatusStringAborted {
				return
			}
			debugLogger.Printf("[%s] try cancel statement", *executeOutput.Id)
			output, err := conn.client.CancelStatement(ctx, &redshiftdata.CancelStatementInput{
				Id: executeOutput.Id,
			})
			if err != nil {

				errLogger.Printf("[%s] failed cancel statement: %v", *executeOutput.Id, err)
				return
			}
			if !*output.Status {
				debugLogger.Printf("[%s] cancel statement status is false", *executeOutput.Id)
			}
		}
	}()
	delay := time.NewTimer(conn.cfg.Polling)
	for {
		if describeOutput.Status == types.StatusStringAborted {
			return nil, nil, fmt.Errorf("query aborted: %s", *describeOutput.Error)
		}
		if describeOutput.Status == types.StatusStringFailed {
			return nil, nil, fmt.Errorf("query failed: %s", *describeOutput.Error)
		}
		if describeOutput.Status == types.StatusStringFinished {
			break
		}
		debugLogger.Printf("[%s] wating finsih query: elapsed_time=%s", *executeOutput.Id, time.Since(queryStart))
		delay.Reset(conn.cfg.Polling)
		select {
		case <-ectx.Done():
			if !delay.Stop() {
				<-delay.C
			}
			return nil, nil, ectx.Err()
		case <-delay.C:
		case <-conn.aliveCh:
			if !delay.Stop() {
				<-delay.C
			}
			return nil, nil, ErrConnClosed
		}
		describeOutput, err = conn.client.DescribeStatement(ctx, &redshiftdata.DescribeStatementInput{
			Id: executeOutput.Id,
		})
		if err != nil {
			return nil, nil, fmt.Errorf("describe statement:%w", err)
		}
	}
	isFinished = true
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

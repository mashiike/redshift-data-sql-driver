package redshiftdatasqldriver

import (
	"context"
	"errors"

	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

type mockRedshiftDataClient struct {
	ExecuteStatementFunc      func(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error)
	DescribeStatementFunc     func(ctx context.Context, params *redshiftdata.DescribeStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.DescribeStatementOutput, error)
	CancelStatementFunc       func(ctx context.Context, params *redshiftdata.CancelStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.CancelStatementOutput, error)
	GetStatementResultFunc    func(ctx context.Context, params *redshiftdata.GetStatementResultInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.GetStatementResultOutput, error)
	BatchExecuteStatementFunc func(ctx context.Context, params *redshiftdata.BatchExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.BatchExecuteStatementOutput, error)
}

func (m *mockRedshiftDataClient) ExecuteStatement(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error) {
	if m.ExecuteStatementFunc == nil {
		return nil, errors.New("unexpected call ExecuteStatement")
	}
	return m.ExecuteStatementFunc(ctx, params)
}

func (m *mockRedshiftDataClient) DescribeStatement(ctx context.Context, params *redshiftdata.DescribeStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.DescribeStatementOutput, error) {
	if m.DescribeStatementFunc == nil {
		return nil, errors.New("unexpected call DescribeStatement")
	}
	return m.DescribeStatementFunc(ctx, params)
}

func (m *mockRedshiftDataClient) CancelStatement(ctx context.Context, params *redshiftdata.CancelStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.CancelStatementOutput, error) {
	if m.DescribeStatementFunc == nil {
		return nil, errors.New("unexpected call CancelStatement")
	}
	return m.CancelStatementFunc(ctx, params)
}

func (m *mockRedshiftDataClient) GetStatementResult(ctx context.Context, params *redshiftdata.GetStatementResultInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.GetStatementResultOutput, error) {
	if m.DescribeStatementFunc == nil {
		return nil, errors.New("unexpected call GetStatementResult")
	}
	return m.GetStatementResultFunc(ctx, params)
}

func (m *mockRedshiftDataClient) BatchExecuteStatement(ctx context.Context, params *redshiftdata.BatchExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.BatchExecuteStatementOutput, error) {
	if m.DescribeStatementFunc == nil {
		return nil, errors.New("unexpected call BatchExecuteStatement")
	}
	return m.BatchExecuteStatementFunc(ctx, params)
}

package redshiftdatasqldriver

import (
	"context"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
)

type RedshiftDataClient interface {
	ExecuteStatement(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error)
	DescribeStatement(ctx context.Context, params *redshiftdata.DescribeStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.DescribeStatementOutput, error)
	CancelStatement(ctx context.Context, params *redshiftdata.CancelStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.CancelStatementOutput, error)
	redshiftdata.GetStatementResultAPIClient
}

var RedshiftDataClientConstructor func(ctx context.Context, cfg *RedshiftDataConfig) (RedshiftDataClient, error)

func newRedshiftDataClient(ctx context.Context, cfg *RedshiftDataConfig) (RedshiftDataClient, error) {
	if RedshiftDataClientConstructor != nil {
		return RedshiftDataClientConstructor(ctx, cfg)
	}
	return DefaultRedshiftDataClientConstructor(ctx, cfg)
}

func DefaultRedshiftDataClientConstructor(ctx context.Context, cfg *RedshiftDataConfig) (RedshiftDataClient, error) {
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return nil, err
	}
	client := redshiftdata.NewFromConfig(awsCfg, cfg.RedshiftDataOptFns...)
	return client, nil
}

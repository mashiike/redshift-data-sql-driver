package redshiftdatasqldriver

import (
	"net/url"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/stretchr/testify/require"
)

func TestRedshiftDataConfig__String(t *testing.T) {
	cases := []struct {
		dsn      *RedshiftDataConfig
		expected string
	}{
		{
			dsn:      &RedshiftDataConfig{},
			expected: "",
		},
		{
			dsn: &RedshiftDataConfig{
				ClusterIdentifier: aws.String("default"),
				DbUser:            aws.String("admin"),
				Database:          aws.String("dev"),
			},
			expected: "admin@cluster(default)/dev",
		},
		{
			dsn: &RedshiftDataConfig{
				ClusterIdentifier: aws.String("default"),
				DbUser:            aws.String("admin"),
				Database:          aws.String("dev"),
				Timeout:           15 * time.Minute,
			},
			expected: "admin@cluster(default)/dev?timeout=15m0s",
		},
		{
			dsn: &RedshiftDataConfig{
				ClusterIdentifier: aws.String("default"),
				DbUser:            aws.String("admin"),
				Database:          aws.String("dev"),
				Polling:           5 * time.Millisecond,
			},
			expected: "admin@cluster(default)/dev?polling=5ms",
		},
		{
			dsn: &RedshiftDataConfig{
				ClusterIdentifier: aws.String("default"),
				DbUser:            aws.String("admin"),
				Database:          aws.String("dev"),
				Params: url.Values{
					"extra": []string{"hoge"},
				},
			},
			expected: "admin@cluster(default)/dev?extra=hoge",
		},
		{
			dsn: (&RedshiftDataConfig{
				ClusterIdentifier: aws.String("default"),
				DbUser:            aws.String("admin"),
				Database:          aws.String("dev"),
			}).WithRegion("us-east-1"),
			expected: "admin@cluster(default)/dev?region=us-east-1",
		},
		{
			dsn: &RedshiftDataConfig{
				WorkgroupName: aws.String("default"),
				Database:      aws.String("dev"),
			},
			expected: "workgroup(default)/dev",
		},
		{
			dsn: &RedshiftDataConfig{
				SecretsARN: aws.String("arn:aws:secretsmanager:us-east-1:0123456789012:secret:redshift"),
				Timeout:    30 * time.Second,
			},
			expected: "arn:aws:secretsmanager:us-east-1:0123456789012:secret:redshift?timeout=30s",
		},
	}

	for _, c := range cases {
		t.Run(c.expected, func(t *testing.T) {
			actual := c.dsn.String()
			require.Equal(t, c.expected, actual)
			if c.expected != "" {
				cfg, err := ParseDSN(actual)
				require.NoError(t, err)
				require.Equal(t, len(c.dsn.RedshiftDataOptFns), len(cfg.RedshiftDataOptFns))
				cfg.RedshiftDataOptFns = c.dsn.RedshiftDataOptFns
				require.EqualValues(t, c.dsn, cfg)
			}
		})
	}
}

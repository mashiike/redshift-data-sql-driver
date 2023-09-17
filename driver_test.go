package redshiftdatasqldriver

import (
	"context"
	"database/sql"
	"errors"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata"
	"github.com/aws/aws-sdk-go-v2/service/redshiftdata/types"
	"github.com/stretchr/testify/require"
)

var mockClients = map[string]RedshiftDataClient{}

func init() {
	RedshiftDataClientConstructor = func(ctx context.Context, cfg *RedshiftDataConfig) (RedshiftDataClient, error) {
		if mockName := cfg.Params.Get("mock"); mockName != "" {
			return mockClients[mockName], nil
		}
		return DefaultRedshiftDataClientConstructor(ctx, cfg)
	}
}

func runTestsWithDB(t *testing.T, dsn string, tests ...func(*testing.T, *sql.DB)) {
	if dsn == "" {
		t.Log("dsn is empty")
		t.SkipNow()
	}
	db, err := sql.Open("redshift-data", dsn)
	if err != nil {
		t.Fatalf("error connecting: %s", err.Error())
	}
	defer db.Close()
	for _, test := range tests {
		test(t, db)
	}
}

var (
	dsn string = (&RedshiftDataConfig{
		WorkgroupName:     nullif(os.Getenv("TEST_WORKGROUP_NAME")),
		Database:          nullif(os.Getenv("TEST_DATABASE")),
		DbUser:            nullif(os.Getenv("TEST_DB_USER")),
		SecretsARN:        nullif(os.Getenv("TEST_SECRETS_ARN")),
		ClusterIdentifier: nullif(os.Getenv("TEST_CLUSTER_IDENTIFIER")),
	}).String()
)

func TestSimpleQuery(t *testing.T) {
	runTestsWithDB(t, dsn, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		sql := `SELECT 1 as number, 'hoge' as string UNION SELECT 2 as number, 'fuga' as string`
		rows, err := db.QueryContext(context.Background(), sql)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()
		require.True(t, rows.Next())
		columns, err := rows.Columns()
		require.NoError(t, err)
		require.EqualValues(t, []string{"number", "string"}, columns)
		var num int64
		var text string
		require.NoError(t, rows.Scan(&num, &text))
		require.Equal(t, int64(1), num)
		require.Equal(t, "hoge", text)
		require.True(t, rows.Next())
		require.NoError(t, rows.Scan(&num, &text))
		require.Equal(t, int64(2), num)
		require.Equal(t, "fuga", text)
		require.False(t, rows.Next())
	})
}

func TestTimestampQuery(t *testing.T) {
	runTestsWithDB(t, dsn, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		query := `SELECT
			getdate()::timestamp without time zone as without_timezone
			,getdate()::timestamp with time zone as with_timezone`
		rows, err := db.QueryContext(context.Background(), query)
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()
		require.True(t, rows.Next())
		var withoutTimezone, withTimezone sql.NullTime
		require.NoError(t, rows.Scan(&withoutTimezone, &withTimezone))
		require.True(t, time.Until(withoutTimezone.Time) <= time.Hour)
		require.True(t, time.Until(withTimezone.Time) <= time.Hour)
	})
}

func TestOrdinalParameterQuery(t *testing.T) {
	runTestsWithDB(t, dsn, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		query := `SELECT usesysid, usename FROM pg_user WHERE usename = :1`
		rows, err := db.QueryContext(context.Background(), query, "rdsdb")
		require.NoError(t, err)
		defer func() {
			require.NoError(t, rows.Close())
		}()
		require.True(t, rows.Next())
		var userID int64
		var userName string
		require.NoError(t, rows.Scan(&userID, &userName))
		require.Equal(t, int64(1), userID)
		require.Equal(t, "rdsdb", userName)
	})
}

func TestSimpleExec(t *testing.T) {
	runTestsWithDB(t, dsn, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		_, err := db.ExecContext(context.Background(), `DROP TABLE IF EXISTS "public"."redshift_data_sql_driver_test"`)
		require.NoError(t, err)
		_, err = db.ExecContext(context.Background(), `CREATE TABLE "public"."redshift_data_sql_driver_test" (id BIGINT, created_at TIMESTAMP)`)
		require.NoError(t, err)
		result, err := db.ExecContext(
			context.Background(),
			`INSERT INTO "public"."redshift_data_sql_driver_test" SELECT :id as id, getdate() as created_at`,
			sql.Named("id", 1),
		)
		require.NoError(t, err)
		rowsAffected, err := result.RowsAffected()
		require.NoError(t, err)
		require.Equal(t, int64(1), rowsAffected)

		rows, err := db.QueryContext(
			context.Background(),
			`SELECT * FROM "public"."redshift_data_sql_driver_test" WHERE id = :id`,
			sql.Named("id", 1),
		)
		defer func() {
			require.NoError(t, rows.Close())
		}()
		require.NoError(t, err)
		require.True(t, rows.Next())
		var id int64
		var createdAt sql.NullTime
		require.NoError(t, rows.Scan(&id, &createdAt))
		require.Equal(t, int64(1), id)
		require.True(t, time.Until(createdAt.Time) <= time.Hour)
	})
}

func TestQueryCancel(t *testing.T) {
	cancelCalled := false
	mockClients["query_cancel"] = &mockRedshiftDataClient{
		ExecuteStatementFunc: func(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error) {
			return &redshiftdata.ExecuteStatementOutput{
				Id: aws.String("dummy"),
			}, nil
		},
		DescribeStatementFunc: func(ctx context.Context, params *redshiftdata.DescribeStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.DescribeStatementOutput, error) {
			return &redshiftdata.DescribeStatementOutput{
				Id:     aws.String("dummy"),
				Status: types.StatusStringStarted,
			}, nil
		},
		CancelStatementFunc: func(ctx context.Context, params *redshiftdata.CancelStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.CancelStatementOutput, error) {
			cancelCalled = true
			return &redshiftdata.CancelStatementOutput{
				Status: aws.Bool(true),
			}, nil
		},
	}
	mockDSN := (&RedshiftDataConfig{
		SecretsARN: aws.String("arn:aws:secretsmanager:us-east-1:0123456789012:secret:redshift"),
		Params:     url.Values{"mock": []string{"query_cancel"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		query := `SELECT * FROM long_long_view`
		ctx, cancel := context.WithTimeout(context.Background(), 1*time.Second)
		defer cancel()
		_, err := db.QueryContext(ctx, query)
		require.ErrorIs(t, err, context.DeadlineExceeded)
		require.True(t, cancelCalled)
	})
}

func TestMockSuccess(t *testing.T) {
	var lastStatus types.StatusString
	query := `SELECT * FROM success_view`
	mockClients["success"] = &mockRedshiftDataClient{
		ExecuteStatementFunc: func(ctx context.Context, params *redshiftdata.ExecuteStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.ExecuteStatementOutput, error) {
			require.EqualValues(t, query, coalesce(params.Sql))
			return &redshiftdata.ExecuteStatementOutput{
				Id: aws.String("dummy"),
			}, nil
		},
		DescribeStatementFunc: func(ctx context.Context, params *redshiftdata.DescribeStatementInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.DescribeStatementOutput, error) {
			switch lastStatus {
			case types.StatusStringSubmitted:
				lastStatus = types.StatusStringPicked
			case types.StatusStringPicked:
				lastStatus = types.StatusStringStarted
			case types.StatusStringStarted:
				lastStatus = types.StatusStringFinished
			default:
				lastStatus = types.StatusStringSubmitted
			}
			return &redshiftdata.DescribeStatementOutput{
				Id:           aws.String("dummy"),
				Status:       lastStatus,
				HasResultSet: aws.Bool(true),
				ResultRows:   4,
			}, nil
		},
		GetStatementResultFunc: func(ctx context.Context, params *redshiftdata.GetStatementResultInput, optFns ...func(*redshiftdata.Options)) (*redshiftdata.GetStatementResultOutput, error) {
			columns := []types.ColumnMetadata{
				{
					Name:     aws.String("name"),
					TypeName: aws.String("varchar"),
				},
				{
					Name:     aws.String("age"),
					TypeName: aws.String("int4"),
				},
				{
					Name:     aws.String("auth"),
					TypeName: aws.String("bool"),
				},
				{
					Name:     aws.String("value"),
					TypeName: aws.String("float"),
				},
				{
					Name:     aws.String("bin"),
					TypeName: aws.String("binary"),
				},
			}
			if params.NextToken == nil {
				return &redshiftdata.GetStatementResultOutput{
					ColumnMetadata: columns,
					Records: [][]types.Field{
						{
							&types.FieldMemberStringValue{Value: "hoge"},
							&types.FieldMemberLongValue{Value: 16},
							&types.FieldMemberBooleanValue{Value: true},
							&types.FieldMemberDoubleValue{Value: 0.99},
							&types.FieldMemberBlobValue{Value: []byte{12, 13}},
						},
						{
							&types.FieldMemberStringValue{Value: "fuga"},
							&types.FieldMemberLongValue{Value: 18},
							&types.FieldMemberIsNull{Value: false},
							&types.FieldMemberDoubleValue{Value: 0.8},
							&types.FieldMemberIsNull{Value: false},
						},
					},
					NextToken:    aws.String("dummy"),
					TotalNumRows: 4,
				}, nil
			}
			if *params.NextToken == "dummy" {
				return &redshiftdata.GetStatementResultOutput{
					ColumnMetadata: columns,
					Records: [][]types.Field{
						{
							&types.FieldMemberStringValue{Value: "piyo"},
							&types.FieldMemberLongValue{Value: 22},
							&types.FieldMemberBooleanValue{Value: false},
							&types.FieldMemberIsNull{Value: false},
							&types.FieldMemberIsNull{Value: false},
						},
						{
							&types.FieldMemberStringValue{Value: "tora"},
							&types.FieldMemberLongValue{Value: 25},
							&types.FieldMemberBooleanValue{Value: false},
							&types.FieldMemberIsNull{Value: false},
							&types.FieldMemberIsNull{Value: false},
						},
					},
					NextToken:    nil,
					TotalNumRows: 4,
				}, nil
			}
			return nil, errors.New("unexpected next token")
		},
	}
	mockDSN := (&RedshiftDataConfig{
		SecretsARN: aws.String("arn:aws:secretsmanager:us-east-1:0123456789012:secret:redshift"),
		Params:     url.Values{"mock": []string{"success"}},
	}).String()
	runTestsWithDB(t, mockDSN, func(t *testing.T, db *sql.DB) {
		restore := requireNoErrorLog(t)
		defer restore()
		rows, err := db.QueryContext(context.Background(), query)
		require.NoError(t, err)
		require.Equal(t, types.StatusStringFinished, lastStatus)
		actualColumns, err := rows.Columns()
		require.NoError(t, err)
		require.EqualValues(t, []string{"name", "age", "auth", "value", "bin"}, actualColumns)
		actual := make([]map[string]interface{}, 0, 4)
		for rows.Next() {
			var (
				name  string
				age   int64
				auth  sql.NullBool
				value sql.NullFloat64
				bin   []byte
			)
			err := rows.Scan(&name, &age, &auth, &value, &bin)
			require.NoError(t, err)
			actual = append(actual, map[string]interface{}{
				"name":  name,
				"age":   age,
				"auth":  auth,
				"value": value,
				"bin":   bin,
			})
		}
		require.Equal(t, []map[string]interface{}{
			{
				"name": "hoge",
				"age":  int64(16),
				"auth": sql.NullBool{
					Bool:  true,
					Valid: true,
				},
				"value": sql.NullFloat64{
					Float64: 0.99,
					Valid:   true,
				},
				"bin": []byte{12, 13},
			},
			{
				"name": "fuga",
				"age":  int64(18),
				"auth": sql.NullBool{
					Bool:  false,
					Valid: false,
				},
				"value": sql.NullFloat64{
					Float64: 0.8,
					Valid:   true,
				},
				"bin": []byte(nil),
			},
			{
				"name": "piyo",
				"age":  int64(22),
				"auth": sql.NullBool{
					Bool:  false,
					Valid: true,
				},
				"value": sql.NullFloat64{
					Valid: false,
				},
				"bin": []byte(nil),
			},
			{
				"name": "tora",
				"age":  int64(25),
				"auth": sql.NullBool{
					Bool:  false,
					Valid: true,
				},
				"value": sql.NullFloat64{
					Valid: false,
				},
				"bin": []byte(nil),
			},
		}, actual)
	})
}

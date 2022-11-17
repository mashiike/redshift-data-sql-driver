package redshiftdatasqldriver

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestRewriteQuery(t *testing.T) {
	cases := []struct {
		casename    string
		query       string
		paramsCount int
		expected    string
	}{
		{
			casename:    "no params",
			query:       `SELECT * FROM pg_user`,
			paramsCount: 0,
			expected:    `SELECT * FROM pg_user`,
		},
		{
			casename:    "no change",
			query:       `SELECT * FROM pg_user WHERE usename = :name`,
			paramsCount: 1,
			expected:    `SELECT * FROM pg_user WHERE usename = :name`,
		},
		{
			casename:    "? rewrite",
			query:       `SELECT 'hoge?' FROM pg_user WHERE usename = ? AND usesysid > ?`,
			paramsCount: 1,
			expected:    `SELECT 'hoge?' FROM pg_user WHERE usename = :1 AND usesysid > :2`,
		},
		{
			casename:    "$ rewrite",
			query:       `SELECT '3$1$' FROM table WHERE "$column" = $1 AND column1 > $2 AND column2 < $1`,
			paramsCount: 1,
			expected:    `SELECT '3$1$' FROM table WHERE "$column" = :1 AND column1 > :2 AND column2 < :1`,
		},
	}
	for _, c := range cases {
		t.Run(c.casename, func(t *testing.T) {
			actual := rewriteQuery(c.query, c.paramsCount)
			require.Equal(t, c.expected, actual)
		})
	}
}

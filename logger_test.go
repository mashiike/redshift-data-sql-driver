package redshiftdatasqldriver

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/require"
)

func requireNoErrorLog(t *testing.T) func() {
	t.Helper()
	var errBuilder, debugBuilder strings.Builder
	errOrig := errLogger.Writer()
	debugOrig := debugLogger.Writer()
	errLogger.SetOutput(&errBuilder)
	debugLogger.SetOutput(&debugBuilder)
	return func() {
		errLogger.SetOutput(errOrig)
		debugLogger.SetOutput(debugOrig)
		t.Log(debugBuilder.String())
		if errLogs := errBuilder.String(); errLogs != "" {
			require.FailNow(t, errLogs)
		}
	}
}

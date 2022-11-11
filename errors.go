package redshiftdatasqldriver

import "errors"

var (
	ErrNotSupported = errors.New("not supported")
	ErrDSNEmpty     = errors.New("dsn is empty")
	ErrConnClosed   = errors.New("connection closed")
)

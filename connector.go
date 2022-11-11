package redshiftdatasqldriver

import (
	"context"
	"database/sql/driver"
)

type redshiftDataConnector struct {
	d   *redshiftDataDriver
	cfg *RedshiftDataConfig
}

func (c *redshiftDataConnector) Connect(ctx context.Context) (driver.Conn, error) {
	client, err := newRedshiftDataClient(ctx, c.cfg)
	if err != nil {
		return nil, err
	}
	return newConn(client, c.cfg), nil
}

func (c *redshiftDataConnector) Driver() driver.Driver {
	return c.d
}

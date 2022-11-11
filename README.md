# redshift-data-sql-driver

[![Documentation](https://godoc.org/github.com/mashiike/redshift-data-sql-driver?status.svg)](https://godoc.org/github.com/mashiike/redshift-data-sql-driver)
![Latest GitHub tag](https://img.shields.io/github/tag/mashiike/redshift-data-sql-driver.svg)
![Github Actions test](https://github.com/mashiike/redshift-data-sql-driver/workflows/Test/badge.svg?branch=main)
[![Go Report Card](https://goreportcard.com/badge/mashiike/redshift-data-sql-driver)](https://goreportcard.com/report/mashiike/redshift-data-sql-driver)
[![License](https://img.shields.io/badge/license-MIT-blue.svg)](https://github.com/mashiike/redshift-data-sql-driver/blob/master/LICENSE)

Redshift-Data API SQL Driver for Go's [database/sql](https://pkg.go.dev/database/sql) package

## Usage 

for example:

```go
package main

import (
	"context"
	"database/sql"
	"log"

	_ "github.com/mashiike/redshift-data-sql-driver"
)

func main() {
	db, err := sql.Open("redshift-data", "workgroup(default)/dev?timeout=1m")
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()
	rows, err := db.QueryContext(
		context.Background(),
		`SELECT table_schema,table_name,table_type FROM svv_tables WHERE table_schema not like :ignore_schema`,
		sql.Named("ignore_schema", "pg_%"),
	)
	if err != nil {
		log.Fatalln(err)
	}
	for rows.Next() {
		var schema, name, tableType string
		err := rows.Scan(&schema, &name, &tableType)
		if err != nil {
			log.Println(err)
			return
		}
		log.Printf("%s.%s\t%s", schema, name, tableType)
	}
}
```

The pattern for specifying DSNs is as follows

- with redshift serverless: `workgroup([name])/[database]`
- with provisoned cluster: `[dbuser]@cluster([name])/[database]`
- with AWS Secrets Manager: `arn:aws:secretsmanager:us-east-1:0123456789012:secret:redshift`

The DSN parameters include

- `timeout`: Timeout for query execution. default = `15m0s`
- `polling`: Interval to check for the end of a running query. default = `10ms`
- `region`: Redshift Data API's region. Default is environment setting

Parameter settings are in the format of URL query parameter

`workgroup(default)/dev?timeout=1m&polling=1ms`

## Unsupported Features

The following functions are not available

### [BeginTx](https://pkg.go.dev/database/sql#DB.BeginTx),[Begin](https://pkg.go.dev/database/sql#DB.Begin)  

The Redshift Data API does not have an interface for pasting transactions and querying sequentially.
Therefore, transactional functionality is not available.

### [Prepare](https://pkg.go.dev/database/sql#DB.Prepare)

Prepared Statements were not supported because the Redshift Data API does not have the concept of connecting to a DB.

## LICENSE

MIT

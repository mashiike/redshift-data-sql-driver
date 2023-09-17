package redshiftdatasqldriver

type redshiftDataTx struct {
	onCommit   func() error
	onRollback func() error
}

func (tx *redshiftDataTx) Commit() error {
	debugLogger.Printf("tx commit called")
	return tx.onCommit()
}

func (tx *redshiftDataTx) Rollback() error {
	debugLogger.Printf("tx rollback called")
	return tx.onRollback()
}

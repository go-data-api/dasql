package dasql

import "context"

// Tx represents a SQL transaction
type Tx interface {
	Exec(ctx context.Context, q string, args ...interface{}) (Result, error)
}

// daTx implements the Tx interface for the Data API
type daTx struct {
	id string
	db *DB
}

// Exec executes sql inside of the transaction
func (tx daTx) Exec(ctx context.Context, q string, args ...interface{}) (Result, error) {
	return tx.db.exec(ctx, tx.id, q, args...)
}

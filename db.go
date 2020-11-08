package dasql

import "context"

// DB can be used to execute SQL using the AWS Aurora Data API
type DB struct{}

// New initializes the database abstraction
func New() *DB { return &DB{} }

// Exec executes SQL.The args are for any placeholder parameters in the query.
func (db *DB) Exec(ctx context.Context, q string, args ...interface{}) (Result, error) {
	return db.exec(ctx, "", q, args...)
}

// exec is the private implementation that also works with a transaction
func (db *DB) exec(ctx context.Context, tid string, q string, args ...interface{}) (Result, error) {
	return nil, nil
}

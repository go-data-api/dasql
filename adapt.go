package dasql

import (
	"context"
	"database/sql"
)

// Adapt wraps the stdlib database to provide an implementation of the same interface this library
// provides for using the AWS Aurora Data API.
func Adapt(db *sql.DB) *StdDB { return &StdDB{db} }

// StdDB wraps a *sql.DB
type StdDB struct{ db *sql.DB }

// Exec executes sql for a query that doesn't return any results
func (db *StdDB) Exec(ctx context.Context, q string, args ...interface{}) (Result, error) {
	res, err := db.db.ExecContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}

	// @TODO fold res into a Result implementation
	_ = res

	return nil, nil
}

// Query executes sql for a query that is expected to return rows
func (db *StdDB) Query(ctx context.Context, q string, args ...interface{}) (Result, error) {
	rows, err := db.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}

	// load all results in to the iterator. But how to call scan? How to close?
	_ = rows

	return nil, nil
}

// Tx starts a transaction
func (db *StdDB) Tx(ctx context.Context) (Tx, error) {
	tx, err := db.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}

	return &stdTx{tx}, nil
}

// stdTx wraps *sql.Tx while implementing this package's Tx interface
type stdTx struct{ tx *sql.Tx }

func (tx *stdTx) Query(ctx context.Context, q string, args ...interface{}) (Result, error) {
	rows, err := tx.tx.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	_ = rows // @TODO turn into result
	return nil, nil
}
func (tx *stdTx) Exec(ctx context.Context, q string, args ...interface{}) (Result, error) {
	res, err := tx.tx.ExecContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	_ = res // @TODO turn into Result
	return nil, nil
}
func (tx *stdTx) Commit() error   { return tx.tx.Commit() }
func (tx *stdTx) Rollback() error { return tx.tx.Rollback() }
func (tx *stdTx) ExecBatch(ctx context.Context, b *Batch) ([]Result, error) {
	stmt, err := tx.tx.PrepareContext(ctx, b.q)
	if err != nil {
		return nil, err
	}

	for _, args := range b.p {
		_, _ = args, stmt
	}

	return nil, nil
}

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

// ExecBatch sets up a prepared statement and runs the whole batch in it
func (db *StdDB) ExecBatch(ctx context.Context, b *Batch) ([]Result, error) {
	return batch(ctx, b, db.db.PrepareContext)
}

// Query executes sql for a query that is expected to return rows
func (db *StdDB) Query(ctx context.Context, q string, args ...interface{}) (Rows, error) {
	return db.db.QueryContext(ctx, q, args...)
}

// Exec executes sql for a query that doesn't return any results
func (db *StdDB) Exec(ctx context.Context, q string, args ...interface{}) (Result, error) {
	return db.db.ExecContext(ctx, q, args...)
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

func (tx *stdTx) Commit() error   { return tx.tx.Commit() }
func (tx *stdTx) Rollback() error { return tx.tx.Rollback() }

func (tx *stdTx) Exec(ctx context.Context, q string, args ...interface{}) (Result, error) {
	return tx.tx.ExecContext(ctx, q, args...)
}

func (tx *stdTx) Query(ctx context.Context, q string, args ...interface{}) (Rows, error) {
	return tx.tx.QueryContext(ctx, q, args...)
}

func (tx *stdTx) ExecBatch(ctx context.Context, b *Batch) ([]Result, error) {
	return batch(ctx, b, tx.tx.PrepareContext)
}

func batch(
	ctx context.Context,
	b *Batch,
	pf func(ctx context.Context, query string) (*sql.Stmt, error),
) ([]Result, error) {
	stmt, err := pf(ctx, b.sql)
	if err != nil {
		return nil, err
	}

	for _, args := range b.qrys {
		rows, err := stmt.Query(args...)
		if err != nil {
			// @TODO collect all errors?
		}
		_ = rows // @TODO turn rows into our result struct
	}

	for _, args := range b.exes {
		res, err := stmt.Exec(args...)
		if err != nil {
			// @TODO collect errors?
		}
		_ = res // @TODO turn into our our result struct
	}

	return nil, stmt.Close()
}

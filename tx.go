package dasql

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

// Tx represents a SQL transaction
type Tx interface {
	Query(ctx context.Context, q string, args ...interface{}) (Rows, error)
	Exec(ctx context.Context, q string, args ...interface{}) (Result, error)
	Commit() error
	Rollback() error
	ExecBatch(ctx context.Context, b *Batch) ([]Rows, error)
}

// daTx implements the Tx interface for the Data API
type daTx struct {
	id  string
	db  *DB
	ctx context.Context
}

// Query executes sql that expects to return rows inside of the transaction
func (tx daTx) Query(ctx context.Context, q string, args ...interface{}) (Rows, error) {
	return tx.db.query(ctx, tx.id, q, args...)
}

// Exec executes sql inside of the transaction
func (tx daTx) Exec(ctx context.Context, q string, args ...interface{}) (Result, error) {
	return tx.db.exec(ctx, tx.id, q, args...)
}

// ExecBatch executes the batch as part the transaction
func (tx daTx) ExecBatch(ctx context.Context, b *Batch) ([]Rows, error) {
	return tx.db.execBatch(ctx, tx.id, b)
}

// Commit the transaction
func (tx daTx) Commit() error {
	in := (&rdsdataservice.CommitTransactionInput{}).
		SetResourceArn(tx.db.resourceARN).
		SetSecretArn(tx.db.secretARN).
		SetTransactionId(tx.id)

	_, err := tx.db.da.CommitTransactionWithContext(tx.ctx, in)
	if err != nil {
		return fmt.Errorf("dasql: failed to commit transaction: %w", err)
	}

	return nil
}

// Roolback the transaction
func (tx daTx) Rollback() error {
	in := (&rdsdataservice.RollbackTransactionInput{}).
		SetResourceArn(tx.db.resourceARN).
		SetSecretArn(tx.db.secretARN).
		SetTransactionId(tx.id)

	_, err := tx.db.da.RollbackTransactionWithContext(tx.ctx, in)
	if err != nil {
		return fmt.Errorf("dasql: failed to rollback transaction: %w", err)
	}

	return nil
}

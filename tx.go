package dasql

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

// Tx represents a SQL transaction
type Tx interface {
	Exec(ctx context.Context, q string, args ...interface{}) (Result, error)
	Commit() error
	Rollback() error
}

// daTx implements the Tx interface for the Data API
type daTx struct {
	id  string
	db  *DB
	ctx context.Context
}

// Exec executes sql inside of the transaction
func (tx daTx) Exec(ctx context.Context, q string, args ...interface{}) (Result, error) {
	return tx.db.exec(ctx, tx.id, q, args...)
}

// Commit the transaction
func (tx daTx) Commit() error {
	var in rdsdataservice.CommitTransactionInput
	in.SetResourceArn(tx.db.resourceARN)
	in.SetSecretArn(tx.db.secretARN)
	in.SetTransactionId(tx.id)

	_, err := tx.db.da.CommitTransactionWithContext(tx.ctx, &in)
	if err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// Roolback the transaction
func (tx daTx) Rollback() error {
	var in rdsdataservice.RollbackTransactionInput
	in.SetResourceArn(tx.db.resourceARN)
	in.SetSecretArn(tx.db.secretARN)
	in.SetTransactionId(tx.id)

	_, err := tx.db.da.RollbackTransactionWithContext(tx.ctx, &in)
	if err != nil {
		return fmt.Errorf("failed to rollback transaction: %w", err)
	}

	return nil
}

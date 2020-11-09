package dasql

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

// DB can be used to execute SQL using the AWS Aurora Data API
type DB struct {
	secretARN   string
	resourceARN string

	da DA
}

// New initializes the database abstraction
func New(da DA, resourceARN, secretARN string) *DB {
	return &DB{secretARN, resourceARN, da}
}

// Tx begins a transaction. The provided context will be used for the duration of that transaction.
func (db *DB) Tx(ctx context.Context) (Tx, error) {
	var in rdsdataservice.BeginTransactionInput
	in.SetResourceArn(db.resourceARN)
	in.SetSecretArn(db.secretARN)

	out, err := db.da.BeginTransactionWithContext(ctx, &in)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &daTx{aws.StringValue(out.TransactionId), db}, nil
}

// Exec executes SQL.The args are for any placeholder parameters in the query.
func (db *DB) Exec(ctx context.Context, q string, args ...interface{}) (Result, error) {
	return db.exec(ctx, "", q, args...)
}

// exec is the private implementation that also works with a transaction
func (db *DB) exec(ctx context.Context, tid string, q string, args ...interface{}) (Result, error) {
	params, err := ConvertArgs(args...)
	if err != nil {
		return nil, fmt.Errorf("failed to convert arguments: %w", err)
	}

	var in rdsdataservice.ExecuteStatementInput
	in.SetResourceArn(db.resourceARN)
	in.SetSecretArn(db.secretARN)
	in.SetParameters(params)
	in.SetSql(q)
	if tid != "" {
		in.SetTransactionId(tid)
	}

	out, err := db.da.ExecuteStatementWithContext(ctx, &in)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statement: %w", err)
	}

	return &daResult{out}, nil
}

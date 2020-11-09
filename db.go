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
	in := (&rdsdataservice.BeginTransactionInput{}).
		SetResourceArn(db.resourceARN).
		SetSecretArn(db.secretARN)

	out, err := db.da.BeginTransactionWithContext(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("failed to begin transaction: %w", err)
	}

	return &daTx{aws.StringValue(out.TransactionId), db, ctx}, nil
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

	in := (&rdsdataservice.ExecuteStatementInput{}).
		SetResourceArn(db.resourceARN).
		SetSecretArn(db.secretARN).
		SetSql(q).
		SetParameters(params)

	if tid != "" {
		in.SetTransactionId(tid)
	}

	out, err := db.da.ExecuteStatementWithContext(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("failed to execute statement: %w", err)
	}

	return &daResult{out.GeneratedFields, out.Records}, nil
}

// ExecBatch will execute the batch
func (db *DB) ExecBatch(ctx context.Context, b *Batch) ([]Result, error) {
	return db.execBatch(ctx, "", b)
}

// execBatch is the private implementation for batching with support for doing it as part of a tx
func (db *DB) execBatch(ctx context.Context, tid string, b *Batch) (res []Result, err error) {
	params := make([][]*rdsdataservice.SqlParameter, len(b.p))
	for i, bp := range b.p {
		params[i], err = ConvertArgs(bp...)
		if err != nil {
			return nil, err
		}
	}

	in := (&rdsdataservice.BatchExecuteStatementInput{}).
		SetResourceArn(db.resourceARN).
		SetSecretArn(db.secretARN).
		SetSql(b.q).
		SetParameterSets(params)

	if tid != "" {
		in.SetTransactionId(tid)
	}

	out, err := db.da.BatchExecuteStatementWithContext(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("failed to batch execute statement: %w", err)
	}

	for _, upres := range out.UpdateResults {
		res = append(res, &daResult{genFields: upres.GeneratedFields})
	}

	return res, nil
}

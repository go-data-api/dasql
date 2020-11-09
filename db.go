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
		return nil, fmt.Errorf("dasql: failed to begin transaction: %w", err)
	}

	return &daTx{aws.StringValue(out.TransactionId), db, ctx}, nil
}

// Query queries SQL.The args are for any named parameters in the query.
func (db *DB) Query(ctx context.Context, q string, args ...interface{}) (Rows, error) {
	return db.query(ctx, "", q, args...)
}

// qury is the private implementation that also works with a transaction
func (db *DB) query(ctx context.Context, tid string, q string, args ...interface{}) (Rows, error) {
	params, err := ConvertArgs(args...)
	if err != nil {
		return nil, fmt.Errorf("dasql: failed to convert arguments: %w", err)
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
		return nil, fmt.Errorf("dasql: failed to execute statement: %w", err)
	}

	return &daRows{out.GeneratedFields, out.Records, -1}, nil
}

// Exec executes SQL.The args are for any named parameters in the query.
func (db *DB) Exec(ctx context.Context, q string, args ...interface{}) (Rows, error) {
	return db.exec(ctx, "", q, args...)
}

// exec is the private implementation that also works with a transaction
func (db *DB) exec(ctx context.Context, tid string, q string, args ...interface{}) (Rows, error) {
	return nil, nil
	// params, err := ConvertArgs(args...)
	// if err != nil {
	// 	return nil, fmt.Errorf("dasql: failed to convert arguments: %w", err)
	// }

	// in := (&rdsdataservice.ExecuteStatementInput{}).
	// 	SetResourceArn(db.resourceARN).
	// 	SetSecretArn(db.secretARN).
	// 	SetSql(q).
	// 	SetParameters(params)

	// if tid != "" {
	// 	in.SetTransactionId(tid)
	// }

	// out, err := db.da.ExecuteStatementWithContext(ctx, in)
	// if err != nil {
	// 	return nil, fmt.Errorf("dasql: failed to execute statement: %w", err)
	// }

	// return &daRows{out.GeneratedFields, out.Records, -1}, nil
}

// ExecBatch will execute the batch
func (db *DB) ExecBatch(ctx context.Context, b *Batch) ([]Rows, error) {
	return db.execBatch(ctx, "", b)
}

// execBatch is the private implementation for batching with support for doing it as part of a tx
func (db *DB) execBatch(ctx context.Context, tid string, b *Batch) (res []Rows, err error) {
	params := make([][]*rdsdataservice.SqlParameter, len(b.qrys)+len(b.exes))
	for i, bp := range append(b.qrys, b.exes...) {
		params[i], err = ConvertArgs(bp...)
		if err != nil {
			return nil, err
		}
	}

	in := (&rdsdataservice.BatchExecuteStatementInput{}).
		SetResourceArn(db.resourceARN).
		SetSecretArn(db.secretARN).
		SetSql(b.sql).
		SetParameterSets(params)

	if tid != "" {
		in.SetTransactionId(tid)
	}

	out, err := db.da.BatchExecuteStatementWithContext(ctx, in)
	if err != nil {
		return nil, fmt.Errorf("dasql: failed to batch execute statement: %w", err)
	}

	for _, upres := range out.UpdateResults {
		res = append(res, &daRows{genFields: upres.GeneratedFields, pos: -1})
	}

	return res, nil
}

package dasql

import (
	"context"

	"github.com/aws/aws-sdk-go/service/rdsdataservice"
	"github.com/aws/aws-sdk-go/service/rdsdataservice/rdsdataserviceiface"
)

// DB can be used to execute SQL using the AWS Aurora Data API
type DB struct {
	secretARN   string
	resourceARN string

	api rdsdataserviceiface.RDSDataServiceAPI
}

// New initializes the database abstraction
func New(api rdsdataserviceiface.RDSDataServiceAPI, resourceARN, secretARN string) *DB {
	return &DB{resourceARN, secretARN, api}
}

// Exec executes SQL.The args are for any placeholder parameters in the query.
func (db *DB) Exec(ctx context.Context, q string, args ...interface{}) (Result, error) {
	return db.exec(ctx, "", q, args...)
}

// exec is the private implementation that also works with a transaction
func (db *DB) exec(ctx context.Context, tid string, q string, args ...interface{}) (Result, error) {
	params, err := ConvertArgs(args...)
	if err != nil {
		return nil, err
	}

	var in rdsdataservice.ExecuteStatementInput
	in.SetResourceArn(db.resourceARN)
	in.SetSecretArn(db.resourceARN)
	in.SetParameters(params)
	in.SetSql(q)
	if tid != "" {
		in.SetTransactionId(tid)
	}

	// @TODO finish this

	return nil, nil
}

package dasql

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

// DA is a strict sub-set of the rdsdataservice package from the official Go AWS SDK. It excludes
// the deprecated parts and methods without a context.
type DA interface {
	ExecuteStatementWithContext(
		aws.Context,
		*rdsdataservice.ExecuteStatementInput,
		...request.Option) (*rdsdataservice.ExecuteStatementOutput, error)

	BeginTransactionWithContext(
		aws.Context,
		*rdsdataservice.BeginTransactionInput,
		...request.Option) (*rdsdataservice.BeginTransactionOutput, error)

	CommitTransactionWithContext(
		aws.Context,
		*rdsdataservice.CommitTransactionInput,
		...request.Option) (*rdsdataservice.CommitTransactionOutput, error)

	RollbackTransactionWithContext(
		aws.Context,
		*rdsdataservice.RollbackTransactionInput,
		...request.Option) (*rdsdataservice.RollbackTransactionOutput, error)

	BatchExecuteStatementWithContext(
		aws.Context,
		*rdsdataservice.BatchExecuteStatementInput,
		...request.Option) (*rdsdataservice.BatchExecuteStatementOutput, error)
}

package dasql

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

var _ DA = &stubDA{}

type stubDA struct{}

func (stubDA) ExecuteStatementWithContext(
	aws.Context,
	*rdsdataservice.ExecuteStatementInput,
	...request.Option) (out *rdsdataservice.ExecuteStatementOutput, err error) {
	return
}

func (stubDA) BeginTransactionWithContext(
	aws.Context,
	*rdsdataservice.BeginTransactionInput,
	...request.Option) (out *rdsdataservice.BeginTransactionOutput, err error) {
	return
}

func (stubDA) CommitTransactionWithContext(
	aws.Context,
	*rdsdataservice.CommitTransactionInput,
	...request.Option) (out *rdsdataservice.CommitTransactionOutput, err error) {
	return
}

func (stubDA) RollbackTransactionWithContext(
	aws.Context,
	*rdsdataservice.RollbackTransactionInput,
	...request.Option) (out *rdsdataservice.RollbackTransactionOutput, err error) {
	return
}

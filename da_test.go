package dasql

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

var _ DA = &stubDA{}

type stubDA struct {
	lastESI  *rdsdataservice.ExecuteStatementInput
	nextESO  *rdsdataservice.ExecuteStatementOutput
	nextESOE error

	lastBTI  *rdsdataservice.BeginTransactionInput
	nextBTO  *rdsdataservice.BeginTransactionOutput
	nextBTOE error
}

func (s *stubDA) ExecuteStatementWithContext(
	ctx aws.Context,
	in *rdsdataservice.ExecuteStatementInput,
	opts ...request.Option) (out *rdsdataservice.ExecuteStatementOutput, err error) {
	s.lastESI = in
	return s.nextESO, s.nextESOE
}

func (s *stubDA) BeginTransactionWithContext(
	ctx aws.Context,
	in *rdsdataservice.BeginTransactionInput,
	opts ...request.Option) (out *rdsdataservice.BeginTransactionOutput, err error) {
	s.lastBTI = in
	return s.nextBTO, s.nextBTOE
}

func (s *stubDA) CommitTransactionWithContext(
	aws.Context,
	*rdsdataservice.CommitTransactionInput,
	...request.Option) (out *rdsdataservice.CommitTransactionOutput, err error) {
	return
}

func (s *stubDA) RollbackTransactionWithContext(
	aws.Context,
	*rdsdataservice.RollbackTransactionInput,
	...request.Option) (out *rdsdataservice.RollbackTransactionOutput, err error) {
	return
}

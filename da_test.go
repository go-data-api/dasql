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

	lastCTI  *rdsdataservice.CommitTransactionInput
	nextCTO  *rdsdataservice.CommitTransactionOutput
	nextCTOE error

	lastRTI  *rdsdataservice.RollbackTransactionInput
	nextRTO  *rdsdataservice.RollbackTransactionOutput
	nextRTOE error

	lastBESI  *rdsdataservice.BatchExecuteStatementInput
	nextBESO  *rdsdataservice.BatchExecuteStatementOutput
	nextBESOE error
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
	ctx aws.Context,
	in *rdsdataservice.CommitTransactionInput,
	opts ...request.Option) (out *rdsdataservice.CommitTransactionOutput, err error) {
	s.lastCTI = in
	return s.nextCTO, s.nextCTOE
}

func (s *stubDA) RollbackTransactionWithContext(
	ctx aws.Context,
	in *rdsdataservice.RollbackTransactionInput,
	opts ...request.Option) (out *rdsdataservice.RollbackTransactionOutput, err error) {
	s.lastRTI = in
	return s.nextRTO, s.nextRTOE
}

func (s *stubDA) BatchExecuteStatementWithContext(
	ctx aws.Context,
	in *rdsdataservice.BatchExecuteStatementInput,
	opts ...request.Option) (*rdsdataservice.BatchExecuteStatementOutput, error) {
	s.lastBESI = in
	return s.nextBESO, s.nextBESOE
}

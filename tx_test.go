package dasql

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

func TestTxExec(t *testing.T) {
	da, ctx := &stubDA{nextESO: &rdsdataservice.ExecuteStatementOutput{}}, context.Background()
	db := New(da, "res", "sec")
	tx := &daTx{"1234", db, ctx}

	res, err := tx.Exec(ctx, `SELECT * FROM foo`)
	if err != nil {
		t.Fatalf("got: %v", err)
	}

	if act := aws.StringValue(da.lastESI.TransactionId); act != "1234" {
		t.Fatalf("got: %v", act)
	}

	if res == nil {
		t.Fatalf("got: %v", res)
	}
}

func TestTxCommit(t *testing.T) {
	da, ctx := &stubDA{}, context.Background()
	db := New(da, "res", "sec")
	tx := &daTx{"1234", db, ctx}

	err := tx.Commit()
	if err != nil {
		t.Fatalf("got: %v", err)
	}

	if act := aws.StringValue(da.lastCTI.ResourceArn); act != "res" {
		t.Fatalf("got: %v", act)
	}

	if act := aws.StringValue(da.lastCTI.SecretArn); act != "sec" {
		t.Fatalf("got: %v", act)
	}
}

func TestTxCommitErr(t *testing.T) {
	da, ctx := &stubDA{nextCTOE: awserr.New("400", "foo", nil)}, context.Background()
	tx := &daTx{"1234", New(da, "", ""), ctx}

	err := tx.Commit()
	if err == nil {
		t.Fatalf("got: %v", err)
	}

	if !strings.Contains(err.Error(), "commit") {
		t.Fatalf("got: %v", err)
	}

	var aerr awserr.Error
	if !errors.As(err, &aerr) {
		t.Fatalf("got: %T", err)
	}
}

func TestTxRollback(t *testing.T) {
	da, ctx := &stubDA{}, context.Background()
	db := New(da, "res", "sec")
	tx := &daTx{"1234", db, ctx}

	err := tx.Rollback()
	if err != nil {
		t.Fatalf("got: %v", err)
	}

	if act := aws.StringValue(da.lastRTI.ResourceArn); act != "res" {
		t.Fatalf("got: %v", act)
	}

	if act := aws.StringValue(da.lastRTI.SecretArn); act != "sec" {
		t.Fatalf("got: %v", act)
	}
}

func TestTxRollbackErr(t *testing.T) {
	da, ctx := &stubDA{nextRTOE: awserr.New("400", "foo", nil)}, context.Background()
	tx := &daTx{"1234", New(da, "", ""), ctx}

	err := tx.Rollback()
	if err == nil {
		t.Fatalf("got: %v", err)
	}

	if !strings.Contains(err.Error(), "rollback") {
		t.Fatalf("got: %v", err)
	}

	var aerr awserr.Error
	if !errors.As(err, &aerr) {
		t.Fatalf("got: %T", err)
	}
}

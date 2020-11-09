package dasql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

func TestDBExecOK(t *testing.T) {
	da, ctx := &stubDA{}, context.Background()
	db := New(da, "arn:aws:rds:", "arn:aws:secret:")
	query := `SELECT * FROM foo WHERE bar = :fbar`

	res, err := db.Exec(ctx, query, sql.Named("fbar", "foo"))
	if err != nil {
		t.Fatalf("got: %v", err)
	}

	if act := aws.StringValue(da.lastESI.ResourceArn); act != "arn:aws:rds:" {
		t.Fatalf("got: %v", act)
	}

	if act := aws.StringValue(da.lastESI.SecretArn); act != "arn:aws:secret:" {
		t.Fatalf("got: %v", act)
	}

	if act := aws.StringValue(da.lastESI.Sql); act != query {
		t.Fatalf("got: %v", act)
	}

	if len(da.lastESI.Parameters) < 1 || aws.StringValue(da.lastESI.Parameters[0].Name) != `fbar` ||
		aws.StringValue(da.lastESI.Parameters[0].Value.StringValue) != `foo` {
		t.Fatalf("got: %s", da.lastESI.Parameters)
	}

	if res == nil {
		t.Fatalf("got: %v", res)
	}
}

func TestDBExecArgErr(t *testing.T) {
	_, err := New(nil, "", "").Exec(nil, ``, sql.Named("bogus", func() {}))
	if err == nil {
		t.Fatalf("got: %v", err)
	}

	var aerr ArgErr
	if !errors.As(err, &aerr) {
		t.Fatalf("got: %T", err)
	}
}

func TestDBExecArgAwsErr(t *testing.T) {
	da := &stubDA{nextESOE: awserr.New("400", "foo", nil)}

	_, err := New(da, "", "").Exec(nil, ``, sql.Named("foo", "bar"))
	if err == nil {
		t.Fatalf("got: %v", err)
	}

	var aerr awserr.Error
	if !errors.As(err, &aerr) {
		t.Fatalf("got: %T", err)
	}
}

func TestDBBeginOK(t *testing.T) {
	nestBTO := &rdsdataservice.BeginTransactionOutput{TransactionId: aws.String("1234")}
	da, ctx := &stubDA{nextBTO: nestBTO}, context.Background()
	db := New(da, "arn:aws:rds:", "arn:aws:secret:")
	tx, err := db.Tx(ctx)
	if err != nil {
		t.Fatalf("got: %v", err)
	}

	if act := aws.StringValue(da.lastBTI.ResourceArn); act != "arn:aws:rds:" {
		t.Fatalf("got: %v", act)
	}

	if act := aws.StringValue(da.lastBTI.SecretArn); act != "arn:aws:secret:" {
		t.Fatalf("got: %v", act)
	}

	if tx == nil || tx.(*daTx).id != "1234" || tx.(*daTx).ctx != ctx {
		t.Fatalf("got: %v", tx)
	}
}

func TestDBBeginAwsErr(t *testing.T) {
	da := &stubDA{nextBTOE: awserr.New("400", "foo", nil)}

	_, err := New(da, "", "").Tx(nil)
	if err == nil {
		t.Fatalf("got: %v", err)
	}

	var aerr awserr.Error
	if !errors.As(err, &aerr) {
		t.Fatalf("got: %T", err)
	}
}

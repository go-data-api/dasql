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
	da, ctx := &stubDA{nextESO: &rdsdataservice.ExecuteStatementOutput{}}, context.Background()
	db := New(da, "arn:aws:rds:", "arn:aws:secret:")
	query := `SELECT * FROM foo WHERE bar = :fbar`

	// exec an query do the same thing with the data api
	for i := 0; i < 2; i++ {
		var res Result
		var err error
		if i == 0 {
			res, err = db.Exec(ctx, query, sql.Named("fbar", "foo"))
		} else {
			res, err = db.Query(ctx, query, sql.Named("fbar", "foo"))
		}

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

func TestDBBatch(t *testing.T) {
	beso := &rdsdataservice.BatchExecuteStatementOutput{
		UpdateResults: []*rdsdataservice.UpdateResult{
			{GeneratedFields: []*rdsdataservice.Field{}}, {}},
	}
	da, ctx := &stubDA{nextBESO: beso}, context.Background()
	db := New(da, "arn:aws:rds:", "arn:aws:secret:")

	b := NewBatch(`UPDATE * WHERE bar = :foos`).
		Exec(sql.Named("foo", "foo1")).
		Query(sql.Named("foo", "foo1"))

	res, err := db.ExecBatch(ctx, b)
	if err != nil {
		t.Fatalf("got: %v", err)
	}

	if len(res) != 2 || res[0].(*daResult).genFields == nil {
		t.Fatalf("got: %v", res)
	}
}

func TestDBBatchArgErr(t *testing.T) {
	b := NewBatch(`UPDATE * WHERE bar = :foos`).Query(sql.Named("foo", func() {}))

	_, err := New(nil, "", "").ExecBatch(nil, b)
	if err == nil {
		t.Fatalf("got: %v", err)
	}

	var aerr ArgErr
	if !errors.As(err, &aerr) {
		t.Fatalf("got: %T", err)
	}
}

func TestDBBatchAwsErr(t *testing.T) {
	da := &stubDA{nextBESOE: awserr.New("400", "foo", nil)}
	b := NewBatch(`UPDATE * WHERE bar = :foos`).Exec(sql.Named("foo", "bar"))

	_, err := New(da, "", "").ExecBatch(nil, b)
	if err == nil {
		t.Fatalf("got: %v", err)
	}

	var aerr awserr.Error
	if !errors.As(err, &aerr) {
		t.Fatalf("got: %T", err)
	}
}

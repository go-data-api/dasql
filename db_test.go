package dasql

import (
	"context"
	"database/sql"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
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
	_, err := New(nil, "", "").Exec(nil, ``, sql.Named("fbar", func() {}))
	if err == nil {
		t.Fatalf("got: %v", err)
	}

	var aerr ArgErr
	if !errors.As(err, &aerr) {
		t.Fatalf("got: %T", err)
	}
}

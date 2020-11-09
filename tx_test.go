package dasql

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
)

func TestTxExec(t *testing.T) {
	da, ctx := &stubDA{}, context.Background()
	db := New(da, "res", "sec")
	tx := &daTx{"1234", db}

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

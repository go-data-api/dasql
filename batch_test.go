package dasql

import "testing"

func TestBatchExec(t *testing.T) {
	b := NewBatch(`UPDATE foo`).
		Exec("foo", "bar").
		Exec(1234, 1.3).
		Query(0.1, "bar")

	if b.sql != `UPDATE foo` {
		t.Fatalf("got: %v", b.sql)
	}

	if len(b.exes) != 2 || b.exes[0][1].(string) != "bar" || b.exes[1][0].(int) != 1234 {
		t.Fatalf("got: %v", b.exes)
	}

	if len(b.qrys) != 1 || b.qrys[0][0].(float64) != 0.1 {
		t.Fatalf("got: %v", b.qrys)
	}
}

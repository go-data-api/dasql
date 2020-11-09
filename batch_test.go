package dasql

import "testing"

func TestBatchExec(t *testing.T) {
	b := NewBatch(`UPDATE foo`)
	b.Append("foo", "bar").Append(1234, 1.3)

	if b.q != `UPDATE foo` {
		t.Fatalf("got: %v", b.q)
	}

	if len(b.p) != 2 || b.p[0][1].(string) != "bar" || b.p[1][0].(int) != 1234 {
		t.Fatalf("got: %v", b.p)
	}
}

package dasql

import "testing"

func TestResult(t *testing.T) {
	res := &daResult{numRecordsUpdated: 100}

	n, err := res.RowsAffected()
	if err != nil {
		t.Fatalf("got: %v", err)
	}

	if n != 100 {
		t.Fatalf("got: %v", n)
	}
}

package dasql

import (
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

func TestRowScan(t *testing.T) {
	res := &daRows{recs: [][]*rdsdataservice.Field{{{StringValue: aws.String("foo")}}}, pos: -1}

	var s1 string
	for res.Next() {
		if err := res.Scan(&s1); err != nil {
			t.Fatalf("got: %v", err)
		}
	}

	if s1 != "foo" {
		t.Fatalf("got: %v", s1)
	}

	if err := res.Close(); err != nil {
		t.Fatalf("got: %v", err)
	}
}

func TestRowScanErrors(t *testing.T) {
	t.Run("next not called", func(t *testing.T) {
		err := (&daRows{pos: -1}).Scan()
		if err == nil || !strings.Contains(err.Error(), "next") {
			t.Fatalf("got: %v", err)
		}
	})

	t.Run("out-of-range", func(t *testing.T) {
		err := (&daRows{}).Scan()
		if err == nil || !strings.Contains(err.Error(), "out-of-range") {
			t.Fatalf("got: %v", err)
		}
	})

	t.Run("out-of-range", func(t *testing.T) {
		err := (&daRows{recs: [][]*rdsdataservice.Field{{}}}).Scan(nil, nil)
		if err == nil || !strings.Contains(err.Error(), "not enough") {
			t.Fatalf("got: %v", err)
		}
	})
}

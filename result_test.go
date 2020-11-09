package dasql

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

func TestResultScan(t *testing.T) {
	res := &daResult{recs: [][]*rdsdataservice.Field{{{StringValue: aws.String("foo")}}}, pos: -1}

	var s1 string
	for res.Next() {
		if err := res.Scan(&s1); err != nil {
			t.Fatalf("got: %v", err)
		}
	}

	if s1 != "foo" {
		t.Fatalf("got: %v", s1)
	}
}

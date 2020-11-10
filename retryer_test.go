package dasql

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

func TestRetryer(t *testing.T) {
	var r Retryer

	if act := r.ShouldRetry(&request.Request{}); act == true {
		t.Fatalf("got: %v", act)
	}

	err := &rdsdataservice.BadRequestException{
		Message_: aws.String("Communications link failure: foo")}

	if act := r.ShouldRetry(&request.Request{Error: err}); act != true {
		t.Fatalf("got: %v", act)
	}
}

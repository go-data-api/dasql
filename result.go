package dasql

import "github.com/aws/aws-sdk-go/service/rdsdataservice"

// Result represents the results of an SQL execution.
type Result interface{}

// daResult implements the Result interface for the Data API
type daResult struct {
	out *rdsdataservice.ExecuteStatementOutput
}

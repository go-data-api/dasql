package dasql

import "github.com/aws/aws-sdk-go/service/rdsdataservice"

// Result summarizes an executed sql command
type Result interface{}

type daResult struct {
	generatedFields   []*rdsdataservice.Field
	numRecordsUpdated int64
}

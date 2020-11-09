package dasql

import "github.com/aws/aws-sdk-go/service/rdsdataservice"

// Result represents the results of an SQL execution.
type Result interface {
	Next() bool
	Scan(dest ...interface{}) (err error)
}

// daResult implements the Result interface for the Data API
type daResult struct {
	genFields []*rdsdataservice.Field
	recs      [][]*rdsdataservice.Field
	pos       int
}

// Next will prepare the next results for scanning
func (r *daResult) Next() bool {
	r.pos++
	return r.pos < len(r.recs)
}

// Scan the current result set
func (r *daResult) Scan(dest ...interface{}) (err error) {
	// @TODO assert that r.pos >= 0
	// @TODO assert that len(dest) == len(recs)

	return Scan(r.recs[r.pos], dest...)
}

package dasql

import (
	"errors"

	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

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
	switch {
	case r.pos < 0:
		return errors.New("dasql: scan called before next")
	case r.pos > len(r.recs)-1:
		return errors.New("dasql: scan called out-of-range")
	case len(r.recs[r.pos]) != len(dest):
		return errors.New("dasql: not enough arguments to scan row")
	}

	return Scan(r.recs[r.pos], dest...)
}

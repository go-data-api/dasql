package dasql

import (
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

// Result summarizes an executed sql command
type Result interface {
	LastInsertId() (int64, error)
	RowsAffected() (int64, error)
}

type daResult struct {
	generatedFields   []*rdsdataservice.Field
	numRecordsUpdated int64
}

// LastInsertID returns the last numeric id that was generated by the database.
func (r daResult) LastInsertId() (int64, error) {

	// mysql only allows one auto_increment per table, and postgres doesn't use this feature
	// at all. So it is unclear when the data api would return more then one field, for now
	// well will find the first field that has a "long" type and assume that this is the
	// last inserted id
	for _, f := range r.generatedFields {
		if f.LongValue != nil {
			return aws.Int64Value(f.LongValue), nil
		}
	}

	// @TODO what does a regular mysql return if no id was produced?
	return 0, nil
}

// RowsAffected return the nr of effected rows
func (r daResult) RowsAffected() (int64, error) {
	return r.numRecordsUpdated, nil
}

package dasql

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

// ScanErr describes a failure to scan
type ScanErr struct {
	Kind       ScanErrKind
	err        error
	Row, Field int
}

// ScanErrKind indicates certain behaviour
type ScanErrKind int

const (
	// ScanErrKindUnkown is an unkown scan error
	ScanErrKindUnknown ScanErrKind = iota

	// ScanErrKindUnsupported means that a certain field or record (type) is not supported
	ScanErrKindUnsupported

	// ScanErrKindTypeMismatch means that the type was supported but not used correctly
	ScanErrKindTypeMismatch

	// ScanErrKindNextNotCalled is returned when scan is called without calling Next() first
	ScanErrKindNextNotCalled

	// ScanErrKindRowOutOfRange is returned when the row index is larger than the nr of rows
	ScanErrKindRowOutOfRange

	// ScanErrTooManyFields is returned when there are more fields then scan values
	ScanErrTooManyFields
)

// Unwrap returns the underlying error, if any
func (se ScanErr) Unwrap() error {
	return se.err
}

// Error implements the error interface
func (se ScanErr) Error() string {
	return fmt.Sprintf("failed to scan field %d of row %d: %v", se.Field, se.Row, se.err)
}

// Scan copies fields frow row 'row' into the valuees pointed to by 'dest'. The number of values in
// dest must be the same as the number of columns in the row.
func Scan(row []*rdsdataservice.Field, dest ...interface{}) (err error) {
	for i, f := range row {
		err = scanField(f, dest[i])
		if err != nil {
			return
		}
	}

	return
}

// scanField will attempt to scan the provided field into 'dst' if it has a supported type
func scanField(src *rdsdataservice.Field, dst interface{}) (err error) {
	var expType string

	switch {
	case src.IsNull != nil && *src.IsNull == true:
		expType = "sql.Scanner"
		if dt, ok := dst.(sql.Scanner); ok {
			return dt.Scan(nil)
		}

	case src.ArrayValue != nil:
		return scanArrayValue(src.ArrayValue, dst, 0)

	case src.StringValue != nil:
		expType = "*string,sql.Scanner"
		switch p := dst.(type) {
		case *string:
			*p, expType = aws.StringValue(src.StringValue), ""
		case sql.Scanner:
			return p.Scan(*src.StringValue)
		}

	case src.DoubleValue != nil:
		expType = "*float64,sql.Scanner"
		switch p := dst.(type) {
		case *float64:
			*p, expType = aws.Float64Value(src.DoubleValue), ""
		case sql.Scanner:
			return p.Scan(*src.DoubleValue)
		}

	case src.LongValue != nil:
		expType = "*int64,sql.Scanner"
		switch p := dst.(type) {
		case *int64:
			*p, expType = aws.Int64Value(src.LongValue), ""
		case sql.Scanner:
			return p.Scan(*src.LongValue)
		}

	case src.BooleanValue != nil:
		expType = "*bool,sql.Scanner"
		switch p := dst.(type) {
		case *bool:
			*p, expType = aws.BoolValue(src.BooleanValue), ""
		case sql.Scanner:
			return p.Scan(*src.BooleanValue)
		}

	case src.BlobValue != nil:
		expType = "*[]byte,*sql.RawBytes,sql.Scanner"
		switch dt := dst.(type) {
		case *sql.RawBytes:
			*dt, expType = src.BlobValue, ""
		case *[]byte:
			*dt, expType = cloneBytes(src.BlobValue), ""
		case sql.Scanner:
			return dt.Scan(src.BlobValue)
		}

	default:
		return ScanErr{ScanErrKindUnsupported,
			fmt.Errorf("unsupported field: %s", src.String()), 0, 0}
	}

	if expType != "" {
		return ScanErr{ScanErrKindTypeMismatch,
			fmt.Errorf("invalid type, expected: %s, got: %T", expType, dst), 0, 0}
	}

	return nil
}

func cloneBytes(b []byte) []byte {
	if b == nil {
		return nil
	}
	c := make([]byte, len(b))
	copy(c, b)
	return c
}

// ScanArrayFieldErr is returned from scan when scanning an array field fails
type ScanArrayFieldErr struct {
	Kind             ScanErrKind // stringified array value
	ExpType, ActType string      // type conflict, if any
	Depth            int         // depth in multi-dimensional array
}

// Is implements error comparison
func (e ScanArrayFieldErr) Is(other error) bool {
	ee, ok := other.(ScanArrayFieldErr)
	if !ok {
		return false
	}

	return ee.Kind == e.Kind &&
		ee.ExpType == e.ExpType &&
		ee.ActType == e.ActType &&
		ee.Depth == e.Depth
}

// Error implements the standard error interface
func (e ScanArrayFieldErr) Error() string {
	switch e.Kind {
	case ScanErrKindTypeMismatch:
		return fmt.Sprintf("expected type '%s' but got '%s' at depth '%d'",
			e.ExpType, e.ActType, e.Depth)
	case ScanErrKindUnsupported:
		return fmt.Sprintf("unsupported value at depth: %d", e.Depth)
	default:
		return fmt.Sprintf("failed to scan at depth: %v", e.Depth)
	}
}

// scanArrayValue allows scanning of array values as supported by Postgres for example. It uses
// a small bit reflection for multi-dimensional arrays
func scanArrayValue(av *rdsdataservice.ArrayValue, dst interface{}, depth int) (err error) {
	switch {
	case av.ArrayValues != nil:
		rv := reflect.ValueOf(dst)
		if rv.Kind() == reflect.Ptr {
			rv = rv.Elem()
		}

		if !rv.CanSet() {
			return ScanArrayFieldErr{ScanErrKindTypeMismatch, "*[],[]", rv.Type().String(), depth}
		}

		// adjust array size to wat is needed to hold the field size
		if rv.Len() < len(av.ArrayValues) {
			rv.Set(reflect.MakeSlice(
				rv.Type(), len(av.ArrayValues), len(av.ArrayValues)))
		}

		// recurse for each array value
		for i, av := range av.ArrayValues {
			err = scanArrayValue(av, rv.Index(i).Addr().Interface(), depth+1)
			if err != nil {
				return
			}
		}

		return
	case av.StringValues != nil:
		dstv, ok := dst.(*[]string)
		if !ok {
			return ScanArrayFieldErr{ScanErrKindTypeMismatch, "*[]string", reflect.TypeOf(dst).String(), depth}
		}

		vs := make([]string, len(av.StringValues))
		for i, v := range av.StringValues {
			vs[i] = aws.StringValue(v)
		}

		*dstv = vs
		return
	case av.LongValues != nil:
		dstv, ok := dst.(*[]int64)
		if !ok {
			return ScanArrayFieldErr{ScanErrKindTypeMismatch, "*[]int64", reflect.TypeOf(dst).String(), depth}
		}

		vs := make([]int64, len(av.LongValues))
		for i, v := range av.LongValues {
			vs[i] = aws.Int64Value(v)
		}

		*dstv = vs
		return
	case av.DoubleValues != nil:
		dstv, ok := dst.(*[]float64)
		if !ok {
			return ScanArrayFieldErr{ScanErrKindTypeMismatch, "*[]float64", reflect.TypeOf(dst).String(), depth}
		}

		vs := make([]float64, len(av.DoubleValues))
		for i, v := range av.DoubleValues {
			vs[i] = aws.Float64Value(v)
		}

		*dstv = vs
		return
	case av.BooleanValues != nil:
		dstv, ok := dst.(*[]bool)
		if !ok {
			return ScanArrayFieldErr{ScanErrKindTypeMismatch, "*[]bool", reflect.TypeOf(dst).String(), depth}
		}

		vs := make([]bool, len(av.BooleanValues))
		for i, v := range av.BooleanValues {
			vs[i] = aws.BoolValue(v)
		}

		*dstv = vs
		return
	default:
		return ScanArrayFieldErr{Kind: ScanErrKindUnsupported, Depth: depth}
	}
}

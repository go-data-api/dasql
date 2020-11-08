package dasql

import (
	"database/sql"
	"fmt"
	"reflect"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

// ArgErr represents an erro the convert arguments int Data API parameters
type ArgErr struct {
	Kind ArgErrKind
	Type string
}

func (e ArgErr) Error() string {
	switch e.Kind {
	case ArgErrKindUnsupported:
		return fmt.Sprintf("unsupported argument type, got: %v", e.Type)
	default:
		return "error while converting argument"
	}
}

// ArgErrKind describes the kind of error retruned from converting args
type ArgErrKind int

const (
	// ArgErrKindUnknown is an arg error that has an unspecified kind
	ArgErrKindUnknown ArgErrKind = iota

	// ArgErrKindUnsupported is returned when an unsupported arg type is returned
	ArgErrKindUnsupported
)

// ConvertArgs converts the provided named arguments into a slice of rds data parameters. It
// currently only supports sql.NamedArg values but this might change in the future.
func ConvertArgs(args ...interface{}) (ps []*rdsdataservice.SqlParameter, err error) {
	ps = make([]*rdsdataservice.SqlParameter, 0, len(args))
	for _, arg := range args {
		named, ok := arg.(sql.NamedArg)
		if !ok {
			return nil, ArgErr{ArgErrKindUnsupported, reflect.ValueOf(arg).Type().String()}
		}

		field, hint, err := convertArg(named.Value)
		if err != nil {
			return nil, err
		}

		var p rdsdataservice.SqlParameter
		p.SetName(named.Name)
		p.SetValue(field)
		if hint != "" {
			p.SetTypeHint(hint)
		}

		ps = append(ps, &p)
	}

	return
}

// convertArg converts the provided arg into a parameter field and an optional hint
func convertArg(arg interface{}) (f *rdsdataservice.Field, hint string, err error) {
	f = &rdsdataservice.Field{}

	switch at := arg.(type) {
	case nil:
		f.IsNull = aws.Bool(true)
	case string:
		f.StringValue = aws.String(at)
	case int:
		f.LongValue = aws.Int64(int64(at))
	case int64:
		f.LongValue = aws.Int64(at)
	case float64:
		f.DoubleValue = aws.Float64(at)
	case bool:
		f.BooleanValue = aws.Bool(at)
	case []byte:
		f.BlobValue = cloneBytes(at)
	case sql.RawBytes:
		f.BlobValue = at
	default:

		// try it as a multi-dimentsional array they
		f.ArrayValue, err = convertArrayArg(arg)
		if err != nil {
			return nil, "", err
		}
	}

	return
}

// convertArrayArg will try to convert 'arg' into a 'multi-dimensional' array value for use as
// as a sql parameter for the data api
func convertArrayArg(arg interface{}) (av *rdsdataservice.ArrayValue, err error) {
	av = &rdsdataservice.ArrayValue{}
	switch at := arg.(type) {
	case []string:
		av.StringValues = aws.StringSlice(at)
	case []int64:
		av.LongValues = aws.Int64Slice(at)
	case []float64:
		av.DoubleValues = aws.Float64Slice(at)
	case []bool:
		av.BooleanValues = aws.BoolSlice(at)
	default:
		rv := reflect.ValueOf(arg)
		if rv.Kind() != reflect.Array && rv.Kind() != reflect.Slice {
			return nil, ArgErr{ArgErrKindUnsupported, rv.Type().String()}
		}

		av.ArrayValues = make([]*rdsdataservice.ArrayValue, rv.Len())
		for i := 0; i < rv.Len(); i++ {
			av.ArrayValues[i], err = convertArrayArg(rv.Index(i).Interface())
		}
	}

	return
}

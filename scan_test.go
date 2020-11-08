package db

import (
	"database/sql"
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/rdsdataservice"
)

func TestScan(t *testing.T) {
	t.Run("No next", func(t *testing.T) {
		err := scan(-1, nil)
		if serr, ok := err.(ScanErr); !ok || serr.Kind != ScanErrKindNextNotCalled {
			t.Fatalf("got: %v", err)
		}
	})

	t.Run("Out of Range", func(t *testing.T) {
		err := scan(0, nil)
		if serr, ok := err.(ScanErr); !ok || serr.Kind != ScanErrKindRowOutOfRange {
			t.Fatalf("got: %v", err)
		}
	})

	t.Run("Too many fields", func(t *testing.T) {
		err := scan(0, [][]*rdsdataservice.Field{{{}}})
		if serr, ok := err.(ScanErr); !ok || serr.Kind != ScanErrTooManyFields {
			t.Fatalf("got: %v", err)
		}
	})

	t.Run("Mismatch", func(t *testing.T) {
		var dst1, dst2 float64
		err := scan(1, [][]*rdsdataservice.Field{{}, {{DoubleValue: aws.Float64(1.1)}, {StringValue: aws.String("foo")}}}, &dst1, &dst2)
		serr, ok := err.(ScanErr)
		if !ok {
			t.Fatalf("got: %v", err)
		}

		if serr.Row != 1 || serr.Field != 1 {
			t.Fatalf("got: %v", serr)
		}

		if !strings.Contains(serr.Error(), "*float64") {
			t.Fatalf("got: %v", err.Error())
		}
	})

	t.Run("Valid", func(t *testing.T) {
		var dst string
		err := scan(0, [][]*rdsdataservice.Field{{{StringValue: aws.String("foo")}}}, &dst)
		if err != nil {
			t.Fatalf("got: %v", err)
		}

		if dst != "foo" {
			t.Fatalf("got: %v", dst)
		}
	})
}

func TestScanFieldBlobRefCopy(t *testing.T) {
	t.Run("copy", func(t *testing.T) {
		f := &rdsdataservice.Field{BlobValue: []byte{0x01}}
		var dst []byte
		if err := scanField(f, &dst); err != nil {
			t.Fatal(err)
		}

		f.BlobValue[0] = 0x02

		if len(dst) < 1 || dst[0] == 0x02 {
			t.Fatalf("should have copied bytes")
		}
	})

	t.Run("ref", func(t *testing.T) {
		f := &rdsdataservice.Field{BlobValue: []byte{0x01}}
		var dst sql.RawBytes
		if err := scanField(f, &dst); err != nil {
			t.Fatal(err)
		}

		f.BlobValue[0] = 0x02

		if len(dst) < 1 || dst[0] != 0x02 {
			t.Fatalf("should not have copied bytes")
		}
	})
}

type nopScan struct{ v interface{} }

func (s *nopScan) Scan(v interface{}) error { s.v = v; return nil }

func TestScanField(t *testing.T) {
	for i, c := range []struct {
		f          *rdsdataservice.Field
		dst        interface{}
		exp        interface{}
		expErrKind ScanErrKind
	}{
		{
			dst: aws.String(""),
			exp: "foo",
			f:   &rdsdataservice.Field{StringValue: aws.String("foo")},
		},
		{
			dst: aws.Float64(0.0),
			exp: 1.345,
			f:   &rdsdataservice.Field{DoubleValue: aws.Float64(1.345)},
		},
		{
			dst: aws.Int64(0),
			exp: int64(12345),
			f:   &rdsdataservice.Field{LongValue: aws.Int64(12345)},
		},
		{
			dst: aws.Bool(false),
			exp: true,
			f:   &rdsdataservice.Field{BooleanValue: aws.Bool(true)},
		},

		{
			dst: &sql.RawBytes{},
			exp: sql.RawBytes{0x01},
			f:   &rdsdataservice.Field{BlobValue: []byte{0x01}},
		},
		{
			dst: &[]byte{},
			exp: []byte{0x02},
			f:   &rdsdataservice.Field{BlobValue: []byte{0x02}},
		},
		{
			dst: &[]string{},
			exp: []string{"foo", "bar"},
			f: &rdsdataservice.Field{ArrayValue: &rdsdataservice.ArrayValue{
				StringValues: aws.StringSlice([]string{"foo", "bar"})}},
		},

		{
			dst: &sql.NullString{},
			exp: sql.NullString{String: "foo", Valid: true},
			f:   &rdsdataservice.Field{StringValue: aws.String("foo")},
		},
		{
			dst: &sql.NullFloat64{},
			exp: sql.NullFloat64{Float64: 1.345, Valid: true},
			f:   &rdsdataservice.Field{DoubleValue: aws.Float64(1.345)},
		},
		{
			dst: &sql.NullInt64{},
			exp: sql.NullInt64{Int64: 12345, Valid: true},
			f:   &rdsdataservice.Field{LongValue: aws.Int64(12345)},
		},
		{
			dst: &sql.NullBool{},
			exp: sql.NullBool{Bool: true, Valid: true},
			f:   &rdsdataservice.Field{BooleanValue: aws.Bool(true)},
		},
		{
			dst: &nopScan{},
			exp: nopScan{[]byte{0x02}},
			f:   &rdsdataservice.Field{BlobValue: []byte{0x02}},
		},
		{
			dst: &sql.NullString{},
			exp: sql.NullString{String: "", Valid: false},
			f:   &rdsdataservice.Field{StringValue: aws.String("foo"), IsNull: aws.Bool(true)},
		},
		{
			dst:        "",
			f:          &rdsdataservice.Field{StringValue: aws.String("foo")},
			expErrKind: ScanErrKindTypeMismatch,
		},
		{
			f:          &rdsdataservice.Field{},
			expErrKind: ScanErrKindUnsupported,
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			err := scanField(c.f, c.dst)
			if serr, ok := err.(ScanErr); ok && serr.Kind != c.expErrKind {
				t.Fatalf("exp: %v got: %v", c.expErrKind, err)
			}

			if err == nil {
				indir := reflect.Indirect(reflect.ValueOf(c.dst)).Interface()
				if !reflect.DeepEqual(indir, c.exp) {
					t.Fatalf("exp: %v (%T), got: %v (%T)", c.exp, c.exp, indir, indir)
				}
			}
		})
	}
}

func TestScanArrayValues(t *testing.T) {
	for i, c := range []struct {
		av     *rdsdataservice.ArrayValue
		dst    interface{}
		exp    interface{}
		expErr error
	}{
		{
			dst: &[]string{},
			exp: []string{"foo", "bar"},
			av:  &rdsdataservice.ArrayValue{StringValues: aws.StringSlice([]string{"foo", "bar"})},
		},
		{
			dst: &[]int64{},
			exp: []int64{199, 1},
			av:  &rdsdataservice.ArrayValue{LongValues: aws.Int64Slice([]int64{199, 1})},
		},
		{
			dst: &[]float64{},
			exp: []float64{0.1, 2.4},
			av:  &rdsdataservice.ArrayValue{DoubleValues: aws.Float64Slice([]float64{0.1, 2.4})},
		},
		{
			dst: &[]bool{},
			exp: []bool{true, false},
			av:  &rdsdataservice.ArrayValue{BooleanValues: aws.BoolSlice([]bool{true, false})},
		},
		{
			dst: &[][]string{},
			exp: [][]string{{"foo", "bar"}},
			av: &rdsdataservice.ArrayValue{ArrayValues: []*rdsdataservice.ArrayValue{
				{StringValues: aws.StringSlice([]string{"foo", "bar"})},
			}},
		},

		{
			av:     &rdsdataservice.ArrayValue{},
			expErr: ScanArrayFieldErr{Kind: ScanErrKindUnsupported, Depth: 0},
		},

		{
			av:  &rdsdataservice.ArrayValue{LongValues: aws.Int64Slice(nil)},
			dst: "",
			expErr: ScanArrayFieldErr{
				Kind:    ScanErrKindTypeMismatch,
				ExpType: "*[]int64", ActType: "string",
				Depth: 0,
			},
		},
		{
			av:  &rdsdataservice.ArrayValue{BooleanValues: aws.BoolSlice(nil)},
			dst: "",
			expErr: ScanArrayFieldErr{
				Kind:    ScanErrKindTypeMismatch,
				ExpType: "*[]bool", ActType: "string",
				Depth: 0,
			},
		},
		{
			av:  &rdsdataservice.ArrayValue{DoubleValues: aws.Float64Slice(nil)},
			dst: "",
			expErr: ScanArrayFieldErr{
				Kind:    ScanErrKindTypeMismatch,
				ExpType: "*[]float64", ActType: "string",
				Depth: 0,
			},
		},
		{
			av:  &rdsdataservice.ArrayValue{StringValues: aws.StringSlice([]string{"foo", "bar"})},
			dst: "",
			expErr: ScanArrayFieldErr{
				Kind:    ScanErrKindTypeMismatch,
				ExpType: "*[]string", ActType: "string",
				Depth: 0,
			},
		},

		{
			av: &rdsdataservice.ArrayValue{ArrayValues: []*rdsdataservice.ArrayValue{
				{StringValues: aws.StringSlice(nil)},
			}},
			dst: "",
			expErr: ScanArrayFieldErr{
				Kind:    ScanErrKindTypeMismatch,
				ExpType: "*[],[]", ActType: "string",
				Depth: 0,
			},
		},
		{
			av: &rdsdataservice.ArrayValue{ArrayValues: []*rdsdataservice.ArrayValue{
				{StringValues: aws.StringSlice([]string{"foo", "bar"})},
			}},
			dst: [][]int{},
			expErr: ScanArrayFieldErr{
				Kind:    ScanErrKindTypeMismatch,
				ExpType: "*[],[]", ActType: "[][]int",
				Depth: 0,
			},
		},
		{
			av: &rdsdataservice.ArrayValue{ArrayValues: []*rdsdataservice.ArrayValue{
				{StringValues: aws.StringSlice([]string{"foo", "bar"})},
			}},
			dst: &[][]int{},
			expErr: ScanArrayFieldErr{
				Kind:    ScanErrKindTypeMismatch,
				ExpType: "*[]string", ActType: "*[]int",
				Depth: 1,
			},
		},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			err := scanArrayValue(c.av, c.dst, 0)
			if !errors.Is(err, c.expErr) {
				t.Fatalf("exp: %v got: %v", c.expErr, err)
			}

			if err == nil {
				indir := reflect.Indirect(reflect.ValueOf(c.dst)).Interface()
				if !reflect.DeepEqual(indir, c.exp) {
					t.Fatalf("exp: %v, got :%v", c.exp, indir)
				}
			}
		})
	}
}

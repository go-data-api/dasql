package dasql

import (
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

func TestConvertArrayArg(t *testing.T) {
	for i, c := range []struct {
		arg    interface{}
		exp    string
		expErr error
	}{
		{[]string{"foo", "bar"}, `{StringValues:["foo","bar"]}`, nil},
		{[]float64{.1, 100.2}, `{DoubleValues:[0.1,100.2]}`, nil},
		{[]int64{1213, 34534}, `{LongValues:[1213,34534]}`, nil},
		{[]bool{true, false, true}, `{BooleanValues:[true,false,true]}`, nil},

		{[][]string{{"foo"}, {"bar"}}, `{ArrayValues:[{StringValues:["foo"]},{StringValues:["bar"]}]}`, nil},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			av, err := convertArrayArg(c.arg)
			if !errors.Is(err, c.expErr) {
				t.Fatalf("exp: %v got: %v", c.expErr, err)
			}

			if act := strings.Join(strings.Fields(av.String()), ""); act != c.exp {
				t.Fatalf("exp: %v got: %v", c.exp, act)
			}
		})
	}
}

func TestConvertArg(t *testing.T) {
	for i, c := range []struct {
		arg     interface{}
		exp     string
		expHint string
		expErr  error
	}{
		{"foo", `{StringValue:"foo"}`, "", nil},
		{"foo", `{StringValue:"foo"}`, "", nil},
		{int64(1234), `{LongValue:1234}`, "", nil},
		{1234, `{LongValue:1234}`, "", nil},
		{0.1, `{DoubleValue:0.1}`, "", nil},
		{true, `{BooleanValue:true}`, "", nil},
		{[]byte{0x01}, `{BlobValue:<binary>len1}`, "", nil},
		{sql.RawBytes{0x01, 0x02}, `{BlobValue:<binary>len2}`, "", nil},
		{nil, `{IsNull:true}`, "", nil},

		{[]string{"foo", "bar"}, `{ArrayValue:{StringValues:["foo","bar"]}}`, "", nil},
	} {
		t.Run(strconv.Itoa(i), func(t *testing.T) {
			f, h, err := convertArg(c.arg)
			if !errors.Is(err, c.expErr) {
				t.Fatalf("exp: %v got: %v", c.expErr, err)
			}

			if act := strings.Join(strings.Fields(f.String()), ""); act != c.exp {
				t.Fatalf("exp: %v got: %v", c.exp, act)
			}

			if h != c.expHint {
				t.Fatalf("exp: %v got: %v", c.exp, h)
			}
		})
	}
}

func TestConvertArgRefClone(t *testing.T) {
	t.Run("ref", func(t *testing.T) {
		arg := sql.RawBytes{0x01}
		f, _, _ := convertArg(arg)
		f.BlobValue[0] = 0x02

		if arg[0] != 0x02 {
			t.Fatal("should be same memory")
		}
	})

	t.Run("clone", func(t *testing.T) {
		arg := []byte{0x01}
		f, _, _ := convertArg(arg)
		f.BlobValue[0] = 0x02

		if arg[0] == 0x02 {
			t.Fatal("should not be same memory")
		}
	})
}

func TestConvertArgs(t *testing.T) {
	t.Run("ok", func(t *testing.T) {
		params, err := ConvertArgs(sql.Named("foo", "bar"), sql.Named("bar", 1.1))
		if err != nil {
			t.Fatalf("got: %v", err)
		}

		exp := `[{Name:"foo",Value:{StringValue:"bar"}}{Name:"bar",Value:{DoubleValue:1.1}}]`
		if act := strings.Join(strings.Fields(fmt.Sprintf("%s", params)), ""); act != exp {
			t.Fatalf("exp: %v got: %v", exp, act)
		}
	})

	t.Run("non-named", func(t *testing.T) {
		_, err := ConvertArgs("foo")
		if aerr, ok := err.(ArgErr); !ok || aerr.Kind != ArgErrKindUnsupported {
			t.Fatalf("got: %v", err)
		}

		if !strings.Contains(err.Error(), "string") {
			t.Fatalf("got: %v", err)
		}
	})

	t.Run("unsupported type", func(t *testing.T) {
		_, err := ConvertArgs(sql.Named("foo", func() {}))
		if aerr, ok := err.(ArgErr); !ok || aerr.Kind != ArgErrKindUnsupported {
			t.Fatalf("got: %v", err)
		}

		if !strings.Contains(err.Error(), "func()") {
			t.Fatalf("got: %v", err)
		}

	})
}

func TestArgErr(t *testing.T) {
	var err ArgErr
	if err.Error() != "error while converting argument" {
		t.Fatalf("got: %v", err.Error())
	}
}

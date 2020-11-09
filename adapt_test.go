package dasql

import "context"

// TXDB is the interface shared between a transaction and the DB
type TXDB interface {
	Exec(ctx context.Context, q string, args ...interface{}) (Result, error)
	Query(ctx context.Context, q string, args ...interface{}) (Result, error)
}

var _ TXDB = &StdDB{}
var _ TXDB = &DB{}
var _ TXDB = Tx(nil)

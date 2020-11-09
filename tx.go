package dasql

// Tx represents a SQL transaction
type Tx interface{}

// daTx implements the Tx interface for the Data API
type daTx struct {
	id string
	db *DB
}

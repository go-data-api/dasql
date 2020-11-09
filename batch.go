package dasql

// Batch represents the set of parameters for a single SQL query.
type Batch struct {
	sql  string
	exes [][]interface{}
	qrys [][]interface{}
}

// NewBatch creates a new batch wit the provided SQL query
func NewBatch(query string) *Batch {
	return &Batch{sql: query}
}

// Exec adds an extra set of exec parameters to the batched.
func (b *Batch) Exec(args ...interface{}) *Batch {
	b.exes = append(b.exes, args)
	return b
}

// Query adds an extra set of query parameters to the batched.
func (b *Batch) Query(args ...interface{}) *Batch {
	b.qrys = append(b.qrys, args)
	return b
}

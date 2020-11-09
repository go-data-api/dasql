package dasql

// Batch represents the set of parameters for a single SQL query.
type Batch struct {
	q string
	p [][]interface{}
}

// NewBatch creates a new batch wit the provided SQL query
func NewBatch(query string) *Batch {
	return &Batch{q: query}
}

// Append adds an extra set of parameters to the batched.
func (b *Batch) Append(args ...interface{}) *Batch {
	b.p = append(b.p, args)
	return b
}

# dasql
Interact with the AWS Aurora Data API using an interface that resembles "database/sql" from the standard library

## features
- Simple, only depends on the official AWS SDK for Go

## research
- [ ] Figure out what the Data API return in case of datetime,date and timestamp column types
- [ ] Figure out if json column types work as expected
- [ ] It only supports named parameters

## limitations
- The RDS Data API doesn't return datetime/date/timestamp field values specifically. So a sql.Scanner
will never be passed a time.time in those cases. 

- The Current Go SDK will not retry correctly on sleeping databases, use a custom retryer to
fix that: https://github.com/aws/aws-sdk-go/issues/3628

## backlog
- [ ] SHOULD implement scanning into *int, *int8, *int16, *int32, *uint, *uint8, *uint16, *uint32, 
             *uint64 instead of only int64
- [ ] SHOULD document the types that scan supports similar to how the stdlib does it: 
             https://github.com/golang/go/blob/master/src/database/sql/sql.go
- [ ] COULD  simplify the scan errors, we got two now but one should be plenty
- [ ] COULD  add a easy-to-use mock result for testing with a `Exec(...)` interface
- [ ] SHOULD benchmark the allocs of scan and param functions with all the aws.String and what not
- [ ] SHOULD enable scanning into *time.Time from String and Timestamp, depending on what the data
             API returns
- [ ] SHOULD on cold start, support "Communications link failure, The last packet sent successfully
             to the server was 0 milliseconds ago. The driver has not received any packets from 
             the server."
             SameFixAs: https://github.com/aws/aws-sdk-js/pull/2931

             - https://github.com/aws/aws-sdk-go/blob/v1.35.23/aws/request/retryer.go#L250 //(r *Request) IsErrorRetryable() bool {
- [ ] SHOULD support time.time as an argument and for scanning
- [ ] SHOULD support passing the the following exec options as arguments: 
             ContinueAfterTimeout, IncludeResultMetadata, ResultSetOptions
- [ ] SHOULD support https://golang.org/pkg/database/sql/#Rows.ColumnTypes 
             and https://golang.org/pkg/database/sql/#Rows.Columns on result type
- [ ] SHOULD rollback the transaction when the ctx is cancelled like https://godoc.org/database/sql#DB.BeginTx
- [ ] SHOULD add options for configuring defaults for: database name and schema. Both by default
             and maybe per BeginTransaction() and ExecuteStatement()
# dasql
Interact with the AWS Aurora Data API using an interface that resembles "database/sql" from the standard library

## backlog
- [ ] SHOULD implement scanning into *int, *int8, *int16, *int32, *uint, *uint8, *uint16, *uint32, 
             *uint64 instead of only int64
- [ ] SHOULD document the types that scan supports similar to how the stdlib does it: 
             https://github.com/golang/go/blob/master/src/database/sql/sql.go
   
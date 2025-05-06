# Presto Go client

A [Presto](http://prestodb.io/) client for the [Go](https://golang.org)
programming language, adapted from [github.com/trinodb/trino-go-client](https://github.com/trinodb/trino-go-client).

This library strips out some of the more advanced Trino-specific functionality
(such as the advanced type-scanning logic) and adapts it to work with Presto.
This library also works with Trino when configured in Presto-compatibility mode,
per the docs [here](https://trino.io/blog/2021/01/04/migrating-from-prestosql-to-trino.html#client-protocol-compatiblity).

Note that this driver aims to be compliant with the [database/sql](https://pkg.go.dev/database/sql)
package interface. In particular, only valid [driver.Value](https://pkg.go.dev/database/sql/driver#Value)
types are returned (unlike the upstream Trino driver, which returns
`map[string]interface{}` for `MAP` types and `[]interface{}` for `ARRAY`/`ROW`
types). The driver therefore behaves as documented in the
[Rows.Scan](https://pkg.go.dev/database/sql#Rows.Scan) documentation.

Note that all date and time types are returned as strings, to maintain the
precise format in which they're returned from Presto/Trino itself.

## License

Apache License V2.0, as described in the [LICENSE](./LICENSE) file.

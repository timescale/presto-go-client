# Presto Go client

A [Presto](http://prestodb.io/) client for the [Go](https://golang.org)
programming language, adapted from [github.com/trinodb/trino-go-client](https://github.com/trinodb/trino-go-client)
(mostly just replacing "trino" with "presto" in the source files). See those
docs for more details. Note that some Trino-specific features won't work with
Presto.

## License

Apache License V2.0, as described in the [LICENSE](./LICENSE) file.

## Build

You can build the client code locally and run tests with the following command:

```
go test -v -race -timeout 2m ./...
```

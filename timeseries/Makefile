
all: binaries

binaries: deps bin/influxdb-queries bin/influxdb-updates

bin/influxdb-queries:
	test -d bin || mkdir bin
	go build -o bin/influxdb-queries cmd/influxdb-queries.go

bin/influxdb-updates:
	test -d bin || mkdir bin
	go build -o bin/influxdb-updates cmd/influxdb-updates.go

deps:
	go get github.com/gogo/protobuf/proto
	go get go.uber.org/zap
	go get github.com/influxdata/influxdb/client/v2
	go get github.com/julienschmidt/httprouter
	go get github.com/mattn/go-sqlite3
	go get github.com/olebedev/config
	go get github.com/ugorji/go/codec

clean:
	rm -f bin/influxdb-queries
	rm -f bin/influxdb-updates
	rm -fd bin

.PHONY: all binaries deps clean


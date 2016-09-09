package main

import (
	"flag"
	"github.com/ajgb/go-opsview/timeseries"
)

func main() {
	conf_dir := flag.String("c", "./etc", "default configuration directory")
	flag.Parse()

	server := &timeseries.TimeseriesServer{}
	server.ReadConfig(*conf_dir)
	server.Launch("queries")
}

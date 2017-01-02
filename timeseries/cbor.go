package timeseries

import (
	"errors"
	"github.com/ugorji/go/codec"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type TimeSeriesRequest map[string]map[string]map[string][4]string

type TimeSeriesData struct {
	MetricEscaped string
	Metric        string
	Dstype        string
	Uom           string
	Value         float64
}

type TimeSeries struct {
	HostEscaped    string
	ServiceEscaped string
	Host           string
	Service        string
	Timestamp      time.Time
	Data           []TimeSeriesData
}

func (this *TimeseriesServer) DecodeCbor(raw io.Reader) (ts []TimeSeries, fail error) {
	defer func() {
		if r := recover(); r != nil {
			switch x := r.(type) {
			case string:
				fail = errors.New(x)
			case error:
				fail = x
			default:
				fail = errors.New("Unknown panic")
			}
			ts = nil
		}
	}()
	var ch = codec.NewDecoder(raw, new(codec.CborHandle))
	var ts_data TimeSeriesRequest

	ch.MustDecode(&ts_data)

	ts = make([]TimeSeries, 0, this.config.Server.Updates.ExpectedResultsCount)

	for host_escaped, sc_data := range ts_data {
		for sc_escaped, t_data := range sc_data {
			for timestamp, data := range t_data {
				epoch, err := strconv.ParseInt(timestamp, 10, 64)
				if err != nil {
					continue
				}

				host, err := url.QueryUnescape(host_escaped)
				if err != nil {
					continue
				}

				sc, err := url.QueryUnescape(sc_escaped)
				if err != nil {
					continue
				}
				var item = TimeSeries{
					HostEscaped:    host_escaped,
					Host:           host,
					ServiceEscaped: sc_escaped,
					Service:        sc,
					Timestamp:      time.Unix(epoch, 0),
					Data:           make([]TimeSeriesData, 0, 200),
				}

				metrics := strings.Split(data[0], ":")
				dstypes := strings.Split(data[1], ":")
				uoms := strings.Split(data[2], ":")
				values := strings.Split(data[3], ":")

				for i := 0; i < len(metrics); i++ {
					val, err := strconv.ParseFloat(values[i], 64)
					if err != nil {
						continue
					}

					metric, err := url.QueryUnescape(metrics[i])
					if err != nil {
						continue
					}

					item.Data = append(item.Data,
						TimeSeriesData{
							MetricEscaped: metrics[i],
							Metric:        metric,
							Dstype:        dstypes[i],
							Uom:           uoms[i],
							Value:         val,
						},
					)
				}
				ts = append(ts, item)
			}
		}
	}

	return ts, nil
}

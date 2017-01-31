package timeseries

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/influxdata/influxdb/client/v2"
	"github.com/julienschmidt/httprouter"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

type QueryParamsHSM struct {
	HSM, Host, eHost, Service, eService, Metric, eMetric string
}
type QueryParams struct {
	dataPoints         int64
	minTimeSlot        int64
	fixedTimeSlot      int64
	fillOption         string
	counterMetricsMode string
	retentionPolicy    string
	startEpoch         int64
	endEpoch           int64
	includeTzOffset    bool
	HSMs               []QueryParamsHSM
}
type QueryResultDataStats struct {
	Min    interface{} `json:"min"`
	Max    interface{} `json:"max"`
	Avg    interface{} `json:"avg"`
	Stddev interface{} `json:"stddev"`
	P95    interface{} `json:"p95"`
}
type QueryResultData struct {
	Data  [][2]interface{}      `json:"data"`
	Uom   string                `json:"uom"`
	Stats *QueryResultDataStats `json:"stats,omitempty"`
}
type QueryResults map[string]*QueryResultData

func (this *TimeseriesServer) parseQueryParams(query url.Values) (*QueryParams, error) {
	var qsParams = &QueryParams{}

	this.log.Debug("Params: %s\n", query)

	startEpoch := query.Get("start")
	if startEpoch == "" {
		return nil, errors.New(fmt.Sprintf("Missing parameter: start"))
	}
	if i, err := strconv.ParseInt(startEpoch, 10, 64); err != nil {
		return nil, errors.New(fmt.Sprintf("Invalid parameter: start"))
	} else {
		qsParams.startEpoch = i
	}

	endEpoch := query.Get("end")
	if endEpoch == "" {
		return nil, errors.New(fmt.Sprintf("Missing parameter: end"))
	}
	if i, err := strconv.ParseInt(endEpoch, 10, 64); err != nil {
		return nil, errors.New(fmt.Sprintf("Invalid parameter: end"))
	} else {
		qsParams.endEpoch = i
	}
	includeTzOffset := query.Get("include_offset_timezone")
	if includeTzOffset == "1" {
		qsParams.includeTzOffset = true
	}

	if hsms, ok := query["hsm"]; ok {
		qsParams.HSMs = make([]QueryParamsHSM, 0, len(hsms))
		for _, hsm := range hsms {
			var qsHSM QueryParamsHSM

			h_s_m := strings.Split(hsm, "::")

			if len(h_s_m) == 3 {
				qsHSM.eHost = h_s_m[0]
				qsHSM.eService = h_s_m[1]
				qsHSM.eMetric = h_s_m[2]
			} else {
				continue
			}

			host, err := url.QueryUnescape(qsHSM.eHost)
			if err != nil {
				this.log.Warning("Failed to unescape host: %s\n", err)
				continue
			}
			qsHSM.Host = host

			service, err := url.QueryUnescape(qsHSM.eService)
			if err != nil {
				this.log.Warning("Failed to unescape service: %s\n", err)
				continue
			}
			qsHSM.Service = service

			metric, err := url.QueryUnescape(qsHSM.eMetric)
			if err != nil {
				this.log.Warning("Failed to unescape metric: %s\n", err)
				continue
			}
			qsHSM.Metric = metric

			qsHSM.HSM = strings.Join([]string{host, service, metric}, "::")

			qsParams.HSMs = append(qsParams.HSMs, qsHSM)
		}
	} else {
		return nil, errors.New(fmt.Sprintf("Missing parameter: hsm"))
	}

	dataPoints := query.Get("data_points")
	if dataPoints == "" {
		qsParams.dataPoints = this.config.Server.Queries.DataPoints
	} else {
		if i, err := strconv.ParseInt(dataPoints, 10, 64); err != nil {
			return nil, errors.New(fmt.Sprintf("Invalid parameter: data_points"))
		} else {
			qsParams.dataPoints = i
		}
	}

	minTimeSlot := query.Get("min_time_slot")
	if minTimeSlot == "" {
		qsParams.minTimeSlot = this.config.Server.Queries.MinTimeSlot
	} else {
		if i, err := strconv.ParseInt(minTimeSlot, 10, 64); err != nil {
			return nil, errors.New(fmt.Sprintf("Invalid parameter: min_time_slot"))
		} else {
			qsParams.minTimeSlot = i
		}
	}

	fixedTimeSlot := query.Get("fixed_time_slot")
	if fixedTimeSlot == "" {
		qsParams.fixedTimeSlot = this.config.Server.Queries.FixedTimeSlot
	} else {
		if i, err := strconv.ParseInt(fixedTimeSlot, 10, 64); err != nil {
			return nil, errors.New(fmt.Sprintf("Invalid parameter: fixed_time_slot"))
		} else {
			qsParams.fixedTimeSlot = i
		}
	}

	fillOption := query.Get("fill_option")
	if fillOption == "linear" || fillOption == "none" || fillOption == "null" || fillOption == "previous" {
		qsParams.fillOption = fillOption
	} else {
		if fillOption == "" {
			qsParams.fillOption = this.config.Server.Queries.FillOption
		} else {
			if _, err := strconv.ParseInt(fillOption, 10, 64); err != nil {
				return nil, errors.New(fmt.Sprintf("Invalid parameter fill_option: %s", fillOption))
			} else {
				qsParams.fillOption = fillOption
			}
		}
	}

	counterMetricsMode := query.Get("counter_metrics_mode")
	if counterMetricsMode == "difference" || counterMetricsMode == "per_second" {
		qsParams.counterMetricsMode = counterMetricsMode
	} else {
		qsParams.counterMetricsMode = this.config.Server.Queries.CounterMetricsMode
	}

	retentionPolicy := query.Get("rp")
	if retentionPolicy != "" && !strings.ContainsAny(retentionPolicy, ";\"") {
		qsParams.retentionPolicy = retentionPolicy
	} else {
		qsParams.retentionPolicy = this.config.InfluxDB.RetentionPolicy
	}

	return qsParams, nil
}

func (this *TimeseriesServer) QueryHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	if err := r.ParseForm(); err != nil {
		this.sendHTTPError(w, http.StatusBadRequest, "Failed to parse query: %s", err)
		return
	}
	qsParams, err := this.parseQueryParams(r.Form)
	if err != nil {
		this.sendHTTPError(w, http.StatusBadRequest, "Failed to parse query: %s", err)
		return
	}
	this.log.Debug("qsParams: %+v\n", qsParams)

	dbRp := fmt.Sprintf("%s.\"%s\"", this.config.InfluxDB.Database, qsParams.retentionPolicy)

	clientConfig := client.HTTPConfig{
		Addr: this.config.InfluxDB.Server,
	}
	if this.config.InfluxDB.User != "" {
		clientConfig.Username = this.config.InfluxDB.User
		clientConfig.Password = this.config.InfluxDB.Password
	}
	db, err := client.NewHTTPClient(clientConfig)

	if err != nil {
		this.sendHTTPError(w, http.StatusInternalServerError, "Failed to connect to InfluxDB: %s", err)
		return
	}
	defer db.Close()

	metrics := make(QueryResults)

	var tz_offset = 0
	if qsParams.includeTzOffset {
		localtime := time.Now()
		_, tz_offset = localtime.Zone()
	}

	for _, hsm := range qsParams.HSMs {
		var column string

		this.log.Debug("Host(%s) Service(%s) Metric(%s)\n", hsm.Host, hsm.Service, hsm.Metric)

		dstype, uomLabel, uomMultiplier, err := this.GetHSMsetup(hsm.Host, hsm.Service, hsm.Metric)
		if err != nil {
			this.sendHTTPError(w, http.StatusInternalServerError, "Failed to query metadata information: %s", err)
			return
		}

		if dstype == "COUNTER" || dstype == "DERIVE" {
			switch qsParams.counterMetricsMode {
			case "difference":
				column = "DIFFERENCE(MEAN(value))"
			case "per_second":
				column = "DERIVATIVE(MEAN(value), 1s)"
			}
		} else { //case "GAUGE":
			column = fmt.Sprintf("MEAN(value) * %f", uomMultiplier)
		}

		sql := fmt.Sprintf(
			"SELECT %s FROM %s.\"%s\" WHERE service = '%s' AND metric = '%s' AND time >= %ds AND time <= %ds GROUP BY time(%s) fill(%s); "+
				"SELECT MIN(value) * %[10]f, MAX(value) * %[10]f, MEAN(value) * %[10]f, STDDEV(value) * %[10]f, PERCENTILE(value, 95) * %[10]f FROM %[2]s.\"%[3]s\" WHERE service = '%[4]s' AND metric = '%[5]s' AND time => %[6]ds AND time <= %[7]ds",
			column,
			dbRp,
			hsm.eHost,
			hsm.Service,
			hsm.Metric,
			qsParams.startEpoch,
			qsParams.endEpoch,
			CalculateTimeSlotSize(qsParams.dataPoints, qsParams.startEpoch, qsParams.endEpoch, float64(qsParams.minTimeSlot), float64(qsParams.fixedTimeSlot)),
			qsParams.fillOption,
			uomMultiplier,
		)
		this.log.Debug("dstype(%s) uomLabel(%s) uomMultiplier(%f)\n", dstype, uomLabel, uomMultiplier)
		this.log.Debug("sql(%s)\n", sql)

		q := client.Query{
			Command:   sql,
			Database:  this.config.InfluxDB.Database,
			Precision: "s",
		}
		var results []client.Result
		if response, err := db.Query(q); err == nil {
			if resError := response.Error(); resError != nil {
				this.sendHTTPError(w, http.StatusInternalServerError, "Failed to query database: %s", resError)
				return
			}
			results = response.Results
		} else {
			this.sendHTTPError(w, http.StatusInternalServerError, "Failed to query database: %s", err)
			return
		}

		if (len(results) == 2 && len(results[0].Series) == 1 && len(results[1].Series) == 1) &&
			(len(results[1].Series[0].Values) == 1 && len(results[1].Series[0].Values[0]) == 6) {

			rowsCount := len(results[0].Series[0].Values)

			metrics[hsm.HSM] = &QueryResultData{
				Data: make([][2]interface{}, rowsCount),
				Stats: &QueryResultDataStats{
					Min:    results[1].Series[0].Values[0][1],
					Max:    results[1].Series[0].Values[0][2],
					Avg:    results[1].Series[0].Values[0][3],
					Stddev: results[1].Series[0].Values[0][4],
					P95:    results[1].Series[0].Values[0][5],
				},
				Uom: uomLabel,
			}
			for i, row := range results[0].Series[0].Values {
				ts, _ := row[0].(json.Number).Int64()
				ts += int64(tz_offset)

				metrics[hsm.HSM].Data[i] = [2]interface{}{
					ts,
					row[1],
				}
			}
		}
	}

	json, err := json.Marshal(metrics)

	if err != nil {
		this.sendHTTPError(w, http.StatusInternalServerError, "Failed to encode JSON response: %s", err)
		return
	}
	w.Write(json)
}

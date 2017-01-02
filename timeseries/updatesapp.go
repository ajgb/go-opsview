package timeseries

import (
	"github.com/influxdata/influxdb/client/v2"
	//"github.com/influxdata/influxdb/models"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

func (this *TimeseriesServer) WriteHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	ts, err := this.DecodeCbor(r.Body)
	defer r.Body.Close()
	if err != nil {
		this.sendHTTPError(w, http.StatusBadRequest, "Failed to decode: %s", err)
		return
	}
	r.Close = true

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

	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        this.config.InfluxDB.Database,
		RetentionPolicy: this.config.InfluxDB.RetentionPolicy,
		Precision:       "s",
	})

	if err != nil {
		this.sendHTTPError(w, http.StatusInternalServerError, "Failed to create writer for InfluxDB: %s", err)
		return
	}

	metadata := make([][5]string, 0, len(ts)*this.config.Server.Updates.ExpectedResultsCount)

	for _, hs := range ts {
		for _, data := range hs.Data {
			tags := map[string]string{
				"service": hs.Service,
				"metric":  data.Metric,
			}
			fields := map[string]interface{}{"value": data.Value}

			metadata = append(metadata,
				[5]string{
					hs.Host,
					hs.Service,
					data.Metric,
					data.Dstype,
					data.Uom,
				})

			pt, _ := client.NewPoint(
				hs.Host,
				tags,
				fields,
				hs.Timestamp,
			)
			bp.AddPoint(pt)
		}
	}

	if err := db.Write(bp); err != nil {
		this.sendHTTPError(w, http.StatusInternalServerError, "Failed to write metrics to InfluxDB: %s", err)
		return
	}
	this.queue <- metadata
	w.Write([]byte("{\"status\":0}"))
}

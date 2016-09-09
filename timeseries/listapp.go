package timeseries

import (
	"encoding/json"
	"github.com/julienschmidt/httprouter"
	"net/http"
)

func (this *TimeseriesServer) ListHandler(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
	hsm2u, err := this.ListHSM2U()
	if err != nil {
		this.sendHTTPError(w, http.StatusInternalServerError, "Failed to list metadata information: %s", err)
		return
	}
	json, err := json.Marshal(hsm2u)

	if err != nil {
		this.sendHTTPError(w, http.StatusInternalServerError, "Failed to encode JSON response: %s", err)
		return
	}
	w.Write(json)
}

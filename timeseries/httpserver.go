package timeseries

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/julienschmidt/httprouter"
	"log"
	"net/http"
	"sync"
	"time"
)

type TimeseriesServer struct {
	config *TimeseriesConfig
	metadb *sql.DB
	queue  chan [][5]string
	log    *TimeseriesLogger
}

type TimeseriesErrorResponse struct {
	Error string `json:"error"`
}

func (this *TimeseriesServer) ReadConfig(confdir string) {
	this.config = ReadConfig(confdir)
}

func (this *TimeseriesServer) AccessLog(h httprouter.Handle) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		started := time.Now()

		h(w, r, ps)
		this.log.Info("%s %s \"%s %s\" %s",
			r.RemoteAddr,
			r.Host,
			r.Method,
			r.RequestURI,
			time.Since(started),
		)
	}
}

func (this *TimeseriesServer) BasicAuth(h httprouter.Handle, requiredUser, requiredPassword string) httprouter.Handle {
	return func(w http.ResponseWriter, r *http.Request, ps httprouter.Params) {
		user, password, hasAuth := r.BasicAuth()

		if hasAuth && user == requiredUser && password == requiredPassword {
			// always responds with json
			w.Header().Set("Content-Type", "application/json")

			h(w, r, ps)
		} else {
			w.Header().Set("WWW-Authenticate", "Basic realm=Restricted")
			w.WriteHeader(http.StatusUnauthorized)
			w.Write([]byte(http.StatusText(http.StatusUnauthorized)))
		}
	}
}

func (this *TimeseriesServer) sendHTTPError(w http.ResponseWriter, responseCode int, format string, v ...interface{}) {
	var msg string

	if len(v) > 0 {
		msg = fmt.Sprintf(format, v...)
	} else {
		msg = format
	}

	this.log.Error(msg)

	w.WriteHeader(responseCode)

	e := TimeseriesErrorResponse{Error: msg}
	json_error, err := json.Marshal(e)
	if err == nil {
		w.Write(json_error)
	} else {
		this.log.Critical("Failed to create error response:", err)
		w.Write([]byte(`{"error":"Unknown error"}`))
	}
}

func (this *TimeseriesServer) launchUpdatesWorker(port int) {
	bind := fmt.Sprintf("%s:%d", this.config.Server.Updates.Host, port)

	router := httprouter.New()
	router.POST("/", this.AccessLog(this.BasicAuth(this.WriteHandler, this.config.Server.User, this.config.Server.Password)))

	this.log.Notice("Server started on %s\n", bind)
	http.ListenAndServe(bind, router)
}

func (this *TimeseriesServer) launchQueriesWorker() {
	bind := fmt.Sprintf("%s:%d", this.config.Server.Queries.Host, this.config.Server.Queries.Port)

	router := httprouter.New()
	router.GET("/list", this.AccessLog(this.BasicAuth(this.ListHandler, this.config.Server.User, this.config.Server.Password)))
	router.GET("/query", this.AccessLog(this.BasicAuth(this.QueryHandler, this.config.Server.User, this.config.Server.Password)))
	router.POST("/query", this.AccessLog(this.BasicAuth(this.QueryHandler, this.config.Server.User, this.config.Server.Password)))

	this.log.Notice("Server started on %s\n", bind)
	http.ListenAndServe(bind, router)
}

func (this *TimeseriesServer) Launch(role string) {

	if err := this.InitMetadataDB(); err != nil {
		log.Fatalf("Failed to initialize metadata database: %s\n", err)
		return
	}
	defer this.metadb.Close()

	var wg sync.WaitGroup
	switch role {
	case "updates":
		this.log = NewLogger(this.config.Server.Updates.LogFacility, this.config.Server.Updates.LogLevel, "influxdb-updates")
		this.queue = make(chan [][5]string, len(this.config.Server.Updates.Ports))
		wg.Add(1)
		go func() {
			defer wg.Done()
			this.updateMetadata()
		}()
		for _, port := range this.config.Server.Updates.Ports {
			wg.Add(1)
			go func(port int) {
				defer wg.Done()

				this.launchUpdatesWorker(port)
			}(port)
		}
	case "queries":
		this.log = NewLogger(this.config.Server.Queries.LogFacility, this.config.Server.Queries.LogLevel, "influxdb-queries")
		wg.Add(1)
		go func() {
			defer wg.Done()

			this.launchQueriesWorker()
		}()
	}

	wg.Wait()
}

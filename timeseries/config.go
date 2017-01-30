package timeseries

import (
	"errors"
	"fmt"
	"github.com/influxdata/influxdb/influxql"
	"github.com/olebedev/config"
	"io/ioutil"
	"log"
	"path"
	"sort"
	"time"
)

type TimeseriesInfluxDBConfig struct {
	Server          string
	User            string
	Password        string
	Database        string
	RetentionPolicy string
}

type TimeseriesServerUpdatesConfig struct {
	Host                 string
	Ports                []int
	LogLevel             string
	LogFacility          string
	ExpectedResultsCount int
}

type RetentionPolicyConfig struct {
	Name             string
	FillOption       string
	Duration         time.Duration
	GroupingInterval time.Duration
}

type RetentionPoliciesConfig []RetentionPolicyConfig

type TimeseriesServerQueriesConfig struct {
	Host               string
	Port               int
	LogLevel           string
	LogFacility        string
	FillOption         string
	DataPoints         int64
	MinTimeSlot        int64
	FixedTimeSlot      int64
	CounterMetricsMode string
	Downsampling       RetentionPoliciesConfig
}

func (s RetentionPoliciesConfig) Len() int {
	return len(s)
}

func (s RetentionPoliciesConfig) Less(i, j int) bool {
	return s[i].Duration < s[j].Duration
}

func (s RetentionPoliciesConfig) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

type TimeseriesServerConfig struct {
	User     string
	Password string
	Updates  TimeseriesServerUpdatesConfig
	Queries  TimeseriesServerQueriesConfig
}

type TimeseriesConfig struct {
	DataDir  string
	Server   TimeseriesServerConfig
	InfluxDB TimeseriesInfluxDBConfig
}

const (
	DCONFIG_FILE = "timeseriesinfluxdb.defaults.yaml"
	UCONFIG_FILE = "timeseriesinfluxdb.yaml"
)

func (this *TimeseriesConfig) extractSettings(data *config.Config) (fail error) {
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
		}
	}()

	if v, err := data.String("timeseriesinfluxdb.server.user"); err == nil {
		this.Server.User = v
	}
	if v, err := data.String("timeseriesinfluxdb.server.password"); err == nil {
		this.Server.Password = v
	}
	if v, err := data.String("timeseriesinfluxdb.data_dir"); err == nil {
		this.DataDir = v
	}
	if v, err := data.String("timeseriesinfluxdb.influxdb.server"); err == nil {
		this.InfluxDB.Server = v
	}
	if v, err := data.String("timeseriesinfluxdb.influxdb.user"); err == nil {
		this.InfluxDB.User = v
	}
	if v, err := data.String("timeseriesinfluxdb.influxdb.password"); err == nil {
		this.InfluxDB.Password = v
	}
	if v, err := data.String("timeseriesinfluxdb.influxdb.database"); err == nil {
		this.InfluxDB.Database = v
	}
	if v, err := data.String("timeseriesinfluxdb.influxdb.retention_policy"); err == nil {
		this.InfluxDB.RetentionPolicy = v
	}
	if v, err := data.String("timeseriesinfluxdb.server.queries.default_parameters.fill_option"); err == nil {
		this.Server.Queries.FillOption, _ = CheckFillOption(v, "null")
	}
	if v, err := data.Int("timeseriesinfluxdb.server.queries.default_parameters.data_points"); err == nil {
		this.Server.Queries.DataPoints = int64(v)
	}
	if v, err := data.Int("timeseriesinfluxdb.server.queries.default_parameters.min_time_slot"); err == nil {
		this.Server.Queries.MinTimeSlot = int64(v)
	}
	if v, err := data.Int("timeseriesinfluxdb.server.queries.default_parameters.fixed_time_slot"); err == nil {
		this.Server.Queries.FixedTimeSlot = int64(v)
	}
	if v, err := data.String("timeseriesinfluxdb.server.queries.default_parameters.counter_metrics_mode"); err == nil {
		if v == "difference" || v == "per_second" {
			this.Server.Queries.CounterMetricsMode = v
		}
	}
	if v, err := data.String("timeseriesinfluxdb.server.updates.logging.loggers.opsview.level"); err == nil {
		this.Server.Updates.LogLevel = v
	}
	if v, err := data.String("timeseriesinfluxdb.server.updates.logging.loggers.opsview.facility"); err == nil {
		this.Server.Updates.LogFacility = v
	}
	if v, err := data.String("timeseriesinfluxdb.server.updates.host"); err == nil {
		this.Server.Updates.Host = v
	}
	if v, err := data.Int("timeseriesinfluxdb.server.updates.expected_results_count"); err == nil {
		this.Server.Updates.ExpectedResultsCount = v
	}
	if v, err := data.List("timeseriesinfluxdb.server.updates.workers"); err == nil {
		size := len(v)
		this.Server.Updates.Ports = make([]int, size)
		for i, w := range v {
			p := w.(map[string]interface{})
			this.Server.Updates.Ports[i] = int(p["port"].(int))
		}
	}
	if v, err := data.List("timeseriesinfluxdb.server.queries.downsampling"); err == nil {
		size := len(v)
		this.Server.Queries.Downsampling = make(RetentionPoliciesConfig, size)
		for i := 0; i < size; i++ {
			log.Printf("i: %+v\n", i)
			name, err := data.String(fmt.Sprintf("timeseriesinfluxdb.server.queries.downsampling.%d.name", i))
			if err != nil {
				panic(fmt.Errorf("The downsampling period at %d requires name", i))
			}
			d, err := data.String(fmt.Sprintf("timeseriesinfluxdb.server.queries.downsampling.%d.duration", i))
			if err != nil {
				panic(fmt.Errorf("The downsampling period '%s' requires duration", name))
			}
			duration, err := influxql.ParseDuration(d)
			if err != nil {
				panic(fmt.Errorf("The downsampling period '%s' contains invalid duration %s", name, d))
			}
			var grouping_interval time.Duration
			g, err := data.String(fmt.Sprintf("timeseriesinfluxdb.server.queries.downsampling.%d.grouping_interval", i))
			if err == nil {
				gc, err := influxql.ParseDuration(g)
				if err != nil {
					panic(fmt.Errorf("The downsampling period '%s' contains invalid grouping_duration %s", name, g))
				}
				grouping_interval = gc
			}
			fo, err := data.String(fmt.Sprintf("timeseriesinfluxdb.server.queries.downsampling.%d.fill_option", i))

			this.Server.Queries.Downsampling[i] = RetentionPolicyConfig{
				Name:             name,
				Duration:         duration,
				GroupingInterval: grouping_interval,
			}
			if err == nil {
				fill_option, err := CheckFillOption(fo, "")
				if err != nil || fill_option == "" {
					panic(fmt.Errorf("The downsampling period '%s' contains invalid fill_option %s", name, fo))
				} else {
					this.Server.Queries.Downsampling[i].FillOption = fill_option
				}
			}
		}
		if size > 0 {
			sort.Sort(this.Server.Queries.Downsampling)
		}
	}
	if v, err := data.String("timeseriesinfluxdb.server.queries.logging.loggers.opsview.level"); err == nil {
		this.Server.Queries.LogLevel = v
	}
	if v, err := data.String("timeseriesinfluxdb.server.queries.logging.loggers.opsview.facility"); err == nil {
		this.Server.Queries.LogFacility = v
	}
	if v, err := data.String("timeseriesinfluxdb.server.queries.host"); err == nil {
		this.Server.Queries.Host = v
	}
	if v, err := data.Int("timeseriesinfluxdb.server.queries.port"); err == nil {
		this.Server.Queries.Port = v
	}

	return nil
}

func ReadConfig(confdir string) *TimeseriesConfig {
	ddata, err := ioutil.ReadFile(path.Join(confdir, DCONFIG_FILE))
	if err != nil {
		log.Fatalf("Could not read default configuration file: %s\n", err)
	}

	dconf, err := config.ParseYaml(string(ddata))
	if err != nil {
		log.Fatalf("Could not parse default configuration file: %s\n", err)
	}

	conf := TimeseriesConfig{
		Server: TimeseriesServerConfig{
			User:     "opsview",
			Password: "opsview",
			Updates: TimeseriesServerUpdatesConfig{
				Host:                 "127.0.0.1",
				Ports:                []int{1640, 1641, 1642, 1643},
				ExpectedResultsCount: 500,
				LogLevel:             DefaultLogLevel,
				LogFacility:          DefaultLogFacility,
			},
			Queries: TimeseriesServerQueriesConfig{
				Host:               "127.0.0.1",
				Port:               1660,
				LogLevel:           DefaultLogLevel,
				LogFacility:        DefaultLogFacility,
				FillOption:         "null",
				DataPoints:         500,
				MinTimeSlot:        0,
				FixedTimeSlot:      0,
				CounterMetricsMode: "per_second",
			},
		},
		DataDir: "/opt/opsview/timeseriesinfluxdb/var/data",
		InfluxDB: TimeseriesInfluxDBConfig{
			User:            "",
			Password:        "",
			Database:        "opsview",
			RetentionPolicy: "default",
		},
	}
	if err := conf.extractSettings(dconf); err != nil {
		log.Fatalf("Could not parse default configuration file: %s\n", err)
	}
	log.Printf("config.defaults: %+v\n", dconf)

	udata, err := ioutil.ReadFile(path.Join(confdir, UCONFIG_FILE))
	if err == nil {
		uconf, err := config.ParseYaml(string(udata))
		if err != nil {
			log.Fatalf("Could not parse users configuration file: %s\n", err)
		}

		if err := conf.extractSettings(uconf); err != nil {
			log.Fatalf("Could not parse users configuration file: %s\n", err)
		}
	}
	log.Printf("config.final: %+v\n", conf)

	return &conf
}

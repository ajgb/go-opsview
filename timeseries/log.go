package timeseries

import (
	"fmt"
	"log"
	"log/syslog"
)

const (
	DefaultLogLevel    = "NOTICE"
	DefaultLogFacility = "user"
)

func LogLevelStringToPriority(level string) syslog.Priority {
	switch level {
	case "DEBUG":
		return syslog.LOG_DEBUG
	case "INFO":
		return syslog.LOG_INFO
	case "NOTICE":
		return syslog.LOG_NOTICE
	case "WARNING":
		return syslog.LOG_WARNING
	case "ERR":
		return syslog.LOG_ERR
	case "CRIT":
		return syslog.LOG_CRIT
	case "ALERT":
		return syslog.LOG_ALERT
	case "EMERG":
		return syslog.LOG_EMERG
	default:
		return syslog.LOG_NOTICE
	}
}
func LogFacilityStringToPriority(level string) syslog.Priority {
	switch level {
	case "kern":
		return syslog.LOG_KERN
	case "user":
		return syslog.LOG_USER
	case "mail":
		return syslog.LOG_MAIL
	case "daemon":
		return syslog.LOG_DAEMON
	case "auth":
		return syslog.LOG_AUTH
	case "syslog":
		return syslog.LOG_SYSLOG
	case "lpr":
		return syslog.LOG_LPR
	case "news":
		return syslog.LOG_NEWS
	case "uucp":
		return syslog.LOG_UUCP
	case "cron":
		return syslog.LOG_CRON
	case "authpriv":
		return syslog.LOG_AUTHPRIV
	case "ftp":
		return syslog.LOG_FTP
	case "local0":
		return syslog.LOG_LOCAL0
	case "local1":
		return syslog.LOG_LOCAL1
	case "local2":
		return syslog.LOG_LOCAL2
	case "local3":
		return syslog.LOG_LOCAL3
	case "local4":
		return syslog.LOG_LOCAL4
	case "local5":
		return syslog.LOG_LOCAL5
	case "local6":
		return syslog.LOG_LOCAL6
	case "local7":
		return syslog.LOG_LOCAL7
	default:
		return syslog.LOG_USER
	}
}

type TimeseriesLogger struct {
	logLevel syslog.Priority
	logger   *syslog.Writer
}

func NewLogger(facility, priority, name string) *TimeseriesLogger {
	logger, err := syslog.New(LogFacilityStringToPriority(facility), name)
	if err != nil {
		log.Fatalf("Cannot create syslog logger: %s\n", err)
	}

	return &TimeseriesLogger{
		logLevel: LogLevelStringToPriority(priority),
		logger:   logger,
	}
}

func (this *TimeseriesLogger) write(priority syslog.Priority, format string, msgs ...interface{}) {
	if priority > this.logLevel {
		return
	}

	var msg string
	if len(msgs) > 0 {
		msg = fmt.Sprintf(format, msgs...)
	} else {
		msg = format
	}
	switch priority {
	case syslog.LOG_DEBUG:
		this.logger.Debug(msg)
	case syslog.LOG_INFO:
		this.logger.Info(msg)
	case syslog.LOG_NOTICE:
		this.logger.Notice(msg)
	case syslog.LOG_WARNING:
		this.logger.Warning(msg)
	case syslog.LOG_ERR:
		this.logger.Err(msg)
	case syslog.LOG_CRIT:
		this.logger.Crit(msg)
	case syslog.LOG_ALERT:
		this.logger.Alert(msg)
	case syslog.LOG_EMERG:
		this.logger.Emerg(msg)
	}
}

func (this *TimeseriesLogger) Debug(format string, msgs ...interface{}) {
	this.write(syslog.LOG_DEBUG, format, msgs...)
}
func (this *TimeseriesLogger) Info(format string, msgs ...interface{}) {
	this.write(syslog.LOG_INFO, format, msgs...)
}
func (this *TimeseriesLogger) Notice(format string, msgs ...interface{}) {
	this.write(syslog.LOG_NOTICE, format, msgs...)
}
func (this *TimeseriesLogger) Warning(format string, msgs ...interface{}) {
	this.write(syslog.LOG_WARNING, format, msgs...)
}
func (this *TimeseriesLogger) Error(format string, msgs ...interface{}) {
	this.write(syslog.LOG_ERR, format, msgs...)
}
func (this *TimeseriesLogger) Critical(format string, msgs ...interface{}) {
	this.write(syslog.LOG_CRIT, format, msgs...)
}
func (this *TimeseriesLogger) Alert(format string, msgs ...interface{}) {
	this.write(syslog.LOG_ALERT, format, msgs...)
}
func (this *TimeseriesLogger) Emergency(format string, msgs ...interface{}) {
	this.write(syslog.LOG_EMERG, format, msgs...)
}

func (this *TimeseriesLogger) Close() {
	this.logger.Close()
}

package timeseries

import (
	"database/sql"
	_ "github.com/mattn/go-sqlite3"
	"path/filepath"
)

const (
	SQLITE_DB = "+metadata.db"
)

var done = make(chan int, 1)

func (this *TimeseriesServer) updateMetadata() {
	for data := range this.queue {
		done <- 1
		go func(data [][5]string) {
			defer func() {
				<-done
			}()

			tx, err := this.metadb.Begin()
			if err != nil {
				this.log.Error("Failed to start transcation for metadata update: %s", err)
				return
			}
			stmt, err := tx.Prepare("INSERT OR REPLACE INTO uoms (host, service, metric, dstype, uom) VALUES (?,?,?,?,?)")
			if err != nil {
				this.log.Error("Failed to prepare metadata update statement: %s", err)
				return
			}
			defer stmt.Close()
			rollback := false

			for _, i := range data {
				_, err := stmt.Exec(i[0], i[1], i[2], i[3], i[4])
				if err != nil {
					this.log.Error("Failed to add entry to metadata database: %s\n", err)
					rollback = true
					break
				}
			}
			if !rollback {
				err = tx.Commit()
				if err != nil {
					this.log.Error("Failed to commit metadata update: %s", err)
				}
			} else {
				err = tx.Rollback()
				if err != nil {
					this.log.Error("Failed to rollback metadata update: %s", err)
				}
			}
		}(data)
	}
}

func (this *TimeseriesServer) InitMetadataDB() error {

	meta, err := sql.Open("sqlite3", filepath.Join(this.config.DataDir, SQLITE_DB))
	if err != nil {
		return err
	}
	_, err = meta.Exec(`
        CREATE TABLE IF NOT EXISTS uoms (
            host VARCHAR(255) NOT NULL,
            service VARCHAR(255) NOT NULL,
            metric VARCHAR(255) NOT NULL,
            dstype VARCHAR(255) NOT NULL,
            uom VARCHAR(255) NOT NULL,
            PRIMARY KEY(host, service, metric)
        )
        `)
	if err != nil {
		meta.Close()
		return err
	}

	_, err = meta.Exec("PRAGMA journal_mode=WAL")
	if err != nil {
		meta.Close()
		return err
	}
	this.metadb = meta

	return err
}

func (this *TimeseriesServer) CloseMetadataDB() {
	if this.metadb != nil {
		this.metadb.Close()
	}
}

type metatadaMapM2U map[string]string
type metatadaMapS2M map[string]metatadaMapM2U
type metatadaMapH2S map[string]metatadaMapS2M

func (this *TimeseriesServer) ListHSM2U() (metatadaMapH2S, error) {
	rows, err := this.metadb.Query("SELECT host, service, metric, uom FROM uoms")
	if err != nil {
		return nil, err
	}
	metadbMap := make(metatadaMapH2S)

	for rows.Next() {
		var host, service, metric, uom string
		err = rows.Scan(&host, &service, &metric, &uom)
		if err != nil {
			return nil, err
		}
		sm, ok := metadbMap[host]
		if !ok {
			sm = make(metatadaMapS2M)
			metadbMap[host] = sm
		}

		mm, ok := sm[service]
		if !ok {
			mm = make(metatadaMapM2U)
			sm[service] = mm
		}
		mm[metric] = uom
	}

	return metadbMap, nil
}

func (this *TimeseriesServer) GetHSMsetup(host, service, metric string) (string, string, int64, error) {
	var dstype, uom string
	err := this.metadb.QueryRow("SELECT dstype, uom FROM uoms WHERE host = ? AND service = ? AND metric = ?", host, service, metric).Scan(&dstype, &uom)
	if err != nil {
		return "", "", 0, err
	}

	uomLabel, uomMultiplier := ConvertUom(uom)

	return dstype, uomLabel, uomMultiplier, nil
}

# go-opsview/timeseries
Store timeseries data in InfluxDB

## Install
```
mkdir ~/goprojects
export GOPATH=~/goprojects

mkdir -p ~/goprojects/src/github.com/ajgb
cd ~/goprojects/src/github.com/ajgb
git clone https://github.com/ajgb/go-opsview.git
cd go-opsview/timeseries
```

## Build
```
make
```

## Configure
```
curl -i -XPOST http://127.0.0.1:8086/query --data-urlencode "q=CREATE DATABASE opsview"
cp etc/timeseriesinfluxdb.yaml.example etc/timeseriesinfluxdb.yaml
vim etc/timeseriesinfluxdb.yaml

```

## Run
```
nohup bin/influxdb-updates &
nohup bin/influxdb-queries &

```

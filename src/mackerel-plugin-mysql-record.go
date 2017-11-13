package main

import (
	"flag"
	"fmt"
	mp "github.com/mackerelio/go-mackerel-plugin"
	"github.com/ziutek/mymysql/mysql"
	_ "github.com/ziutek/mymysql/native"
	"log"
	"strings"
)

type MySQLPlugin struct {
	Target       string
	Tempfile     string
	prefix       string
	Username     string
	Password     string
	Database     string
	Table        string
	Column       string
	isUnixSocket bool
}

func (m MySQLPlugin) GraphDefinition() map[string]mp.Graphs {
	labelPrefix := strings.Title(m.MetricKeyPrefix())
	return map[string]mp.Graphs{
		"": {
			Label: labelPrefix,
			Unit:  mp.UnitFloat,
			Metrics: []mp.Metrics{
				{Name: "count", Label: "Count", Diff: true},
			},
		},
	}
}

func (m MySQLPlugin) FetchMetrics() (map[string]float64, error) {
	proto := "tcp"
	if m.isUnixSocket {
		proto = "unix"
	}

	db := mysql.New(proto, "", m.Target, m.Username, m.Password, m.Database)
	err := db.Connect()
	if err != nil {
		log.Fatalln("DB Connect: ", err)
	}

	rows, res, err := db.Query("SELECT MAX(" + m.Column + ") AS max_record FROM " + m.Table + " LIMIT 1")
	if err != nil {
		log.Fatalln("DB Query: ", err)
	}

	err = db.Close()
	if err != nil {
		log.Fatalln("DB Connect Close: ", err)
	}

	maxRecord := res.Map("max_record")
	var maxRecordCount float64
	for _, row := range rows {
		maxRecordCount = float64(row.Int(maxRecord))
	}

	return map[string]float64{"count": maxRecordCount}, nil
}

func (m MySQLPlugin) MetricKeyPrefix() string {
	if m.prefix == "" {
		m.prefix = "MaxRecordCount"
	}
	return m.prefix
}

func main() {
	optMetricKeyPrefix := flag.String("metric-key-prefix", "MaxRecordCount", "Metric key prefix")
	optHost := flag.String("host", "localhost", "Hostname")
	optPort := flag.String("port", "3306", "Port")
	optSocket := flag.String("socket", "", "Port")
	optUser := flag.String("username", "root", "Username")
	optPass := flag.String("password", "", "Password")
	optDatabase := flag.String("database", "", "Database name")
	optTable := flag.String("table", "", "Table name")
	optColumn := flag.String("column", "id", "Table name")
	optTempfile := flag.String("tempfile", "", "Temp file name")
	flag.Parse()

	var my MySQLPlugin

	if *optSocket != "" {
		my.Target = *optSocket
		my.isUnixSocket = true
	} else {
		my.Target = fmt.Sprintf("%s:%s", *optHost, *optPort)
	}

	my.Username = *optUser
	my.Password = *optPass
	my.Database = *optDatabase
	my.Table = *optTable
	my.Column = *optColumn
	my.prefix = *optMetricKeyPrefix

	helper := mp.NewMackerelPlugin(my)
	helper.Tempfile = *optTempfile
	if helper.Tempfile == "" {
		helper.Tempfile = fmt.Sprintf("/tmp/mackerel-plugin-%s", *optMetricKeyPrefix)
	}
	helper.Run()
}

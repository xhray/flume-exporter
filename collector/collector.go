package collector

import (
	log "github.com/Sirupsen/logrus"
	simpleJson "github.com/bitly/go-simplejson"

	"zhidaoauto.com/prometheus/flume-exporter/util"
)

type Job struct {}

type FlumeMetric struct {
	Metrics map[string]interface{}
}

func (f *FlumeMetric) GetMetrics(flumeMetricUrl string) FlumeMetric {
	flumeMetric := FlumeMetric{}

	httpClient := util.HttpClient{}
	json, err := httpClient.Get(flumeMetricUrl)
	if err != nil {
		log.Errorf("HttpClient.Get = %v", err)
		return flumeMetric
	}

	js, err := simpleJson.NewJson([]byte(json))
	if err != nil {
		log.Errorf("simpleJson.NewJson = %v", err)
		return flumeMetric
	}

	flumeMetricMap := make(map[string]interface{})
	flumeMetricMap, _ = js.Map()
	flumeMetric.Metrics = flumeMetricMap

	return flumeMetric
}
package exporter

import (
	"fmt"
	"strconv"
	"strings"
	log "github.com/Sirupsen/logrus"

	"gopkg.in/yaml.v2"
  	"io/ioutil"

	"github.com/prometheus/client_golang/prometheus"
	"zhidaoauto.com/prometheus/flume-exporter/collector"
)

type Exporter struct {
	gauges			map[string]prometheus.Gauge
	gaugeVecs		map[string]*prometheus.GaugeVec
	configFile		string
}

type Agent struct {
	Name string
	Enabled bool
	Hosts []string
}
  
type Conf struct {
	Agents []Agent
}

func NewExporter(namespace string, configFile string) *Exporter {
	gauges := make(map[string]prometheus.Gauge)
	gaugeVecs := make(map[string]*prometheus.GaugeVec)

	// source metrics
	gaugeVecs["SOURCE_AppendBatchAcceptedCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name:      "SOURCE_AppendBatchAcceptedCount",
		Help:      "SOURCE_AppendBatchAcceptedCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SOURCE_EventReceivedCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SOURCE_EventReceivedCount",
		Help: "SOURCE_EventReceivedCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SOURCE_AppendReceivedCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SOURCE_AppendReceivedCount",
		Help: "SOURCE_AppendReceivedCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SOURCE_EventAcceptedCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SOURCE_EventAcceptedCount",
		Help: "SOURCE_EventAcceptedCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SOURCE_StartTime"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SOURCE_StartTime",
		Help: "SOURCE_StartTime"},
		[]string{"host", "type", "name"})
	gaugeVecs["SOURCE_AppendAcceptedCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SOURCE_AppendAcceptedCount",
		Help: "SOURCE_AppendAcceptedCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SOURCE_OpenConnectionCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SOURCE_OpenConnectionCount",
		Help: "SOURCE_OpenConnectionCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SOURCE_AppendBatchReceivedCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SOURCE_AppendBatchReceivedCount",
		Help: "SOURCE_AppendBatchReceivedCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SOURCE_StopTime"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SOURCE_StopTime",
		Help: "SOURCE_StopTime"},
		[]string{"host", "type", "name"})

	// channel metrics
	gaugeVecs["CHANNEL_Unhealthy"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_Unhealthy",
		Help: "CHANNEL_Unhealthy"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_ChannelSize"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_ChannelSize",
		Help: "CHANNEL_ChannelSize"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_EventTakeAttemptCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_EventTakeAttemptCount",
		Help: "CHANNEL_EventTakeAttemptCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_StartTime"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_StartTime",
		Help: "CHANNEL_StartTime"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_CheckpointWriteErrorCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_CheckpointWriteErrorCount",
		Help: "CHANNEL_CheckpointWriteErrorCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_ChannelCapacity"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_ChannelCapacity",
		Help: "CHANNEL_ChannelCapacity"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_ChannelFillPercentage"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_ChannelFillPercentage",
		Help: "CHANNEL_ChannelFillPercentage"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_EventTakeErrorCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_EventTakeErrorCount",
		Help: "CHANNEL_EventTakeErrorCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_EventTakeSuccessCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_EventTakeSuccessCount",
		Help: "CHANNEL_EventTakeSuccessCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_CheckpointBackupWriteErrorCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_CheckpointBackupWriteErrorCount",
		Help: "CHANNEL_CheckpointBackupWriteErrorCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_Closed"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_Closed",
		Help: "CHANNEL_Closed"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_EventPutAttemptCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_EventPutAttemptCount",
		Help: "CHANNEL_EventPutAttemptCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_EventPutSuccessCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_EventPutSuccessCount",
		Help: "CHANNEL_EventPutSuccessCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_EventPutErrorCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_EventPutErrorCount",
		Help: "CHANNEL_EventPutErrorCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["CHANNEL_StopTime"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "CHANNEL_StopTime",
		Help: "CHANNEL_StopTime"},
		[]string{"host", "type", "name"})

	// sink metrics
	gaugeVecs["SINK_ConnectionCreatedCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_ConnectionCreatedCount",
		Help: "SINK_ConnectionCreatedCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SINK_BatchCompleteCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_BatchCompleteCount",
		Help: "SINK_BatchCompleteCount"},
		[]string{"host", "type", "name"})	
	gaugeVecs["SINK_BatchEmptyCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_BatchEmptyCount",
		Help: "SINK_BatchEmptyCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SINK_EventDrainAttemptCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_EventDrainAttemptCount",
		Help: "SINK_EventDrainAttemptCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SINK_StartTime"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_StartTime",
		Help: "SINK_StartTime"},
		[]string{"host", "type", "name"})
	gaugeVecs["SINK_BatchUnderflowCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_BatchUnderflowCount",
		Help: "SINK_BatchUnderflowCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SINK_ConnectionFailedCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_ConnectionFailedCount",
		Help: "SINK_ConnectionFailedCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SINK_ConnectionClosedCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_ConnectionClosedCount",
		Help: "SINK_ConnectionClosedCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SINK_RollbackCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_RollbackCount",
		Help: "SINK_RollbackCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SINK_EventDrainSuccessCount"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_EventDrainSuccessCount",
		Help: "SINK_EventDrainSuccessCount"},
		[]string{"host", "type", "name"})
	gaugeVecs["SINK_KafkaEventSendTimer"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_KafkaEventSendTimer",
		Help: "SINK_KafkaEventSendTimer"},
		[]string{"host", "type", "name"})
	gaugeVecs["SINK_StopTime"] = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: namespace,
		Name: "SINK_StopTime",
		Help: "SINK_StopTime"},
		[]string{"host", "type", "name"})

	return &Exporter {
		gauges:		gauges,
		gaugeVecs:	gaugeVecs,
		configFile: configFile,
	}
}

func (e *Exporter) Describe(ch chan<- *prometheus.Desc) {
	for _, value := range e.gauges {
		value.Describe(ch)
	}

	for _, value := range e.gaugeVecs {
		value.Describe(ch)
	}
}

func (e *Exporter) Collect(ch chan<- prometheus.Metric) {
	log.Debugf("%v", e)

	e.collectGaugeVec()

	for _, value := range e.gauges {
		value.Collect(ch)
	}

	for _, value := range e.gaugeVecs {
		value.Collect(ch)
	}
}

func (e *Exporter) collectGaugeVec() {
	f := collector.FlumeMetric{}

	flumeMetricUrls := []string{};

	conf := getConf(e.configFile)
	for _, agent := range conf.Agents {
		if agent.Enabled {
			for _, host := range agent.Hosts {
				flumeMetricUrls = append(flumeMetricUrls, host)
			}
		}
	}

	// flumeMetricUrls := e.flumeMetricUrls
	log.Debugf("flumeMetricUrls=%v", flumeMetricUrls)

	channel := make(chan collector.FlumeMetric)
	for _, url := range flumeMetricUrls {
		go func(url string) {
			channel <- f.GetMetrics(url)
		}(url)
	}

	// receive from all channels
	for i := 0; i < len(flumeMetricUrls); i++ {
		m := <-channel
		url := flumeMetricUrls[i]

		for k, v := range m.Metrics {
			sMetrics := make(map[string]interface{})
			sMetrics = v.(map[string]interface{})
			delete(sMetrics, "Type")

			if strings.HasPrefix(k, "SOURCE") {
				name := strings.Replace(k, "SOURCE.", "", 1)
				fmt.Println(name)

				for mName, mValue := range sMetrics {
					val, err := strconv.ParseFloat(mValue.(string), 64)
					if err != nil {
						log.Errorf("value = %v", val)
						val = 0
					}

					e.gaugeVecs["SOURCE_" + mName].WithLabelValues(url, "SOURCE", name).Set(val)
				}
			} else if strings.HasPrefix(k, "CHANNEL") {
				delete(sMetrics, "Open")
				name := strings.Replace(k, "CHANNEL.", "", 1)
				fmt.Println(name)

				for mName, mValue := range sMetrics {
					val, err := strconv.ParseFloat(mValue.(string), 64)
					if err != nil {
						log.Errorf("value = %v", val)
						val = 0
					}

					e.gaugeVecs["CHANNEL_" + mName].WithLabelValues(url, "CHANNEL", name).Set(val)
				}
			} else if strings.HasPrefix(k, "SINK.") {
				name := strings.Replace(k, "SINK.", "", 1)
				fmt.Println(name)

				for mName, mValue := range sMetrics {
					val, err := strconv.ParseFloat(mValue.(string), 64)
					if err != nil {
						log.Errorf("value = %v", val)
						val = 0
					}

					e.gaugeVecs["SINK_" + mName].WithLabelValues(url, "SINK", name).Set(val)
				}
			}
		}
	}
}

func getConf(configFile string) *Conf {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
	  log.Fatalf("could not read flume.yml file; err: <%s>", err)
	}
	conf := Conf{}
	err = yaml.Unmarshal(data, &conf)
	if err != nil {
	  log.Fatalf("Unmarshal: %v", err)
	}
  
	return &conf
}
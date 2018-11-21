package main

import (
  "fmt"
  log "github.com/Sirupsen/logrus"
  "gopkg.in/urfave/cli.v2"
  "net/http"
  "os"

  "github.com/prometheus/client_golang/prometheus"
  "zhidaoauto.com/prometheus/flume-exporter/exporter"
)

const (
  version = "1.0.0"
  endpoint = "/metrics"
  namespace = "flume"
)

type appOpts struct {
  Name string
  Version string
  Flags []cli.Flag
}

func main() {
  opts := &appOpts {
    Name: "flume_exporter",
    Version: version,
  }

  opts.Flags = []cli.Flag {
    &cli.StringFlag {
      Name: "log-level",
      Usage: "Set Logging level",
      Value: "info",
    },

    &cli.StringFlag {
      Name: "config",
      Usage: "set config file",
      Value: "flume.yml",
    },

    &cli.IntFlag {
      Name: "port",
      Usage: "The port number used to expose metrics via http",
      Value: 9160,
    },

    &cli.StringFlag {
      Name: "flume-metric-url",
      Usage: "get flume metric via http",
    },
  }

  log.Debugf("opts = %v", opts)

  err := newApp(opts).Run(os.Args)
  if err != nil {
    os.Exit(1)
  }
}

func newApp(opts *appOpts) *cli.App {
  return &cli.App {
    Name: opts.Name,
    Version: opts.Version,
    Usage: "Prometheus exporter for Apache Flume",
    Flags: opts.Flags,
    Action: action,
  }
}

func action(c *cli.Context) error {
  setupLogging(c)

  exporter := exporter.NewExporter(namespace, c.String("config"))
  prometheus.MustRegister(exporter)

  // http listen and serve
  port := c.Int("port")
  log.Debugf("port = %v", port)

  http.HandleFunc("/", func(w http.ResponseWriter, req *http.Request) {
    w.Header().Add("Location", endpoint)
    w.WriteHeader(http.StatusMovedPermanently)
  })

  http.Handle(endpoint, prometheus.Handler())

  if err := http.ListenAndServe(fmt.Sprintf(":%d", port), nil); err != nil {
		log.Fatal(err)
  }
  
  return nil
}

func setupLogging(c *cli.Context) {
  log.SetFormatter(&log.TextFormatter {
    FullTimestamp: true,
  })
  levelString := c.String("log-level")
  level, err := log.ParseLevel(levelString)
  if err != nil {
    log.Fatalf("could not set log level to '%s';err:<%s>", levelString, err)
  }
  log.SetLevel(level)
}
package main

import (
	"fmt"

	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

func main() {
	collector, err := jobruntime.NewMetricsCollector(jobruntime.MetricDimensions{})
	if err != nil {
		panic(err)
	}
	fmt.Print(collector.PrometheusText())
}

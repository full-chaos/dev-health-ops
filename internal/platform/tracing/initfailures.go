package tracing

import (
	"io"
	"sync/atomic"
)

// initFailureMetric is the Python family's name (tracing.py
// DEV_HEALTH_OTEL_INIT_FAILURES_TOTAL), kept so the series is the same on
// either plane.
const initFailureMetric = "dev_health_otel_init_failures_total"

// initFailureCounter counts failed tracing initialisations in this process.
// Python retries once and labels attempt="initial" then "final"; Go does not
// retry, so a failure here is by construction the final one: it is counted as
// attempt="final" and attempt="initial" stays at zero. Without this a process
// whose tracing failed to start looked identical on /metrics to a healthy one
// (CHAOS-8219): the only trace of it was one WARN log line.
type initFailureCounter struct{ final atomic.Uint64 }

var initFailures = &initFailureCounter{}

func (c *initFailureCounter) record() { c.final.Add(1) }

// WritePrometheus writes both attempt series, so the family is present at
// zero on a healthy process (health.MetricsSource).
func (c *initFailureCounter) WritePrometheus(w io.Writer) error {
	if _, err := io.WriteString(w,
		"# HELP "+initFailureMetric+" OpenTelemetry tracing initialisation failures, by attempt (Go does not retry: every failure is the final one).\n"+
			"# TYPE "+initFailureMetric+" counter\n"+
			initFailureMetric+"{attempt=\"initial\"} 0\n"); err != nil {
		return err
	}
	_, err := io.WriteString(w, initFailureMetric+"{attempt=\"final\"} "+uitoa(c.final.Load())+"\n")
	return err
}

func uitoa(v uint64) string {
	if v == 0 {
		return "0"
	}
	var buf [20]byte
	i := len(buf)
	for v > 0 {
		i--
		buf[i] = byte('0' + v%10)
		v /= 10
	}
	return string(buf[i:])
}

// InitFailuresSource is the process-wide counter for health.Registry.RegisterMetrics.
func InitFailuresSource() *initFailureCounter { return initFailures }

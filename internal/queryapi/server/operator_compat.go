package server

import (
	"fmt"
	"io"
	"net/http"
	"strings"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

// OperatorCompat serves /healthz, /readyz and /metrics on the QUERY listener for one
// release after they moved to the operator listener (CHAOS-6447, D2627): the chart's
// probes and the scrape still point at the query port until the deploy repo moves them,
// so the image must answer there as it always did and the two changes need no
// ordering. It is deliberately the old shape, not the operator's:
//
//   - /healthz is 200 "ok" (pure liveness);
//   - /readyz is the plain text the query listener always answered (the runbook asserts
//     these bytes): 200 "ready", 200 "ready: /query not configured" when no /query
//     dependency is configured, and 503 "not ready: <class>" (clickhouse, postgres, jwks,
//     postgres_posture) where the class is the only thing said about a failure
//     (CHAOS-4724: /readyz is unauthenticated), from the same required checks the
//     operator /readyz runs;
//   - /metrics is the process's metrics text (the operator's: the runtime series and the
//     OTel instruments) plus the target_info series the old query listener exported
//     (service_name and the SDK identity), so a scrape or alert keyed on it keeps its
//     series for the release. The OTel instruments keep their names (none declares a
//     unit) but no longer carry the otel_scope_* labels.
//
// The following release removes this (a test asserts the query port no longer answers).
func OperatorCompat(registry *health.Registry, operator *health.Server, serviceName string) http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; charset=utf-8")
		status := registry.Readiness(r.Context())
		if status.Ready {
			w.WriteHeader(http.StatusOK)
			for _, check := range status.Checks {
				if check.Name == notConfiguredCheckName {
					_, _ = w.Write([]byte("ready: /query not configured"))
					return
				}
			}
			_, _ = w.Write([]byte("ready"))
			return
		}
		// Only fixed bodies are ever written: the class is chosen from a closed set,
		// never composed from an error or a request.
		w.WriteHeader(http.StatusServiceUnavailable)
		switch compatFailureClass(status.Failed) {
		case readyzClassClickHouse:
			_, _ = w.Write([]byte("not ready: clickhouse"))
		case readyzClassPostgres:
			_, _ = w.Write([]byte("not ready: postgres"))
		case readyzClassJWKS:
			_, _ = w.Write([]byte("not ready: jwks"))
		case readyzClassPosture:
			_, _ = w.Write([]byte("not ready: postgres_posture"))
		default:
			_, _ = w.Write([]byte("not ready: dependency"))
		}
	})
	metrics := operator.Handler()
	mux.HandleFunc("/metrics", func(w http.ResponseWriter, r *http.Request) {
		// The operator's answer goes straight through; on 200 the old target_info
		// series follows it (the operator's text ends in a newline, so it starts a line).
		tracked := &statusTracker{ResponseWriter: w}
		metrics.ServeHTTP(tracked, r)
		if tracked.status == http.StatusOK {
			_, _ = io.WriteString(w, targetInfo(serviceName))
		}
	})
	return mux
}

// compatFailureClass is the class the old /readyz body named for the failing checks: the
// first of clickhouse, postgres, jwks, postgres_posture (the order the old single check
// ran them in) that failed, or the generic "dependency" for anything else (the listener
// not bound yet, the readiness gate not open).
func compatFailureClass(failed []string) string {
	failing := map[string]bool{}
	for _, name := range failed {
		failing[name] = true
	}
	for _, class := range []struct{ check, class string }{
		{"query_clickhouse", readyzClassClickHouse},
		{"query_postgres", readyzClassPostgres},
		{"query_jwks", readyzClassJWKS},
		{"query_role_posture", readyzClassPosture},
	} {
		if failing[class.check] {
			return class.class
		}
	}
	return "dependency"
}

// targetInfo is the target_info series of the old query listener's OTel Prometheus
// exporter: one sample of value 1 carrying the service name and the SDK identity.
func targetInfo(serviceName string) string {
	labels := []attribute.KeyValue{
		attribute.String("service_name", serviceName),
		attribute.String("telemetry_sdk_language", "go"),
		attribute.String("telemetry_sdk_name", "opentelemetry"),
		attribute.String("telemetry_sdk_version", sdk.Version()),
	}
	pairs := make([]string, 0, len(labels))
	for _, label := range labels {
		pairs = append(pairs, fmt.Sprintf("%s=%q", label.Key, label.Value.AsString()))
	}
	return "# HELP target_info Target metadata\n# TYPE target_info gauge\ntarget_info{" + strings.Join(pairs, ",") + "} 1\n"
}

// statusTracker passes a response through and remembers its status.
type statusTracker struct {
	http.ResponseWriter
	status int
}

func (t *statusTracker) WriteHeader(status int) {
	t.status = status
	t.ResponseWriter.WriteHeader(status)
}

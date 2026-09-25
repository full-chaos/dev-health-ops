package internalidentity

import (
	"context"
	"log"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// The identity headers are honoured only for a request that arrived on the
// INTERNAL listener (CHAOS-6780). query-api serves /query, /buildinfo and the
// browser-reachable /api/v1/* routes from one mux; without a listener
// boundary the only thing keeping a browser-set identity header out of /query
// is that no Ingress path names it. The public listener therefore deletes the
// four headers before any handler sees them, loudly; the internal listener
// (QUERY_API_INTERNAL_ADDR, a port no Ingress routes to) marks the request
// context, which authenticateInternalRequest requires before it reads them.

type listenerKey struct{}

// Internal marks every request served through next as arriving on the
// internal listener.
func Internal(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		next.ServeHTTP(w, r.WithContext(context.WithValue(r.Context(), listenerKey{}, true)))
	})
}

// OnInternalListener reports whether the request arrived on the internal
// listener. A request that never passed through Internal is not on it.
func OnInternalListener(ctx context.Context) bool {
	on, _ := ctx.Value(listenerKey{}).(bool)
	return on
}

var droppedCounter = mustDroppedCounter()

func mustDroppedCounter() metric.Int64Counter {
	const name = "devhealth_query_api_internal_headers_dropped_total"
	counter, err := otel.Meter("github.com/full-chaos/dev-health-ops/internal/queryapi/internalidentity").Int64Counter(
		name,
		metric.WithDescription("Requests on the public listener that carried X-DH-Internal-* headers, which were deleted, by path class"),
	)
	if err != nil {
		counter, _ = otel.GetMeterProvider().Meter("noop").Int64Counter(name)
	}
	return counter
}

// pathClass bounds the metric label: the path is caller-controlled.
func pathClass(path string) string {
	switch {
	case path == "/query":
		return "query"
	case path == "/query/proof":
		return "query_proof"
	case path == "/buildinfo":
		return "buildinfo"
	case strings.HasPrefix(path, "/api/v1/"):
		return "rest"
	default:
		return "other"
	}
}

// dropLogGate lets the drop line through at most once a second, so a request
// flood cannot become a log flood; the counter still counts every request.
var dropLogGate atomic.Int64

// Public deletes the internal identity headers from every request served
// through next, and says so: one counter increment per request, and a log
// line naming the path and the header NAMES (never a value).
func Public(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if Present(r.Header) {
			var names []string
			for _, name := range Headers {
				if len(r.Header.Values(name)) > 0 {
					names = append(names, name)
					r.Header.Del(name)
				}
			}
			droppedCounter.Add(r.Context(), 1, metric.WithAttributes(attribute.String("path_class", pathClass(r.URL.Path))))
			if last, now := dropLogGate.Load(), time.Now().UnixNano(); now-last >= int64(time.Second) && dropLogGate.CompareAndSwap(last, now) {
				log.Printf("query-api: dropped internal identity headers on the public listener: path_class=%s headers=%s",
					pathClass(r.URL.Path), strings.Join(names, ","))
			}
		}
		next.ServeHTTP(w, r)
	})
}

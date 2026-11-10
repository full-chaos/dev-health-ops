package accessverify

import (
	"context"
	"net/http"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-go/authverify"
	"github.com/full-chaos/dev-health-ops/internal/httpguard"
)

const (
	// JWKSFetchMetric counts the discovery and JWKS GETs of a verifier's JWKS
	// client, by service, document (discovery, jwks) and result (ok, fail).
	JWKSFetchMetric = "devhealth_auth_jwks_fetch_total"
	// JWKSGraceMetric is the key-set state of the process's JWKS clients:
	// JWKSStateFresh, JWKSStateGrace or JWKSStateExpired. It carries no label;
	// the resource names the service.
	JWKSGraceMetric = "devhealth_auth_jwks_grace_state"
)

// Values of JWKSGraceMetric.
const (
	JWKSStateFresh   int64 = 0
	JWKSStateGrace   int64 = 1
	JWKSStateExpired int64 = 2
)

// NewJWKSKeys builds the JWKS client of cfg for service, with its fetches
// counted and its grace state reported on meter. Every GET goes through the
// client's transport: a 200 is ok, any other status or a transport error is a
// fail. A 200 whose document the client then refuses still counts ok.
func NewJWKSKeys(cfg authverify.JWKSClientConfig, service string, meter metric.Meter) (*authverify.JWKSClient, error) {
	fetches, err := meter.Int64Counter(JWKSFetchMetric,
		metric.WithDescription("Discovery and JWKS GETs of a verifier's JWKS client, by service, document and result."))
	if err != nil {
		return nil, err
	}
	observer := &jwksObserver{service: service, fetches: fetches, now: cfg.Now, ttl: cfg.CacheTTL, grace: cfg.OutageGrace}
	if observer.now == nil {
		observer.now = time.Now
	}
	if err := registerGraceObserver(meter, service, observer); err != nil {
		return nil, err
	}
	client := http.Client{Transport: httpguard.SharedTransport()}
	if cfg.HTTPClient != nil {
		client = *cfg.HTTPClient
	}
	observer.base = client.Transport
	if observer.base == nil {
		observer.base = httpguard.SharedTransport()
	}
	client.Transport = observer
	cfg.HTTPClient = &client
	return authverify.NewJWKSClient(cfg)
}

type jwksObserver struct {
	service string
	fetches metric.Int64Counter
	base    http.RoundTripper
	now     func() time.Time
	ttl     time.Duration
	grace   time.Duration

	mu        sync.Mutex
	attempted bool
	failed    bool
	lastOK    time.Time
}

func (o *jwksObserver) RoundTrip(r *http.Request) (*http.Response, error) {
	response, err := o.base.RoundTrip(r)
	ok := err == nil && response.StatusCode == http.StatusOK
	document, result := "discovery", "fail"
	if strings.HasSuffix(r.URL.Path, "/jwks.json") {
		document = "jwks"
	}
	if ok {
		result = "ok"
	}
	o.fetches.Add(r.Context(), 1, metric.WithAttributes(
		attribute.String("service", o.service), attribute.String("document", document), attribute.String("result", result)))
	o.mu.Lock()
	o.attempted = true
	switch {
	case !ok:
		o.failed = true
	case document == "jwks":
		o.failed, o.lastOK = false, o.now()
	}
	o.mu.Unlock()
	return response, err
}

// state is the client's key-set state, false before its first GET.
func (o *jwksObserver) state() (int64, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	switch {
	case !o.attempted:
		return 0, false
	case !o.failed:
		return JWKSStateFresh, true
	case o.lastOK.IsZero():
		return JWKSStateExpired, true
	}
	age := o.now().Sub(o.lastOK)
	switch {
	case age < o.ttl:
		return JWKSStateFresh, true
	case age < o.ttl+o.grace:
		return JWKSStateGrace, true
	}
	return JWKSStateExpired, true
}

// One grace gauge per meter and service reports the worst state of the
// service's clients (query-api builds two).
type graceKey struct {
	meter   metric.Meter
	service string
}

type graceGroup struct {
	mu        sync.Mutex
	observers []*jwksObserver
}

var (
	graceMu     sync.Mutex
	graceGroups = map[graceKey]*graceGroup{}
)

func registerGraceObserver(meter metric.Meter, service string, observer *jwksObserver) error {
	graceMu.Lock()
	defer graceMu.Unlock()
	key := graceKey{meter: meter, service: service}
	if group, ok := graceGroups[key]; ok {
		group.mu.Lock()
		group.observers = append(group.observers, observer)
		group.mu.Unlock()
		return nil
	}
	group := &graceGroup{observers: []*jwksObserver{observer}}
	_, err := meter.Int64ObservableGauge(JWKSGraceMetric,
		metric.WithDescription("Key-set state of the JWKS clients: 0 fresh, 1 served from the outage grace, 2 past the grace."),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			group.mu.Lock()
			defer group.mu.Unlock()
			worst, seen := int64(0), false
			for _, observer := range group.observers {
				if state, ok := observer.state(); ok {
					worst, seen = max(worst, state), true
				}
			}
			if seen {
				o.Observe(worst)
			}
			return nil
		}))
	if err != nil {
		return err
	}
	graceGroups[key] = group
	return nil
}

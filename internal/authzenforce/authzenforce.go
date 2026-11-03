// Package authzenforce is the per-namespace, per-service switch that turns
// decision-point enforcement on. Every cell is off by default; nothing here
// reads policy data, so flipping a cell back changes no policy row.
package authzenforce

import (
	"context"
	"encoding/json"
	"log/slog"
	"slices"
	"strings"
	"sync"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-ops/contracts/auth/v1/vocabulary"
)

// Service names a process that enforces.
type Service string

const (
	GoAPI    Service = "go-api"
	QueryAPI Service = "query-api"
)

// Services are the services that have cells.
func Services() []Service { return []Service{GoAPI, QueryAPI} }

// Env is the environment variable that lists the service's enforced
// namespaces; "" for a service that has no cells.
func (s Service) Env() string {
	switch s {
	case GoAPI:
		return "GO_API_AUTHZ_ENFORCE"
	case QueryAPI:
		return "QUERY_API_AUTHZ_ENFORCE"
	}
	return ""
}

var vocabularyNamespaces = sync.OnceValue(func() []string {
	var catalog struct {
		Actions []struct {
			Action string `json:"action"`
		} `json:"actions"`
	}
	if err := json.Unmarshal(vocabulary.ActionsV1, &catalog); err != nil {
		return nil
	}
	var namespaces []string
	for _, a := range catalog.Actions {
		if namespace, _, dotted := strings.Cut(a.Action, "."); dotted && !slices.Contains(namespaces, namespace) {
			namespaces = append(namespaces, namespace)
		}
	}
	slices.Sort(namespaces)
	return namespaces
})

// Namespaces are the action namespaces of the catalog, sorted. They come from
// the embedded vocabulary, so a namespace added to the catalog is a new cell.
func Namespaces() []string { return slices.Clone(vocabularyNamespaces()) }

// Switch is one service's immutable set of enforced namespaces. The zero value
// is off everywhere.
type Switch struct {
	service Service
	on      map[string]bool
	ignored int
}

// Parse reads a comma-separated list of namespaces. An item that is not a
// catalog namespace, an empty item, a repeat and "*" are ignored and counted;
// the namespaces that stay off are the ones not named.
func Parse(service Service, raw string) Switch {
	sw := Switch{service: service}
	if service.Env() == "" || strings.TrimSpace(raw) == "" {
		return sw
	}
	known := vocabularyNamespaces()
	for _, item := range strings.Split(raw, ",") {
		item = strings.TrimSpace(item)
		if !slices.Contains(known, item) || sw.on[item] {
			sw.ignored++
			continue
		}
		if sw.on == nil {
			sw.on = map[string]bool{}
		}
		sw.on[item] = true
	}
	return sw
}

// Load reads the service's variable through lookup. A bad value never stops
// the service: the items it cannot accept stay off, one ERROR line says how
// many, and Register exposes the count. The line never quotes a value.
func Load(service Service, lookup func(string) (string, bool), logger *slog.Logger) Switch {
	env := service.Env()
	if env == "" || lookup == nil {
		return Switch{service: service}
	}
	raw, _ := lookup(env)
	sw := Parse(service, raw)
	if sw.ignored > 0 {
		if logger == nil {
			logger = slog.Default()
		}
		logger.ErrorContext(context.Background(), "authz enforcement switch: items ignored, their namespaces stay off",
			slog.String("env", env), slog.String("service", string(service)),
			slog.Int("ignored", sw.ignored), slog.Int("accepted", len(sw.on)))
	}
	return sw
}

// On reports whether the namespace is enforced.
func (s Switch) On(namespace string) bool { return s.on[namespace] }

// Ignored is how many list items were not accepted.
func (s Switch) Ignored() int { return s.ignored }

// Service is the service the switch was read for.
func (s Switch) Service() Service { return s.service }

const (
	enabledMetric = "dev_health_authz_enforcement_enabled"
	ignoredMetric = "dev_health_authz_enforcement_ignored_items"
)

// Register exposes sw as gauges on meter, set once now (a switch is read once and never changes): one series per
// namespace of the service (1 enforced, 0 not) and the count of ignored items. The labels are closed sets: the
// catalog's namespaces and the service. A switch with no service registers nothing.
func Register(meter metric.Meter, sw Switch) error {
	if sw.service.Env() == "" {
		return nil
	}
	enabledGauge, err := meter.Int64Gauge(enabledMetric,
		metric.WithDescription("1 while decision-point enforcement is on for the action namespace in this service"))
	if err != nil {
		return err
	}
	ignoredGauge, err := meter.Int64Gauge(ignoredMetric,
		metric.WithDescription("Items of the enforcement list this service could not accept; their namespaces stay off"))
	if err != nil {
		return err
	}
	ctx := context.Background()
	for _, namespace := range vocabularyNamespaces() {
		value := int64(0)
		if sw.on[namespace] {
			value = 1
		}
		enabledGauge.Record(ctx, value, metric.WithAttributes(attribute.String("namespace", namespace), attribute.String("service", string(sw.service))))
	}
	ignoredGauge.Record(ctx, int64(sw.ignored), metric.WithAttributes(attribute.String("service", string(sw.service))))
	return nil
}

// Package authzenforce is the per-namespace, per-service switch that turns
// decision-point enforcement on. Every cell is off by default; nothing here
// reads policy data.
package authzenforce

import (
	"log/slog"

	"go.opentelemetry.io/otel/metric"
)

// Service names a process that enforces.
type Service string

const (
	GoAPI    Service = "go-api"
	QueryAPI Service = "query-api"
)

// Services are the services that have cells.
func Services() []Service { return nil }

// Env is the environment variable that lists the service's enforced namespaces.
func (s Service) Env() string { return "" }

// Namespaces are the action namespaces of the catalog, sorted.
func Namespaces() []string { return nil }

// Switch is one service's immutable set of enforced namespaces. The zero value is off everywhere.
type Switch struct{}

// Parse reads a comma-separated list of namespaces.
func Parse(service Service, raw string) Switch { return Switch{} }

// Load reads the service's variable through lookup and logs any ignored item at ERROR.
func Load(service Service, lookup func(string) (string, bool), logger *slog.Logger) Switch {
	return Switch{}
}

// On reports whether the namespace is enforced.
func (s Switch) On(namespace string) bool { return false }

// Ignored is how many list items were not accepted.
func (s Switch) Ignored() int { return 0 }

// Service is the service the switch was read for.
func (s Switch) Service() Service { return "" }

// Register exposes the switch as gauges on meter.
func Register(meter metric.Meter, sw Switch) error { return nil }

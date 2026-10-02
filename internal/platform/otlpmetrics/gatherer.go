package otlpmetrics

import (
	"bytes"
	"log/slog"
	"sort"

	"github.com/prometheus/common/expfmt"
	"github.com/prometheus/common/model"

	dto "github.com/prometheus/client_model/go"
)

// FragmentSource hands out the registered Prometheus-text fragments, one per
// source (internal/platform/health.Registry).
type FragmentSource interface {
	EachMetricsFragment(skip map[string]bool, fn func(source string, fragment []byte, err error))
}

// Gatherer is a prometheus.Gatherer over a FragmentSource: each collection
// writes every fragment, parses it into metric families, and merges the
// families by name. A source that fails to write or to parse is skipped, logged
// with its name and counted (Stats), never silently dropped and never able to
// fail the other sources.
type Gatherer struct {
	Source FragmentSource
	// Skip names the sources not to read: the process's OTel instruments, which
	// reach OTLP through their own reader and would otherwise be sent twice.
	Skip   map[string]bool
	Logger *slog.Logger
	Stats  *Stats
}

// Gather implements prometheus.Gatherer.
func (g *Gatherer) Gather() ([]*dto.MetricFamily, error) {
	parser := expfmt.NewTextParser(model.LegacyValidation)
	merged := map[string]*dto.MetricFamily{}
	g.Source.EachMetricsFragment(g.Skip, func(source string, fragment []byte, err error) {
		if err != nil {
			g.sourceFailed(source, "write", err)
			return
		}
		families, err := parser.TextToMetricFamilies(bytes.NewReader(fragment))
		if err != nil {
			g.sourceFailed(source, "parse", err)
			return
		}
		for name, family := range families {
			existing, seen := merged[name]
			if !seen {
				merged[name] = family
				continue
			}
			if existing.GetType() != family.GetType() {
				g.sourceFailed(source, "type conflict on family "+name, nil)
				continue
			}
			existing.Metric = append(existing.Metric, family.Metric...)
		}
	})
	names := make([]string, 0, len(merged))
	for name := range merged {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]*dto.MetricFamily, 0, len(names))
	for _, name := range names {
		out = append(out, merged[name])
	}
	return out, nil
}

func (g *Gatherer) sourceFailed(source, stage string, err error) {
	if g.Stats != nil {
		g.Stats.bridgeSourceFailed(source)
	}
	if g.Logger != nil {
		attrs := []any{"source", source, "stage", stage}
		if err != nil {
			attrs = append(attrs, "error", err)
		}
		g.Logger.Warn("otlp metrics bridge: skipped a metrics source", attrs...)
	}
}

package chartgates_test

import (
	"bytes"
	"context"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/health"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
	"github.com/full-chaos/dev-health-ops/internal/syncreconciler"
)

// Guards that read Go code run it: the sweep and rearm series come from a real scrape of the loops registered
// on a health registry, the artifact-skip series from the real providerfoundation exporter, the credential,
// flag and route surface from the real option registry and Config type. A planted defect for these is a mutated
// copy of the Go file compiled in through `go test -overlay` (see TestExecGuardsPlantedByOverlay).

type zeroObserver struct{}

func (zeroObserver) Step(context.Context, time.Time, int) (syncreconciler.Observation, error) {
	return syncreconciler.Observation{}, nil
}

type zeroRelay struct{}

func (zeroRelay) Step(context.Context, time.Time, int) (joboutbox.StepResult, error) {
	return joboutbox.StepResult{}, nil
}

// scrapeSweepSeries registers the sync-dispatch observer and the outbox reconciler on one registry, exactly
// as the worker does, and returns the registry's /metrics output.
func scrapeSweepSeries(g *guard) string {
	reg := health.NewRegistry(time.Second)
	if _, err := syncreconciler.NewLoop(zeroObserver{}, syncreconciler.DefaultLoopConfig(reg)); err != nil {
		g.failf("cannot construct the sync-dispatch observer loop: %v", err)
		return ""
	}
	if _, err := joboutbox.NewReconcilerLoop(zeroRelay{}, joboutbox.DefaultReconcilerLoopConfig(reg)); err != nil {
		g.failf("cannot construct the outbox reconciler loop: %v", err)
		return ""
	}
	var out bytes.Buffer
	if err := reg.WriteMetrics(&out); err != nil {
		g.failf("the registry scrape failed: %v", err)
	}
	return out.String()
}

func hasSample(scrape, metric string) bool {
	return regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(metric) + `[ {]`).MatchString(scrape)
}

var sampleLabels = regexp.MustCompile(`(\w+)="`)

// scrapeArtifactSkips records one skipped artifact on the real exporter and returns the metric name and label
// keys of the series that came out.
func scrapeArtifactSkips(g *guard) (metric string, labels []string) {
	m := providerfoundation.NewMetrics()
	m.RecordArtifactSkipped("github", "commits", "unreadable")
	var out bytes.Buffer
	if err := m.WritePrometheus(&out); err != nil {
		g.failf("providerfoundation WritePrometheus failed: %v", err)
		return "", nil
	}
	for _, line := range strings.Split(out.String(), "\n") {
		if strings.HasPrefix(line, "dev_health_provider_artifact_skipped") && strings.Contains(line, "{") {
			name, rest, _ := strings.Cut(line, "{")
			for _, m := range sampleLabels.FindAllStringSubmatch(rest, -1) {
				labels = append(labels, m[1])
			}
			sort.Strings(labels)
			return name, labels
		}
	}
	return "", nil
}

// registryFlagNames is every long flag and alias the real option registry accepts.
func registryFlagNames() map[string]bool {
	out := map[string]bool{}
	for _, o := range config.Options() {
		if o.Flag != "" {
			out[o.Flag] = true
		}
		for _, a := range o.Aliases {
			out[a] = true
		}
	}
	return out
}

func registrySecretEnv() []string {
	var out []string
	for _, o := range config.Options() {
		if o.Secret {
			out = append(out, o.Env)
		}
	}
	sort.Strings(out)
	return out
}

func configFieldNames() []string {
	var out []string
	t := reflect.TypeOf(config.Config{})
	for i := 0; i < t.NumField(); i++ {
		out = append(out, t.Field(i).Name)
	}
	return out
}

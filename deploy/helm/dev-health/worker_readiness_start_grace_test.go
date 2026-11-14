package devhealth_test

import (
	"os"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/full-chaos/dev-health-ops/internal/platform/health"
)

type readinessProbeWindow struct {
	InitialDelaySeconds *int `yaml:"initialDelaySeconds"`
	PeriodSeconds       *int `yaml:"periodSeconds"`
	FailureThreshold    *int `yaml:"failureThreshold"`
}

// window is the time from container start until the kubelet has counted a
// full failureThreshold run of failed readiness probes, with the Kubernetes
// defaults for absent fields (0, 10, 3).
func (probe readinessProbeWindow) window() time.Duration {
	initialDelay, period, failureThreshold := 0, 10, 3
	if probe.InitialDelaySeconds != nil {
		initialDelay = *probe.InitialDelaySeconds
	}
	if probe.PeriodSeconds != nil {
		period = *probe.PeriodSeconds
	}
	if probe.FailureThreshold != nil {
		failureThreshold = *probe.FailureThreshold
	}
	return time.Duration(initialDelay+period*failureThreshold) * time.Second
}

// health.ReadinessStartGrace keeps a dependency refusal at WARN while a
// worker, scheduler, reconciler or stream-runner process has never been
// Ready. It must cover every go-worker group's readiness probe window in the
// chart, or a pod the kubelet still treats as starting would log ERROR.
func TestReadinessStartGraceCoversEveryGoWorkerReadinessProbe(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile("values.yaml")
	if err != nil {
		t.Fatal(err)
	}
	var values struct {
		GoWorkers struct {
			DefaultProbes struct {
				ReadinessProbe *readinessProbeWindow `yaml:"readinessProbe"`
			} `yaml:"defaultProbes"`
			Groups []struct {
				Name           string                `yaml:"name"`
				ReadinessProbe *readinessProbeWindow `yaml:"readinessProbe"`
			} `yaml:"groups"`
		} `yaml:"goWorkers"`
	}
	if err := yaml.Unmarshal(raw, &values); err != nil {
		t.Fatal(err)
	}
	if values.GoWorkers.DefaultProbes.ReadinessProbe == nil {
		t.Fatal("goWorkers.defaultProbes.readinessProbe is missing")
	}
	seen := map[string]bool{}
	for _, group := range values.GoWorkers.Groups {
		seen[group.Name] = true
		probe := values.GoWorkers.DefaultProbes.ReadinessProbe
		if group.ReadinessProbe != nil {
			probe = group.ReadinessProbe
		}
		if window := probe.window(); health.ReadinessStartGrace < window {
			t.Errorf("goWorkers group %q readiness probe window %s exceeds health.ReadinessStartGrace %s",
				group.Name, window, health.ReadinessStartGrace)
		}
	}
	for _, name := range []string{
		"heavy", "ops", "sync", "sync-provider", "reconciler", "scheduler",
		"stream-external", "stream-ingest", "stream-pagerduty",
	} {
		if !seen[name] {
			t.Errorf("goWorkers group %q is not in values.yaml; the start grace cell no longer reads it", name)
		}
	}
}

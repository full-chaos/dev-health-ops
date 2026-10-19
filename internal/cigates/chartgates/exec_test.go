package chartgates_test

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"
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

// --- planted defects for the guards above ---

type overlayCase struct {
	name string
	file string
	old  string
	repl string
	run  string
	want string
}

// runWithOverlay compiles a mutated copy of one Go file into a nested `go test` of the named guard and returns
// its output and whether it failed. The planted text must land.
func runWithOverlay(t *testing.T, c overlayCase) (string, bool) {
	t.Helper()
	root := realRoot(t)
	src := filepath.Join(root, filepath.FromSlash(c.file))
	b, err := os.ReadFile(src)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(b), c.old) {
		t.Fatalf("plant %s: text %q not found, the defect did not land", c.file, c.old)
	}
	dir := t.TempDir()
	mutated := filepath.Join(dir, "mutated.go")
	if err := os.WriteFile(mutated, []byte(strings.Replace(string(b), c.old, c.repl, 1)), 0o644); err != nil {
		t.Fatal(err)
	}
	overlay := filepath.Join(dir, "overlay.json")
	js, _ := json.Marshal(map[string]map[string]string{"Replace": {src: mutated}})
	if err := os.WriteFile(overlay, js, 0o644); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command("go", "test", "-count=1", "-overlay="+overlay, "-run", c.run, "./internal/cigates/chartgates")
	cmd.Dir = root
	out, err := cmd.CombinedOutput()
	return string(out), err != nil
}

func TestExecGuardsPlantedByOverlay(t *testing.T) {
	cases := []overlayCase{
		{"the-reconciler-loop-returns-before-writing-the-sweep-series", "internal/syncreconciler/loop.go",
			"func (loop *Loop) WritePrometheus(output io.Writer) error {\n", "func (loop *Loop) WritePrometheus(output io.Writer) error {\n\treturn nil\n",
			"^TestAlertGuardsRealTree$/^sweep-every-alert-metric-emitted$", "does not carry as a sample"},
		{"the-outbox-loop-returns-before-writing-the-rearm-series", "internal/joboutbox/loop.go",
			"func (loop *ReconcilerLoop) WritePrometheus(output io.Writer) error {\n", "func (loop *ReconcilerLoop) WritePrometheus(output io.Writer) error {\n\treturn nil\n",
			"^TestAlertGuardsRealTree$/^sweep-every-alert-metric-emitted$", "does not carry as a sample"},
		{"the-sweep-sample-line-is-deleted-but-its-help-and-type-remain", "internal/syncreconciler/loop.go",
			`# TYPE sync_dispatch_unreclaimable_candidates gauge\nsync_dispatch_unreclaimable_candidates %d\n"`, `# TYPE sync_dispatch_unreclaimable_candidates gauge\n"`,
			"^TestAlertGuardsRealTree$/^sweep-reader-ignores-comments$", "no sample line"},
		{"the-artifact-skip-exporter-returns-early", "internal/providerfoundation/budget.go",
			"func (m *Metrics) WritePrometheus(writer io.Writer) error {\n", "func (m *Metrics) WritePrometheus(writer io.Writer) error {\n\treturn nil\n",
			"^TestAlertGuardsRealTree$/^skip-metric-name-pinned$", "writes the artifact-skipped series as"},
		{"the-artifact-skip-metric-is-renamed", "internal/providerfoundation/budget.go",
			`writer, "dev_health_provider_artifact_skipped_total",`, `writer, "dev_health_provider_artifact_skipped_total_v2",`,
			"^TestAlertGuardsRealTree$/^skip-metric-name-pinned$", "writes the artifact-skipped series as"},
		{"the-artifact-skip-third-label-is-renamed", "internal/providerfoundation/budget.go",
			`"reason", m.artifactSkipped,`, `"kind", m.artifactSkipped,`,
			"^TestAlertGuardsRealTree$/^skip-alert-targets-real-metric$", "real series carries labels"},
		{"a-credential-loses-its-secret-marker", "internal/platform/config/options.go",
			`{Env: "POSTGRES_URI", Secret: true,`, `{Env: "POSTGRES_URI", Secret: false,`,
			"^TestContractGuardsRealTree$/^cli-registry-declares-credentials$", "differ from the documented credential set"},
		{"a-route-flag-enters-the-registry", "internal/platform/config/options.go",
			"var coreOptions = []Option{\n", "var coreOptions = []Option{\n\t{Flag: \"routes\", Kind: KindString, Usage: \"x\"},\n",
			"^TestContractGuardsRealTree$/^cli-provider-routes-have-no-enablement-surface$", "--routes"},
		{"a-route-enablement-variable-enters-the-registry", "internal/platform/config/options.go",
			"var coreOptions = []Option{\n", "var coreOptions = []Option{\n\t{Env: \"WORKER_GITHUB_ENABLED\", Kind: KindString, Usage: \"x\"},\n",
			"^TestContractGuardsRealTree$/^cli-provider-routes-have-no-enablement-surface$", "route enablement variable"},
		{"a-route-enablement-field-enters-the-config", "internal/platform/config/config.go",
			"type Config struct {\n", "type Config struct {\n\tWorkerGithubEnabled bool\n",
			"^TestContractGuardsRealTree$/^cli-provider-routes-have-no-enablement-surface$", "route enablement field"},
		{"the-all-routes-preset-enters-the-registry", "internal/platform/config/options.go",
			"var coreOptions = []Option{\n", "var coreOptions = []Option{\n\t{Env: \"GO_PROVIDER_ROUTES\", Kind: KindString, Usage: \"x\"},\n",
			"^TestContractGuardsRealTree$/^cli-provider-routes-have-no-enablement-surface$", "GO_PROVIDER_ROUTES"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			out, failed := runWithOverlay(t, c)
			if !failed {
				t.Fatalf("planted defect %q was not caught: the nested guard passed:\n%s", c.name, out)
			}
			if !strings.Contains(out, c.want) {
				t.Fatalf("planted defect %q was caught, but the output lacks %q:\n%s", c.name, c.want, out)
			}
		})
	}
}

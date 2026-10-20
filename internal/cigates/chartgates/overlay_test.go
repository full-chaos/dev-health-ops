//go:build !race

package chartgates_test

// Nested `go test -overlay` builds: compile-heavy, so they run in the plain unit leg only and serially.
import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

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

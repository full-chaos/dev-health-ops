package devhealth_test

import (
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// CHAOS-12962: a `helm template` without --namespace takes the namespace of the kube client config, and
// inside a Kubernetes pod that is the pod's own (arc-runners on CI), not "default". Every test render goes
// through helmCommand, which pins the namespace when the caller did not.
const helmDefaultNamespace = "default"

func hasNamespaceArg(args []string) bool {
	for _, a := range args {
		if a == "-n" || a == "--namespace" || strings.HasPrefix(a, "--namespace=") || strings.HasPrefix(a, "-n=") {
			return true
		}
	}
	return false
}

func helmCommand(args ...string) *exec.Cmd {
	if len(args) > 0 && args[0] == "template" && !hasNamespaceArg(args) {
		args = append([]string{"template", "--namespace", helmDefaultNamespace}, args[1:]...)
	}
	return exec.Command("helm", args...)
}

var rawHelmExec = regexp.MustCompile(`exec\.Command(Context)?\([^)]*"helm"`)

// unpinnedHelmCalls names every line of src that starts helm without going through helmCommand.
func unpinnedHelmCalls(name, src string) []string {
	var bad []string
	for i, line := range strings.Split(src, "\n") {
		if rawHelmExec.MatchString(line) {
			bad = append(bad, name+":"+strconv.Itoa(i+1))
		}
	}
	return bad
}

// Updating the choke-point census requires reviewing every new caller.
var helmCallFiles = map[string]bool{
	"auth_backfill_policy_test.go":               true,
	"auth_service_test.go":                       true,
	"clickhouse_configd_render_test.go":          true,
	"clickhouse_replicas_test.go":                true,
	"clickhouse_started_test.go":                 true,
	"go_api_config_env_test.go":                  true,
	"go_api_drain_test.go":                       true,
	"go_api_fence_test.go":                       true,
	"go_api_fenced_marker_test.go":               true,
	"go_api_internal_listener_test.go":           true,
	"go_api_trusted_proxies_test.go":             true,
	"go_pgbouncer_secret_keep_test.go":           true,
	"go_pgbouncer_session_rollingupdate_test.go": true,
	"helm_namespace_test.go":                     true,
	"helm_shim_test.go":                          true,
	"hook_references_test.go":                    true,
	"hs256_secret_scope_test.go":                 true,
	"images_test.go":                             true,
	"ingress_host_refusals_test.go":              true,
	"internal_acr_boundary_test.go":              true,
	"migrate_job_test.go":                        true,
	"networkpolicy_ingress_port_test.go":         true,
	"networkpolicy_prod_shape_test.go":           true,
	"ordering_contract_env_test.go":              true,
	"query_api_mcp_listener_test.go":             true,
	"query_api_subcommand_test.go":               true,
	"reconciler_subcommand_test.go":              true,
	"render_cache_test.go":                       true,
	"scheduler_subcommand_test.go":               true,
	"stream_subcommand_test.go":                  true,
	"valkey_feed_acl_test.go":                    true,
	"worker_group_extra_env_test.go":             true,
	"worker_subcommand_test.go":                  true,
}

func TestEveryHelmTemplateCallPinsTheNamespace(t *testing.T) {
	t.Parallel()

	files, err := filepath.Glob("*_test.go")
	if err != nil || len(files) == 0 {
		t.Fatalf("no test sources found: %v", err)
	}
	sort.Strings(files)
	seenMemo := false
	for _, f := range files {
		b, err := os.ReadFile(f)
		if err != nil {
			t.Fatal(err)
		}
		source, err := parser.ParseFile(token.NewFileSet(), f, b, 0)
		if err != nil {
			t.Fatal(err)
		}
		ast.Inspect(source, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			name, ok := call.Fun.(*ast.Ident)
			if ok && (name.Name == "helmOutput" || name.Name == "helmCommand") && !helmCallFiles[f] {
				t.Errorf("%s: uncensused %s caller", f, name.Name)
			}
			return true
		})
		if f == "render_cache_test.go" {
			routesThroughCommand := false
			for _, decl := range source.Decls {
				fn, ok := decl.(*ast.FuncDecl)
				if !ok || fn.Name.Name != "helmOutput" {
					continue
				}
				ast.Inspect(fn.Body, func(node ast.Node) bool {
					call, ok := node.(*ast.CallExpr)
					if ok {
						if name, ok := call.Fun.(*ast.Ident); ok && name.Name == "helmCommand" {
							routesThroughCommand = true
						}
					}
					return true
				})
			}
			if !routesThroughCommand {
				t.Error("helmOutput must call helmCommand to pin the namespace")
			}
			seenMemo = true
		}
		if bad := unpinnedHelmCalls(f, string(b)); f != "helm_namespace_test.go" && len(bad) > 0 {
			t.Errorf("exec.Command(\"helm\", ...) bypasses helmCommand, so the render takes the kube context namespace: %v", bad)
		}
	}
	if !seenMemo {
		t.Error("render cache source missing from namespace census")
	}
}

func TestHelmCommandPinsNamespace(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		in   []string
		want []string
	}{
		{"unpinned", []string{"template", "t", "."}, []string{"template", "--namespace", "default", "t", "."}},
		{"explicit long", []string{"template", "t", ".", "--namespace", "rel-ns"}, []string{"template", "t", ".", "--namespace", "rel-ns"}},
		{"explicit equals", []string{"template", "t", ".", "--namespace=x"}, []string{"template", "t", ".", "--namespace=x"}},
		{"explicit short", []string{"template", "t", ".", "-n", "x"}, []string{"template", "t", ".", "-n", "x"}},
		{"not a render", []string{"show", "values", "."}, []string{"show", "values", "."}},
	}
	for _, c := range cases {
		got := helmCommand(c.in...).Args[1:]
		if strings.Join(got, " ") != strings.Join(c.want, " ") {
			t.Errorf("%s: args = %v, want %v", c.name, got, c.want)
		}
	}
}

func TestUnpinnedHelmCallGuardFires(t *testing.T) {
	t.Parallel()

	planted := map[string]string{
		"plain":   "\tout, err := exec.Command(\"helm\", \"template\", \"t\", \".\").CombinedOutput()\n",
		"context": "\tcmd := exec.CommandContext(ctx, \"helm\", args...)\n",
		"spread":  "\tcmd := exec.Command(\"helm\", append([]string{\"template\"}, a...)...)\n",
	}
	for name, src := range planted {
		if got := unpinnedHelmCalls("planted.go", src); len(got) != 1 {
			t.Errorf("planted %s: guard found %v, want one hit", name, got)
		}
	}
	clean := "\tout, err := helmCommand(\"template\", \"t\", \".\").CombinedOutput()\n"
	if got := unpinnedHelmCalls("clean.go", clean); len(got) != 0 {
		t.Errorf("clean source flagged: %v", got)
	}
}

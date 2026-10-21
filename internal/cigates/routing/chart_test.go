package routing_test

import (
	"bytes"
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"gopkg.in/yaml.v3"
)

// CHAOS-7536: a differential test of the router generator against the chart's own render. The chart is rendered with
// `helm template`, every rendered Ingress rule is resolved the way ingress-nginx resolves it, and the backend the chart
// gives a path is compared with the backend the router gives it on Host(`traefik`). Without helm the test FAILS: a
// measurement that did not happen is not a pass.

type chartRule struct{ path, pathType, service string }

type chartRender struct {
	refused   bool
	stderr    string
	ingresses []any
}

func renderChart(t *testing.T, chartValues m) chartRender {
	t.Helper()
	helm, err := exec.LookPath("helm")
	if err != nil {
		t.Fatal("helm is required for the CHAOS-7536 differential test")
	}
	root := routingRepoRoot(t)
	chart := filepath.Join(root, "deploy", "helm", "dev-health")
	// go test keys its cache on the files this process opens; helm reads the chart in another process.
	_ = filepath.WalkDir(chart, func(p string, d os.DirEntry, err error) error {
		if err == nil && !d.IsDir() {
			_, _ = os.ReadFile(p)
		}
		return nil
	})
	// helm v4 renders every template before --show-only filters, and web-deployment.yaml requires
	// web.env.BACKEND_URL (CHAOS-8310); the ingress never reads it.
	chartValues["web"] = m{"env": m{"BACKEND_URL": "http://backend.example"}, "authSecret": m{"secretName": "web-auth-placeholder"}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	cmd := exec.CommandContext(ctx, helm, "template", "differential", chart, "-f", "-", "--show-only", "templates/ingress.yaml")
	cmd.Stdin = bytes.NewReader(dump(t, chartValues))
	var so, se bytes.Buffer
	cmd.Stdout, cmd.Stderr = &so, &se
	if err := cmd.Run(); err != nil {
		var ee *exec.ExitError
		if !errors.As(err, &ee) {
			t.Fatalf("helm: %v", err)
		}
		return chartRender{refused: true, stderr: se.String()}
	}
	dec := yaml.NewDecoder(&so)
	var out []any
	for {
		var d any
		if err := dec.Decode(&d); err != nil {
			break
		}
		if d != nil {
			out = append(out, d)
		}
	}
	return chartRender{ingresses: out}
}

func dig(v any, keys ...string) any {
	for _, k := range keys {
		mp, ok := v.(map[string]any)
		if !ok {
			return nil
		}
		v = mp[k]
	}
	return v
}

// chartBackend is the Service the rendered rules give path on host, resolved as ingress-nginx does: without regex mode
// an Exact rule matches that path, a Prefix the path and below it, longest first; in regex mode (an Ingress on the host
// carries use-regex) every rule is a case-insensitive regex tried longest path first, Prefix/Exact unanchored at the end.
func chartBackend(t *testing.T, ingresses []any, host, path string) string {
	t.Helper()
	var rules []chartRule
	regexMode := false
	for _, ing := range ingresses {
		ann, _ := dig(ing, "metadata", "annotations").(map[string]any)
		rs, _ := dig(ing, "spec", "rules").([]any)
		for _, r := range rs {
			if dig(r, "host") != host {
				continue
			}
			regexMode = regexMode || ann["nginx.ingress.kubernetes.io/use-regex"] == "true"
			items, _ := dig(r, "http", "paths").([]any)
			for _, it := range items {
				svc := strings.TrimPrefix(dig(it, "backend", "service", "name").(string), "differential-dev-health-")
				rules = append(rules, chartRule{dig(it, "path").(string), dig(it, "pathType").(string), svc})
			}
		}
	}
	sort.SliceStable(rules, func(i, j int) bool { return len(rules[i].path) > len(rules[j].path) })
	if regexMode {
		for _, r := range rules {
			pattern := r.path
			if r.pathType != "ImplementationSpecific" {
				pattern = "^" + regexp.QuoteMeta(r.path)
			}
			if !strings.HasPrefix(pattern, "^") {
				pattern = "^" + pattern
			}
			if regexp.MustCompile("(?i)" + pattern).MatchString(path) {
				return r.service
			}
		}
		t.Fatalf("no rule for %s on %s", path, host)
	}
	for _, r := range rules {
		if r.pathType == "Exact" && r.path == path {
			return r.service
		}
	}
	for _, r := range rules {
		trimmed := strings.TrimRight(r.path, "/")
		if r.pathType == "Prefix" && (r.path == "/" || path == trimmed || strings.HasPrefix(path, trimmed+"/")) {
			return r.service
		}
	}
	t.Fatalf("no rule for %s on %s", path, host)
	return ""
}

var backendName = map[string]string{goAPI: "go-api", queryAPI: "query-api", python: "api"}

var (
	qaEntry       = m{"path": "/graphql$", "pathType": "ImplementationSpecific", "service": "query-api"}
	goDefaultOnly = l{m{"path": "/", "pathType": "Prefix", "service": "go-api"}}
)

func chartValuesFor(shared l) m {
	return m{
		"ingress": m{
			"enabled": true, "className": "nginx", "pythonAllowList": shared,
			"hosts": l{m{"host": "api.example", "pythonAllowList": true, "paths": goDefaultOnly}},
		},
		"goApi": m{"enabled": true}, "api": m{"enabled": true}, "queryApi": m{"enabled": true},
	}
}

// The one matrix row the Python suite carried: the chart refuses to render it (a Prefix and an Exact Python entry
// beside an anchored query-api entry on one host), so the generator is not asked.
func TestTheRouterGivesEveryPathTheBackendTheChartGivesIt(t *testing.T) {
	shared := l{m{"path": "/ops-status", "pathType": "Prefix"}, m{"path": "/status", "pathType": "Exact"}, qaEntry}
	r := renderChart(t, chartValuesFor(shared))
	if !r.refused {
		t.Fatal("plain-python-only: the matrix says the chart refuses, and it rendered")
	}
	// The refusal must be the ingress template's own fail, not helm failing for another reason.
	if !strings.Contains(r.stderr, "execution error at (dev-health/templates/ingress.yaml:") {
		t.Fatalf("helm failed, but not by the ingress template's refusal:\n%s", r.stderr)
	}
}

// CHAOS-7925 (1): on a host with an anchored entry ingress-nginx matches paths case-insensitively (`~*`): /GRAPHQL
// reaches query-api there, and the router matches the same way.
func TestAnAnchoredEntryMatchesCaseInsensitivelyLikeRegexModeDoes(t *testing.T) {
	cv := chartValuesFor(l{qaEntry})
	rendered := renderChart(t, cv)
	if rendered.refused {
		t.Fatal("the chart refused the anchored query-api entry")
	}
	v := baseValues()
	v["ops"] = m{"ingress": cv["ingress"]}
	routed := emit(t, v)
	for _, path := range []string{"/GRAPHQL", "/Graphql"} {
		if got := chartBackend(t, rendered.ingresses, "api.example", path); got != "query-api" {
			t.Errorf("chart sends %s to %s", path, got)
		}
		if got := backendName[route(t, routed, path)]; got != "query-api" {
			t.Errorf("router sends %s to %s", path, got)
		}
	}
}

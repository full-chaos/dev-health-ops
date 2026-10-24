package main

import (
	"bytes"
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/loadharness"
)

func writeCfg(t *testing.T, url string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "cfg.json")
	doc := `{"base_url":"` + url + `","targets":[{"name":"pdp","method":"POST","path":"/v1/authorize","body":"{}","expect_status":[200]}]}`
	if err := os.WriteFile(p, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func runCLI(t *testing.T, args ...string) (int, string, string) {
	t.Helper()
	var o, e bytes.Buffer
	code := run(context.Background(), args, &o, &e, func(string) string { return "" })
	return code, o.String(), e.String()
}

func TestSLOExitCodeConfigOnly(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	cfg := writeCfg(t, srv.URL)
	common := []string{"--config", cfg, "--rate", "100", "--duration", "300ms", "--concurrency", "4"}

	code, out, _ := runCLI(t, append(common, "--slo", "pdp:p95=5s,p99=5s,err=0")...)
	if code != 0 || !strings.Contains(out, `"slo_ok": true`) {
		t.Fatalf("code=%d out=%s", code, out)
	}
	code, _, errOut := runCLI(t, append(common, "--slo", "pdp:p95=1ns")...)
	if code != 1 || !strings.Contains(errOut, "SLO BREACH pdp p95") {
		t.Fatalf("code=%d err=%s", code, errOut)
	}
}

func TestRefusesProdHost(t *testing.T) {
	code, _, errOut := runCLI(t, "--config", writeCfg(t, "https://app.example.com"), "--duration", "1s")
	if code != 2 || !strings.Contains(errOut, "not allowed") {
		t.Fatalf("code=%d err=%s", code, errOut)
	}
}

func TestUsageErrors(t *testing.T) {
	if code, _, _ := runCLI(t); code != 2 {
		t.Fatal("missing --config must exit 2")
	}
	if code, _, _ := runCLI(t, "--config", writeCfg(t, "http://127.0.0.1:1"), "--profile", "nope"); code != 2 {
		t.Fatal("bad profile must exit 2")
	}
}

func TestExampleConfigParses(t *testing.T) {
	data, err := os.ReadFile("example.config.json")
	if err != nil {
		t.Fatal(err)
	}
	env := func(string) string { return "x" }
	cfg, err := loadharness.ParseConfig(data, env)
	if err != nil || len(cfg.Targets) != 3 {
		t.Fatalf("%v %+v", err, cfg)
	}
}

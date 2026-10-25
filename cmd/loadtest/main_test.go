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
	data, err := os.ReadFile("../../internal/loadharness/testdata/example.config.json")
	if err != nil {
		t.Fatal(err)
	}
	env := func(string) string { return "x" }
	cfg, err := loadharness.ParseConfig(data, env)
	if err != nil || len(cfg.Targets) != 3 {
		t.Fatalf("%v %+v", err, cfg)
	}
}

func writeAbuseCfg(t *testing.T, url string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), "abuse.json")
	doc := `{"base_url":"` + url + `","targets":[{"name":"login","method":"POST","path":"/login","body":"{}","expect_status":[401],"forbid_status":[200],"forbid_5xx":true}]}`
	if err := os.WriteFile(p, []byte(doc), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

func TestAssertionFailureExitsOne(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer srv.Close()
	code, out, errOut := runCLI(t, "--config", writeAbuseCfg(t, srv.URL), "--rate", "50", "--duration", "300ms")
	if code != 1 || !strings.Contains(errOut, "ASSERTION FAILED login forbid_status") || !strings.Contains(out, `"assert_ok": false`) {
		t.Fatalf("code=%d err=%s", code, errOut)
	}
}

func TestFaultWindowFlagsAndRecoveryBound(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }))
	defer srv.Close()
	cfg := writeAbuseCfg(t, srv.URL)
	args := []string{"--config", cfg, "--rate", "20", "--duration", "3s", "--fault-name", "valkey-down",
		"--fault-on", "0s", "--fault-off", "1s", "--fault-sustain", "1"}
	code, out, errOut := runCLI(t, append(args, "--recover-within", "5s")...)
	if code != 0 || !strings.Contains(out, `"name": "valkey-down"`) || !strings.Contains(out, `"recovered": true`) {
		t.Fatalf("code=%d err=%s out=%s", code, errOut, out)
	}
	code, _, errOut = runCLI(t, "--config", cfg, "--rate", "20", "--duration", "1s", "--recover-within", "5s")
	if code != 1 || !strings.Contains(errOut, "ASSERTION FAILED fault recover_within") {
		t.Fatalf("code=%d err=%s", code, errOut)
	}
	if code, _, _ := runCLI(t, "--config", cfg, "--fault-on", "5s", "--fault-off", "1s"); code != 2 {
		t.Fatal("off before on must exit 2")
	}
}

func TestFaultMaxErrMustBeARatio(t *testing.T) {
	cfg := writeAbuseCfg(t, "http://127.0.0.1:1")
	for _, v := range []string{"NaN", "+Inf", "-0.1", "1.1"} {
		code, _, errOut := runCLI(t, "--config", cfg, "--fault-on", "0s", "--fault-off", "1s", "--fault-max-err", v)
		if code != 2 || !strings.Contains(errOut, "--fault-max-err") {
			t.Fatalf("%s: code=%d err=%s", v, code, errOut)
		}
	}
}

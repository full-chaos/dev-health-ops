package main

import (
	"bytes"
	"context"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// stubTransport serves a client's requests from the stub handler in mode, as the venue's pinned hosts would.
type stubTransport struct{ h http.Handler }

func (s stubTransport) RoundTrip(r *http.Request) (*http.Response, error) {
	rec := httptest.NewRecorder()
	r.Host = r.URL.Host
	if r.Body == nil {
		r.Body = http.NoBody
	}
	s.h.ServeHTTP(rec, r)
	return rec.Result(), nil
}

func validateAgainst(t *testing.T, mode string) error {
	t.Helper()
	var buf bytes.Buffer
	client := &http.Client{Transport: stubTransport{handler(fixedLog(&buf), mode)}}
	got, err := providerfoundation.ValidatePagerDutyCredential(context.Background(), client, providerfoundation.PagerDutyRevokeConfig{},
		providerfoundation.PagerDutyCredentialCandidate{AuthMode: "api_token", APIToken: "venue-stub-api-token", Subdomain: "venue-stub", Region: "us"},
		providerfoundation.PagerDutyReadScopeSet())
	if err == nil && got.Subdomain != "venue-stub" {
		t.Fatalf("validated subdomain %q", got.Subdomain)
	}
	if strings.Contains(buf.String(), "venue-stub-api-token") {
		t.Fatal("the token reached the log")
	}
	return err
}

// Seed mode answers only GET api.pagerduty.com/services?limit=1, and that answer passes go-api's own credential
// validation; every other request in seed mode is refused.
func TestSeedModeAnswersOnlyTheSeedProbe(t *testing.T) {
	if err := validateAgainst(t, modeSeed); err != nil {
		t.Fatalf("the seed probe answer does not validate: %v", err)
	}
	var buf bytes.Buffer
	h := handler(fixedLog(&buf), modeSeed)
	for _, c := range []struct{ host, method, target string }{
		{"api.pagerduty.com", "GET", "/services"},
		{"api.pagerduty.com", "GET", "/services?limit=2"},
		{"api.pagerduty.com", "POST", "/services?limit=1"},
		{"identity.pagerduty.com", "GET", "/services?limit=1"},
		{"api.stripe.com", "GET", "/v1/products"},
		{"api.github.com", "GET", "/services?limit=1"},
		{"other.example", "GET", "/services?limit=1"},
	} {
		req := httptest.NewRequest(c.method, "https://"+c.host+c.target, nil)
		req.Host = c.host
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusForbidden {
			t.Errorf("seed mode %s %s%s: %d", c.method, c.host, c.target, rec.Code)
		}
	}
}

// The frozen run-mode stub refuses the seed probe, so the judged runs can never store a credential.
func TestRunModeRefusesTheSeedProbe(t *testing.T) {
	if err := validateAgainst(t, modeRun); err == nil {
		t.Fatal("the run-mode stub validated a PagerDuty credential")
	}
}

// The mode goes with the log name (seed logs hold ".seed.", run logs do not), the status file names the mode, the
// config digest and the start, and seed mode opens no SMTP listener.
func TestModeLogNameAndStatusFile(t *testing.T) {
	dir := t.TempDir()
	certPath, keyPath, _ := testCA(t, dir, 0o600)
	args := func(mode, log, status string) []string {
		return []string{"-ca-cert", certPath, "-ca-key", keyPath, "-mode", mode, "-log", filepath.Join(dir, log), "-status", filepath.Join(dir, status),
			"-https", "127.0.0.1:0", "-smtp", "127.0.0.1:0"}
	}
	quick := func() context.Context {
		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		t.Cleanup(cancel)
		return ctx
	}
	for name, a := range map[string][]string{
		"seed mode, run log":  args(modeSeed, "hits.jsonl", "s1.txt"),
		"run mode, seed log":  args(modeRun, "hits.seed.jsonl", "s2.txt"),
		"unknown mode":        args("replay", "hits3.jsonl", "s3.txt"),
		"no status file flag": {"-ca-cert", certPath, "-ca-key", keyPath, "-log", filepath.Join(dir, "h4.jsonl")},
	} {
		if code := run(quick(), a, io.Discard); code != exitUsage {
			t.Errorf("%s: exit %d", name, code)
		}
	}
	for _, mode := range []string{modeRun, modeSeed} {
		log := "run-hits.jsonl"
		if mode == modeSeed {
			log = "org-e.seed.jsonl"
		}
		if code := run(quick(), args(mode, log, mode+"-status.txt"), io.Discard); code != 0 {
			t.Fatalf("%s mode: exit %d", mode, code)
		}
		raw, err := os.ReadFile(filepath.Join(dir, mode+"-status.txt"))
		if err != nil {
			t.Fatal(err)
		}
		lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
		if len(lines) != 3 || lines[0] != "mode="+mode || lines[1] != "config="+configDigest(mode) || !strings.HasPrefix(lines[2], "started=") {
			t.Fatalf("%s status %q", mode, raw)
		}
		if _, err := time.Parse(time.RFC3339Nano, strings.TrimPrefix(lines[2], "started=")); err != nil {
			t.Fatalf("%s started: %v", mode, err)
		}
	}
	if configDigest(modeRun) == configDigest(modeSeed) || !strings.HasPrefix(configDigest(modeRun), "sha256:") || len(configDigest(modeRun)) != 71 {
		t.Fatalf("digests %s %s", configDigest(modeRun), configDigest(modeSeed))
	}
	if code := run(quick(), args(modeRun, "again.jsonl", modeRun+"-status.txt"), io.Discard); code != exitUsage {
		t.Fatalf("an existing status file: exit %d", code)
	}
	// Seed mode opens no SMTP listener on the address it is given.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int)
	go func() {
		done <- run(ctx, []string{"-ca-cert", certPath, "-ca-key", keyPath, "-mode", modeSeed, "-log", filepath.Join(dir, "b.seed.jsonl"),
			"-status", filepath.Join(dir, "b-status.txt"), "-https", "127.0.0.1:0", "-smtp", addr}, io.Discard)
	}()
	for i := 0; i < 40; i++ {
		if _, err := os.Stat(filepath.Join(dir, "b-status.txt")); err == nil {
			break
		}
		time.Sleep(25 * time.Millisecond)
	}
	if conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond); err == nil {
		conn.Close()
		t.Error("seed mode accepted an SMTP connection")
	}
	cancel()
	<-done
}

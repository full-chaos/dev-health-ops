package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-go/workloadtoken"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

// fakeBootstrap records what the verbs hand the store; err fails every write.
// The failure text quotes the value, as a careless store error could.
type fakeBootstrap struct {
	values []string
	writes int
	err    bool
}

func (f *fakeBootstrap) fail(value string) error {
	return errors.New("pg: duplicate key " + value)
}

func (f *fakeBootstrap) CreateBootstrap(_ context.Context, _, value string, _ []string, _ time.Duration, _ workload.Actor) (workload.BootstrapResult, error) {
	f.writes++
	f.values = append(f.values, value)
	if f.err {
		return workload.BootstrapResult{}, f.fail(value)
	}
	return workload.BootstrapResult{CredentialID: uuid.New(), ExpiresAt: time.Now().Add(time.Hour)}, nil
}

func (f *fakeBootstrap) RotateBootstrap(_ context.Context, _, value string, _, _ time.Duration, _ workload.Actor) (workload.BootstrapResult, error) {
	f.writes++
	f.values = append(f.values, value)
	if f.err {
		return workload.BootstrapResult{}, f.fail(value)
	}
	return workload.BootstrapResult{CredentialID: uuid.New(), ParentID: uuid.New(), ExpiresAt: time.Now().Add(time.Hour), ParentExpiresAt: time.Now()}, nil
}

func (f *fakeBootstrap) RevokeBootstrap(context.Context, uuid.UUID, string, workload.Actor) error {
	f.writes++
	return nil
}

func (f *fakeBootstrap) DisableBootstrap(context.Context, string, workload.Actor) (workload.BootstrapResult, error) {
	f.writes++
	return workload.BootstrapResult{Revoked: 2}, nil
}

func (f *fakeBootstrap) LiveBootstrap(context.Context, string) ([]workload.BootstrapCredential, error) {
	return []workload.BootstrapCredential{{ID: uuid.New()}}, nil
}

func (f *fakeBootstrap) BootstrapStatus(context.Context, uuid.UUID) (string, error) {
	return "active", nil
}

func mustParse(t *testing.T, args ...string) workloadsArgs {
	t.Helper()
	parsed, err := parseWorkloadsArgs(args, io.Discard)
	if err != nil {
		t.Fatalf("%v refused: %v", args, err)
	}
	return parsed
}

func TestParseBootstrapArgs(t *testing.T) {
	id := uuid.NewString()
	ok := map[string][]string{
		"create":              {"bootstrap-create", "-workload", "api", "-out", "/x/v"},
		"create audiences":    {"bootstrap-create", "-workload", "api", "-out", "v", "-audience", "dev-health-api", "-audience", "auth-service", "-apply"},
		"create 90 d":         {"bootstrap-create", "-workload", "api", "-out", "v", "-expires-in", "2160h"},
		"rotate overlap 0":    {"bootstrap-rotate", "-workload", "api", "-out", "v", "-overlap", "0s"},
		"rotate overlap 1h":   {"bootstrap-rotate", "-workload", "api", "-out", "v", "-overlap", "1h", "-expires-in", "1s"},
		"revoke compromised":  {"bootstrap-revoke", "-credential", id, "-reason", "compromised"},
		"revoke owner":        {"bootstrap-revoke", "-credential", id, "-reason", "owner_request", "-apply"},
		"disable":             {"bootstrap-disable", "-workload", "api", "-apply"},
		"seed is still plain": {"seed"},
	}
	for name, args := range ok {
		if _, err := parseWorkloadsArgs(args, io.Discard); err != nil {
			t.Errorf("%s refused: %v", name, err)
		}
	}
	if p := mustParse(t, "bootstrap-create", "-workload", "api", "-out", "v"); p.expiresIn != 30*24*time.Hour {
		t.Fatalf("default -expires-in = %s, want 720h", p.expiresIn)
	}
	bad := map[string][]string{
		"create without -out":        {"bootstrap-create", "-workload", "api"},
		"create to stdout":           {"bootstrap-create", "-workload", "api", "-out", "-"},
		"create no expiry":           {"bootstrap-create", "-workload", "api", "-out", "v", "-expires-in", "0s"},
		"create 90 d + 1 ns":         {"bootstrap-create", "-workload", "api", "-out", "v", "-expires-in", "2160h0m0.000000001s"},
		"create negative expiry":     {"bootstrap-create", "-workload", "api", "-out", "v", "-expires-in", "-1h"},
		"create with -overlap":       {"bootstrap-create", "-workload", "api", "-out", "v", "-overlap", "1m"},
		"create bad audience":        {"bootstrap-create", "-workload", "api", "-out", "v", "-audience", "Bad"},
		"create repeated audience":   {"bootstrap-create", "-workload", "api", "-out", "v", "-audience", "a", "-audience", "a"},
		"create bad name":            {"bootstrap-create", "-workload", "", "-out", "v"},
		"create takes no value flag": {"bootstrap-create", "-workload", "api", "-out", "v", "-value", "fcwlb_x"},
		"rotate without -overlap":    {"bootstrap-rotate", "-workload", "api", "-out", "v"},
		"rotate overlap 1h + 1s":     {"bootstrap-rotate", "-workload", "api", "-out", "v", "-overlap", "1h0m1s"},
		"rotate overlap -1s":         {"bootstrap-rotate", "-workload", "api", "-out", "v", "-overlap", "-1s"},
		"rotate with -audience":      {"bootstrap-rotate", "-workload", "api", "-out", "v", "-overlap", "0s", "-audience", "a"},
		"revoke no reason":           {"bootstrap-revoke", "-credential", id},
		"revoke bad reason":          {"bootstrap-revoke", "-credential", id, "-reason", "expired"},
		"revoke bad id":              {"bootstrap-revoke", "-credential", "x", "-reason", "compromised"},
		"revoke upper-case id":       {"bootstrap-revoke", "-credential", strings.ToUpper(id), "-reason", "compromised"},
		"revoke with -workload":      {"bootstrap-revoke", "-credential", id, "-reason", "compromised", "-workload", "api"},
		"disable with -out":          {"bootstrap-disable", "-workload", "api", "-out", "v"},
		"disable without a name":     {"bootstrap-disable"},
		"seed with -out":             {"seed", "-out", "v"},
		"bind with -reason":          {"bind", "-workload", "api", "-environment", "e", "-trust-domain", "t", "-issuer", "https://i", "-namespace", "n", "-service-account", "s", "-reason", "compromised"},
	}
	for name, args := range bad {
		if _, err := parseWorkloadsArgs(args, io.Discard); err == nil {
			t.Errorf("%s accepted", name)
		}
	}
}

func TestBootstrapArgumentErrorsComeBeforeTheEnvironment(t *testing.T) {
	lookup := func(string) (string, bool) {
		t.Fatal("the environment was read for a bad command line")
		return "", false
	}
	for _, args := range [][]string{
		{"bootstrap-create", "-workload", "api", "-out", "v", "-expires-in", "2161h"},
		{"bootstrap-rotate", "-workload", "api", "-out", "v", "-overlap", "2h"},
		{"bootstrap-revoke", "-credential", "x"},
	} {
		if code := executeWorkloads(t.Context(), args, lookup, io.Discard, io.Discard); code != 2 {
			t.Fatalf("%v exit = %d, want 2", args, code)
		}
	}
}

func TestWriteSecretFileIsNewAndMode0600WhateverTheUmask(t *testing.T) {
	old := syscall.Umask(0)
	defer syscall.Umask(old)
	dir := t.TempDir()
	path := filepath.Join(dir, "value")
	if err := writeSecretFile(path, "fcwlb_value"); err != nil {
		t.Fatal(err)
	}
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	if !info.Mode().IsRegular() || info.Mode().Perm() != 0o600 {
		t.Fatalf("mode = %s, want a regular 0600 file", info.Mode())
	}
	if b, _ := os.ReadFile(path); string(b) != "fcwlb_value" {
		t.Fatalf("content = %q", b)
	}
	if err := writeSecretFile(path, "fcwlb_other"); !errors.Is(err, errOutExists) {
		t.Fatalf("an existing path = %v, want errOutExists", err)
	}
	if b, _ := os.ReadFile(path); string(b) != "fcwlb_value" {
		t.Fatal("an existing file was overwritten")
	}
	target := filepath.Join(dir, "elsewhere")
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if err := writeSecretFile(link, "fcwlb_value"); !errors.Is(err, errOutExists) {
		t.Fatalf("a dangling symlink = %v, want errOutExists", err)
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the symlink was followed")
	}
}

// noLeak fails if any text holds the value or its digest.
func noLeak(t *testing.T, value string, texts ...string) {
	t.Helper()
	d := workload.BootstrapDigest(value)
	for _, text := range texts {
		for _, leak := range []string{value, strings.TrimPrefix(value, workload.BootstrapPrefix), hex.EncodeToString(d[:])} {
			if strings.Contains(text, leak) {
				t.Fatalf("output holds the value or its digest:\n%s", text)
			}
		}
	}
}

func TestBootstrapCreateWritesTheValueOnlyToTheOutFile(t *testing.T) {
	for _, verb := range []string{"bootstrap-create", "bootstrap-rotate"} {
		store := &fakeBootstrap{}
		out := filepath.Join(t.TempDir(), "value")
		args := []string{verb, "-workload", "api", "-out", out, "-apply"}
		if verb == "bootstrap-rotate" {
			args = append(args, "-overlap", "5m")
		}
		var stdout, stderr, logs bytes.Buffer
		prev := slog.Default()
		slog.SetDefault(slog.New(slog.NewTextHandler(&logs, nil)))
		code := runBootstrap(t.Context(), store, mustParse(t, args...), &stdout, &stderr)
		slog.SetDefault(prev)
		if code != 0 || len(store.values) != 1 {
			t.Fatalf("%s exit = %d, values = %d, stderr %s", verb, code, len(store.values), stderr.String())
		}
		b, err := os.ReadFile(out)
		if err != nil || string(b) != store.values[0] || !workload.CanonicalBootstrap(string(b)) {
			t.Fatalf("%s: the out-file does not hold the registered canonical value", verb)
		}
		noLeak(t, store.values[0], stdout.String(), stderr.String(), logs.String(), strings.Join(args, " "))
	}
}

func TestBootstrapCommitFailureRemovesTheOutFileAndLeaksNothing(t *testing.T) {
	store := &fakeBootstrap{err: true}
	out := filepath.Join(t.TempDir(), "value")
	var stdout, stderr bytes.Buffer
	if code := runBootstrap(t.Context(), store, mustParse(t, "bootstrap-create", "-workload", "api", "-out", out, "-apply"), &stdout, &stderr); code != 1 {
		t.Fatalf("exit = %d", code)
	}
	if _, err := os.Lstat(out); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("the out-file of a failed commit was kept")
	}
	noLeak(t, store.values[0], stdout.String(), stderr.String())
}

func TestBootstrapExistingOutFileRegistersNothing(t *testing.T) {
	store := &fakeBootstrap{}
	out := filepath.Join(t.TempDir(), "value")
	if err := os.WriteFile(out, []byte("keep"), 0o600); err != nil {
		t.Fatal(err)
	}
	if code := runBootstrap(t.Context(), store, mustParse(t, "bootstrap-create", "-workload", "api", "-out", out, "-apply"), io.Discard, io.Discard); code != 1 {
		t.Fatalf("exit = %d", code)
	}
	if store.writes != 0 {
		t.Fatal("a value was registered although its out-file could not be written")
	}
	if b, _ := os.ReadFile(out); string(b) != "keep" {
		t.Fatal("the existing file was changed")
	}
}

func TestBootstrapDryRunWritesNothing(t *testing.T) {
	dir := t.TempDir()
	id := uuid.NewString()
	for _, args := range [][]string{
		{"bootstrap-create", "-workload", "api", "-out", filepath.Join(dir, "c")},
		{"bootstrap-rotate", "-workload", "api", "-out", filepath.Join(dir, "r"), "-overlap", "1m"},
		{"bootstrap-revoke", "-credential", id, "-reason", "compromised"},
		{"bootstrap-disable", "-workload", "api"},
	} {
		store := &fakeBootstrap{}
		var stdout bytes.Buffer
		if code := runBootstrap(t.Context(), store, mustParse(t, args...), &stdout, io.Discard); code != 0 {
			t.Fatalf("%v exit = %d", args, code)
		}
		if store.writes != 0 || !strings.Contains(stdout.String(), "no change written") {
			t.Fatalf("%v: dry run wrote (%d) or did not say so: %s", args, store.writes, stdout.String())
		}
	}
	if entries, _ := os.ReadDir(dir); len(entries) != 0 {
		t.Fatalf("a dry run wrote %d file(s)", len(entries))
	}
}

func TestBootstrapDisableSaysIssuedTokensLiveAtMost300s(t *testing.T) {
	var stdout bytes.Buffer
	if code := runBootstrap(t.Context(), &fakeBootstrap{}, mustParse(t, "bootstrap-disable", "-workload", "api", "-apply"), &stdout, io.Discard); code != 0 {
		t.Fatal(code)
	}
	if !strings.Contains(stdout.String(), "revoked=2") || !strings.Contains(stdout.String(), "at most 300 s") {
		t.Fatalf("stdout = %q", stdout.String())
	}
	if !strings.Contains(workloadsUsage, "at most 300 s") {
		t.Fatal("the usage text does not state the 300 s token tail")
	}
}

// subjectCapture is an exchange transport that records the subject token and
// refuses; the client reaches it only with a subject it accepted.
type subjectCapture struct{ got []string }

func (c *subjectCapture) RoundTrip(r *http.Request) (*http.Response, error) {
	if err := r.ParseForm(); err != nil {
		return nil, err
	}
	c.got = append(c.got, r.PostForm.Get("subject_token"))
	return &http.Response{StatusCode: http.StatusServiceUnavailable, Body: io.NopCloser(strings.NewReader("")), Header: http.Header{}, Request: r}, nil
}

func clientSends(t *testing.T, path string) (string, error) {
	t.Helper()
	capture := &subjectCapture{}
	c, err := workloadtoken.New(workloadtoken.Config{
		ExchangeURL: "https://auth.example", Issuer: "https://auth.example",
		Subject: workloadtoken.BootstrapCredentialFile(path), Transport: capture,
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	_, err = c.Token(t.Context(), "dev-health-api")
	if len(capture.got) == 0 {
		return "", err
	}
	return capture.got[0], err
}

// TestEveryMintedValuePassesTheDevHealthGoClient runs each minted value, from
// its real 0600 out-file, through the v0.32.0 client's canonical check: the
// client sends it to the exchange unchanged. The control (stray bits in the
// last character) is refused by the client before any request.
func TestEveryMintedValuePassesTheDevHealthGoClient(t *testing.T) {
	dir := t.TempDir()
	store := &fakeBootstrap{}
	const n = 1024
	for i := range n {
		out := filepath.Join(dir, "v"+uuid.NewString())
		if code := runBootstrap(t.Context(), store, mustParse(t, "bootstrap-create", "-workload", "api", "-out", out, "-apply"), io.Discard, io.Discard); code != 0 {
			t.Fatalf("mint %d exit = %d", i, code)
		}
		sent, err := clientSends(t, out)
		if sent != store.values[i] {
			t.Fatalf("value %d: the client did not send the minted value (err %v)", i, err)
		}
	}
	v := store.values[0]
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_"
	odd := v[:len(v)-1] + string(alphabet[(strings.IndexByte(alphabet, v[len(v)-1])&^0xF)|0x1])
	if !workload.ValidBootstrapShape(odd) || workload.CanonicalBootstrap(odd) {
		t.Fatal("the control is not a shape-valid non-canonical value")
	}
	path := filepath.Join(dir, "odd")
	if err := writeSecretFile(path, odd); err != nil {
		t.Fatal(err)
	}
	if sent, err := clientSends(t, path); sent != "" || !errors.Is(err, workloadtoken.ErrSubjectUnreadable) {
		t.Fatalf("the client sent a non-canonical value (%q, %v)", sent, err)
	}
}

package goapiproof

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func buildInfoServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func TestFetchBuildIdentityReturnsTheRunningCommit(t *testing.T) {
	server := buildInfoServer(t, http.StatusOK, `{"commit":"b18e56fa79cfe20ce0f75df148144b832d92be36","modified":false}`)
	commit, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
		map[string]string{"Authorization": "Bearer x"})
	if err != nil {
		t.Fatalf("FetchBuildIdentity: %v", err)
	}
	if commit != "b18e56fa79cfe20ce0f75df148144b832d92be36" {
		t.Fatalf("got %q", commit)
	}
}

// Every way the process can fail to identify itself is a REFUSAL. None of
// them may fall back to an operator-supplied name.
func TestFetchBuildIdentityRefusesAnUnidentifiableBuild(t *testing.T) {
	for name, testCase := range map[string]struct {
		status int
		body   string
	}{
		"empty commit":    {http.StatusOK, `{"commit":"","modified":false}`},
		"unknown commit":  {http.StatusOK, `{"commit":"unknown","modified":false}`},
		"modified tree":   {http.StatusOK, `{"commit":"b18e56fa7","modified":true}`},
		"route not there": {http.StatusNotFound, `not found`},
	} {
		t.Run(name, func(t *testing.T) {
			server := buildInfoServer(t, testCase.status, testCase.body)
			_, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
				map[string]string{"Authorization": "Bearer x"})
			if !errors.Is(err, ErrNoBuildIdentity) {
				t.Fatalf("expected ErrNoBuildIdentity, got %v", err)
			}
		})
	}
}

func TestFetchBuildIdentitySendsTheEnvelope(t *testing.T) {
	server := buildInfoServer(t, http.StatusOK, `{"commit":"abc","modified":false}`)
	// No Authorization header: the stub answers 401, and that must be a
	// distinct failure from "cannot identify its build" -- one is a
	// credential problem, the other is a deployment problem.
	_, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL, nil)
	if err == nil {
		t.Fatal("an unauthenticated /buildinfo read must fail")
	}
	if errors.Is(err, ErrNoBuildIdentity) {
		t.Fatalf("a 401 is not an unidentifiable build: %v", err)
	}
}

// --candidate-build can FAIL a run; it can never supply the value.
func TestVerifyCandidateBuildTreatsTheFlagAsACrossCheck(t *testing.T) {
	if err := VerifyCandidateBuild("abc", "abc", nil); err != nil {
		t.Fatalf("a matching cross-check must pass: %v", err)
	}
	err := VerifyCandidateBuild("abc", "def", nil)
	if err == nil {
		t.Fatal("a mismatched --candidate-build must fail the run")
	}
	if !strings.Contains(err.Error(), "cross-check, never the source") {
		t.Fatalf("the failure must say what the flag is for, got %v", err)
	}
}

// Routing rows pointing at a build the process is not are describing a
// deployment that does not exist -- the same silent-mismatch class as the
// stale schema digest in CHAOS-5416.
func TestVerifyCandidateBuildRejectsDisagreeingRoutingRows(t *testing.T) {
	err := VerifyCandidateBuild("abc", "", map[string]RoutingRow{
		"featureFlags": {Mode: "canary", CandidateBuild: "abc"},
		"hotspots":     {Mode: "canary", CandidateBuild: "stale-sha"},
	})
	if err == nil {
		t.Fatal("a row pointing at another build must fail the run")
	}
	if !strings.Contains(err.Error(), "hotspots points at stale-sha") {
		t.Fatalf("the failure must NAME the disagreeing row, got %v", err)
	}
	if strings.Contains(err.Error(), "featureFlags") {
		t.Fatalf("an agreeing row must not be reported, got %v", err)
	}
}

func TestFetchRegistryRefusesAnEmptyRegistration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"schema_digest":"sha256:abc","operations":[]}`))
	}))
	t.Cleanup(server.Close)
	if _, err := FetchRegistry(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("a process registering no operations has nothing to prove")
	}
}

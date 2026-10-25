package loadharness

import (
	"context"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func loadProfile(t *testing.T, name string) Config {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("testdata", "abuse", name+".json"))
	if err != nil {
		t.Fatal(err)
	}
	cfg, err := ParseConfig(data, func(string) string { return "x" })
	if err != nil {
		t.Fatalf("%s: %v", name, err)
	}
	return cfg
}

func runProfile(t *testing.T, name string, h http.HandlerFunc, rate float64, d time.Duration) Report {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	cfg := loadProfile(t, name)
	cfg.BaseURL = srv.URL
	rep, err := Run(context.Background(), cfg, Options{
		Profile: Profile{Name: name, Rate: rate, Duration: d, Concurrency: 8},
		Getenv:  func(string) string { return "x" },
	})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func TestAbuseProfilesParseAndCarryRules(t *testing.T) {
	files, _ := filepath.Glob(filepath.Join("testdata", "abuse", "*.json"))
	want := []string{"burst-limit", "fault-mix", "forged-credentials", "login-badcreds", "oversize-body", "refresh-replay", "refresh-storm"}
	var got []string
	for _, f := range files {
		got = append(got, strings.TrimSuffix(filepath.Base(f), ".json"))
	}
	slices.Sort(got)
	if !slices.Equal(got, want) {
		t.Fatalf("profiles %v, want %v", got, want)
	}
	for _, name := range want {
		cfg := loadProfile(t, name)
		if err := CheckHost(cfg.BaseURL, nil); err != nil {
			t.Fatalf("%s: default base url is not a local host: %v", name, err)
		}
		raw, _ := os.ReadFile(filepath.Join("testdata", "abuse", name+".json"))
		if strings.Contains(string(raw), "eyJ") {
			t.Fatalf("%s holds a JWT-looking literal", name)
		}
		for _, tg := range cfg.Targets {
			switch name {
			case "login-badcreds", "burst-limit", "oversize-body", "forged-credentials":
				if !slices.Contains(tg.ForbidStatus, 200) || slices.Contains(tg.ExpectStatus, 200) || !tg.Forbid5xx {
					t.Fatalf("%s/%s: a refused credential must forbid 200 and 5xx: %+v", name, tg.Name, tg)
				}
			case "refresh-replay":
				if !tg.Forbid5xx || !slices.Contains(tg.RequireStatus, 401) || !slices.Contains(tg.ExpectStatus, 200) {
					t.Fatalf("%s/%s: %+v", name, tg.Name, tg)
				}
			case "refresh-storm":
				if tg.Path != "/v1/token/refresh" || !strings.Contains(tg.Body, "${POOL:") || !tg.Forbid5xx {
					t.Fatalf("%s/%s: %+v", name, tg.Name, tg)
				}
			case "fault-mix":
				switch tg.Name {
				case "issue", "refresh":
					if !slices.Contains(tg.ExpectStatus, 503) || slices.Contains(tg.ExpectStatus, 500) || !slices.Contains(tg.ForbidStatus, 500) {
						t.Fatalf("fault-mix/%s must fail closed with 503 and forbid 500: %+v", tg.Name, tg)
					}
				case "login-badcreds":
					if !slices.Contains(tg.ForbidStatus, 200) {
						t.Fatalf("fault-mix/login-badcreds must forbid 200: %+v", tg)
					}
				}
			}
		}
	}
}

func TestBurstProfileSeesTheLimiterOnset(t *testing.T) {
	var n atomic.Int64
	rep := runProfile(t, "burst-limit", func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) > 10 {
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(401)
	}, 100, 600*time.Millisecond)
	if !rep.AssertOK {
		t.Fatalf("%+v", rep.Assertions)
	}
	if _, ok := rep.Paths[0].FirstSeenMs["429"]; !ok {
		t.Fatalf("no 429 onset recorded: %+v", rep.Paths[0])
	}
	rep = runProfile(t, "burst-limit", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }, 100, 300*time.Millisecond)
	if rep.AssertOK {
		t.Fatal("a limiter that never answered 429 passed the burst profile")
	}
}

func TestOversizeProfileSendsAnOverCapBody(t *testing.T) {
	var size atomic.Int64
	rep := runProfile(t, "oversize-body", func(w http.ResponseWriter, r *http.Request) {
		size.Store(r.ContentLength)
		w.WriteHeader(413)
	}, 20, 200*time.Millisecond)
	if size.Load() < 2<<20 {
		t.Fatalf("body of %d bytes is not over 2 MiB", size.Load())
	}
	if !rep.AssertOK {
		t.Fatalf("%+v", rep.Assertions)
	}
	rep = runProfile(t, "oversize-body", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }, 20, 200*time.Millisecond)
	if rep.AssertOK {
		t.Fatal("a 500 on an oversize body passed")
	}
}

func TestForgedCredentialProfileHardAsserts(t *testing.T) {
	rep := runProfile(t, "forged-credentials", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }, 60, 300*time.Millisecond)
	if !rep.AssertOK {
		t.Fatalf("%+v", rep.Assertions)
	}
	rep = runProfile(t, "forged-credentials", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "Bearer svc_loadtest_forged" {
			w.WriteHeader(200)
			return
		}
		w.WriteHeader(401)
	}, 60, 300*time.Millisecond)
	if rep.AssertOK {
		t.Fatal("a forged wrong-class credential accepted with 200 passed")
	}
}

func TestFaultMixHardAsserts(t *testing.T) {
	serve := func(login, issue int) http.HandlerFunc {
		return func(w http.ResponseWriter, r *http.Request) {
			switch r.URL.Path {
			case "/api/v1/auth/login":
				w.WriteHeader(login)
			case "/internal/v1/sessions":
				w.WriteHeader(issue)
			case "/v1/token/refresh":
				w.WriteHeader(401)
			default:
				w.WriteHeader(200)
			}
		}
	}
	if rep := runProfile(t, "fault-mix", serve(401, 503), 80, 500*time.Millisecond); !rep.AssertOK {
		t.Fatalf("a fail-closed 503 on issue failed the run: %+v", rep.Assertions)
	}
	if rep := runProfile(t, "fault-mix", serve(200, 200), 80, 500*time.Millisecond); rep.AssertOK {
		t.Fatal("200 for bad credentials passed under faults")
	}
	if rep := runProfile(t, "fault-mix", serve(401, 500), 80, 500*time.Millisecond); rep.AssertOK {
		t.Fatal("a 500 on issue passed (must fail closed with 503)")
	}
}

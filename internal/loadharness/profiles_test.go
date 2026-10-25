package loadharness

import (
	"context"
	"math"
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

func TestProfilesCarryTheReviewedRules(t *testing.T) {
	for _, name := range []string{"login-badcreds", "burst-limit", "oversize-body", "forged-credentials", "refresh-replay", "refresh-storm"} {
		for _, tg := range loadProfile(t, name).Targets {
			if tg.MaxErrorRate == nil || *tg.MaxErrorRate != 0 {
				t.Fatalf("%s/%s: an abuse profile needs max_error_rate 0", name, tg.Name)
			}
		}
	}
	for _, tg := range loadProfile(t, "fault-mix").Targets {
		if tg.MaxErrorRate != nil {
			t.Fatalf("fault-mix/%s: transport errors are expected inside a fault window", tg.Name)
		}
	}
	login := loadProfile(t, "login-badcreds").Targets[0]
	if !slices.Contains(login.RequireStatus, 401) || !slices.Contains(login.RequireStatus, 429) {
		t.Fatalf("login-badcreds must require 401 and 429: %+v", login.RequireStatus)
	}
	replay := loadProfile(t, "refresh-replay").Targets[0]
	if !slices.Contains(replay.RequireStatus, 200) || !slices.Contains(replay.RequireStatus, 401) || replay.MaxStatusCount["200"] != 1 {
		t.Fatalf("refresh-replay must require 200 and 401 and allow one 200: %+v %+v", replay.RequireStatus, replay.MaxStatusCount)
	}
}

func TestLoginBadCredsNeedsTheAddressLimit(t *testing.T) {
	rep := runProfile(t, "login-badcreds", func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }, 50, 300*time.Millisecond)
	if rep.AssertOK {
		t.Fatalf("a run that never reached the address limit passed: %+v", rep.Paths[0].Statuses)
	}
	var n atomic.Int64
	rep = runProfile(t, "login-badcreds", func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) > 20 {
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(401)
	}, 100, 500*time.Millisecond)
	if !rep.AssertOK {
		t.Fatalf("%+v", rep.Assertions)
	}
}

func TestRefreshReplayNeedsExactlyOneSuccess(t *testing.T) {
	only401 := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) }
	if rep := runProfile(t, "refresh-replay", only401, 50, 300*time.Millisecond); rep.AssertOK {
		t.Fatal("a replay run with no 200 at all passed")
	}
	every200 := func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }
	if rep := runProfile(t, "refresh-replay", every200, 50, 300*time.Millisecond); rep.AssertOK {
		t.Fatal("a replayed credential accepted every time passed")
	}
	var n atomic.Int64
	once := func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) == 1 {
			w.WriteHeader(200)
			return
		}
		w.WriteHeader(401)
	}
	if rep := runProfile(t, "refresh-replay", once, 50, 300*time.Millisecond); !rep.AssertOK {
		t.Fatalf("one 200 then 401 failed: %+v", rep.Assertions)
	}
}

func TestMaxErrorRateAndStatusCountAreValidated(t *testing.T) {
	env := func(string) string { return "x" }
	for _, bad := range []string{
		`"max_error_rate": 1.5`, `"max_error_rate": -0.1`, `"max_error_rate": "NaN"`,
		`"max_status_count": {"99": 1}`, `"max_status_count": {"600": 1}`, `"max_status_count": {"abc": 1}`, `"max_status_count": {"200": -1}`,
	} {
		doc := `{"targets":[{"name":"a","path":"/",` + bad + `}]}`
		if _, err := ParseConfig([]byte(doc), env); err == nil {
			t.Fatalf("%s accepted", bad)
		}
	}
	for _, v := range []float64{math.NaN(), math.Inf(1), math.Inf(-1)} {
		if err := (Target{MaxErrorRate: &v}).validateRules(); err == nil {
			t.Fatalf("max_error_rate %v accepted", v)
		}
	}
	ok := `{"targets":[{"name":"a","path":"/","max_error_rate":0,"max_status_count":{"200":1,"599":0}}]}`
	if _, err := ParseConfig([]byte(ok), env); err != nil {
		t.Fatal(err)
	}
}

func TestMaxErrorRateAssertionFailsOn404(t *testing.T) {
	rep := runOne(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(404) },
		Target{ExpectStatus: []int{200}, MaxErrorRate: ptr(0.0)}, 50, 200*time.Millisecond)
	if rep.AssertOK || rep.Assertions[0].Name != "max_error_rate" {
		t.Fatalf("%+v", rep.Assertions)
	}
}

func TestRequireDegrade(t *testing.T) {
	healthy := Report{AssertOK: true, Fault: &FaultResult{Recovered: true}}
	if ApplyRequireDegrade(healthy).AssertOK {
		t.Fatal("a fault window that never degraded the run passed")
	}
	if ApplyRequireDegrade(Report{AssertOK: true}).AssertOK {
		t.Fatal("no fault window passed")
	}
	got := ApplyRequireDegrade(Report{AssertOK: true, Fault: &FaultResult{TimeToDegradeMs: ptr(int64(1200))}})
	if !got.AssertOK || got.Assertions[0].Name != "require_degrade" {
		t.Fatalf("%+v", got)
	}
}

package loadharness

import (
	"context"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRunCountsAndErrors(t *testing.T) {
	var hits, bad atomic.Int64
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := hits.Add(1)
		if r.URL.Path == "/fail" {
			bad.Add(1)
			w.WriteHeader(500)
			return
		}
		_ = n
		w.WriteHeader(200)
	}))
	defer srv.Close()
	cfg := Config{BaseURL: srv.URL, Targets: []Target{
		{Name: "ok", Method: "GET", Path: "/ok", Weight: 1, ExpectStatus: []int{200}},
		{Name: "fail", Method: "GET", Path: "/fail", Weight: 1, ExpectStatus: []int{200}},
	}}
	rep, err := Run(context.Background(), cfg, Options{
		Profile: Profile{Name: "smoke", Rate: 200, Duration: 500 * time.Millisecond, Concurrency: 8},
		Getenv:  func(string) string { return "" },
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rep.Paths) != 2 {
		t.Fatalf("%+v", rep.Paths)
	}
	var total int
	for _, p := range rep.Paths {
		total += p.Count + p.Dropped
		switch p.Name {
		case "ok":
			if p.Errors != 0 || p.ErrorRate != 0 {
				t.Fatalf("ok path errored: %+v", p)
			}
		case "fail":
			if p.Count == 0 || p.Errors != p.Count {
				t.Fatalf("fail path: %+v", p)
			}
		}
	}
	if total != 100 {
		t.Fatalf("scheduled %d want 100", total)
	}
	if int(hits.Load()) != total {
		t.Fatalf("server saw %d, report %d", hits.Load(), total)
	}
}

func TestRunMeasuresFromScheduledTime(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(50 * time.Millisecond)
	}))
	defer srv.Close()
	cfg := Config{BaseURL: srv.URL, Targets: []Target{{Name: "slow", Method: "GET", Path: "/", Weight: 1, ExpectStatus: []int{200}}}}
	rep, err := Run(context.Background(), cfg, Options{
		Profile: Profile{Name: "q", Rate: 100, Duration: 400 * time.Millisecond, Concurrency: 1},
		Getenv:  func(string) string { return "" },
	})
	if err != nil {
		t.Fatal(err)
	}
	p := rep.Paths[0]
	if p.P99Ms < 150 {
		t.Fatalf("queueing delay not counted (open loop): p99=%vms %+v", p.P99Ms, p)
	}
}

func TestRunNeverLeaksSecrets(t *testing.T) {
	const secret = "s3cr3t-token-value"
	var gotAuth, gotBody atomic.Value
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth.Store(r.Header.Get("Authorization"))
		b, _ := io.ReadAll(r.Body)
		gotBody.Store(string(b))
		_, _ = w.Write([]byte(secret))
	}))
	defer srv.Close()
	env := map[string]string{"TOK": secret, "POOL": "a1,a2"}
	getenv := func(k string) string { return env[k] }
	data := `{"base_url":"` + srv.URL + `","targets":[{"name":"issue","method":"POST","path":"/i?n=${SEQ}",
	 "header_env":{"Authorization":{"env":"TOK","prefix":"Bearer "}},
	 "body":"{\"c\":\"${POOL:POOL}\",\"t\":\"${ENV:TOK}\"}"}]}`
	cfg, err := ParseConfig([]byte(data), getenv)
	if err != nil {
		t.Fatal(err)
	}
	rep, err := Run(context.Background(), cfg, Options{
		Profile: Profile{Name: "s", Rate: 50, Duration: 200 * time.Millisecond, Concurrency: 2},
		Getenv:  getenv,
	})
	if err != nil {
		t.Fatal(err)
	}
	if gotAuth.Load() != "Bearer "+secret || !strings.Contains(gotBody.Load().(string), secret) {
		t.Fatalf("secret was not sent: %v %v", gotAuth.Load(), gotBody.Load())
	}
	var sb strings.Builder
	if err := WriteReport(&sb, rep); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(sb.String(), secret) {
		t.Fatal("secret in report")
	}
}

func TestRunRefusesDisallowedHost(t *testing.T) {
	cfg := Config{BaseURL: "https://app.example.com", Targets: []Target{{Name: "a", Method: "GET", Path: "/", Weight: 1, ExpectStatus: []int{200}}}}
	_, err := Run(context.Background(), cfg, Options{Profile: Profile{Rate: 1, Duration: time.Second, Concurrency: 1}, Getenv: func(string) string { return "" }})
	if err == nil {
		t.Fatal("prod-like host accepted")
	}
}

func TestRunDoesNotFollowRedirects(t *testing.T) {
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusFound)
	}))
	defer srv.Close()
	cfg := Config{BaseURL: srv.URL, Targets: []Target{{Name: "r", Method: "GET", Path: "/", Weight: 1, ExpectStatus: []int{200}}}}
	rep, err := Run(context.Background(), cfg, Options{Profile: Profile{Rate: 20, Duration: 200 * time.Millisecond, Concurrency: 2}, Getenv: func(string) string { return "" }})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Paths[0].ErrorRate != 1 {
		t.Fatalf("redirect counted as success: %+v", rep.Paths[0])
	}
}

func TestRunTruncatedBodyIsAFailure(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Length", "100")
		_, _ = w.Write([]byte("short"))
	}))
	defer srv.Close()
	cfg := Config{BaseURL: srv.URL, Targets: []Target{{Name: "t", Method: "GET", Path: "/", Weight: 1, ExpectStatus: []int{200}}}}
	rep, err := Run(context.Background(), cfg, Options{Profile: Profile{Rate: 20, Duration: 200 * time.Millisecond, Concurrency: 2}, Getenv: func(string) string { return "" }})
	if err != nil {
		t.Fatal(err)
	}
	if rep.Paths[0].ErrorRate != 1 {
		t.Fatalf("truncated body counted as success: %+v", rep.Paths[0])
	}
}

func TestRunRefusesAnEmptySchedule(t *testing.T) {
	cfg := Config{BaseURL: "http://127.0.0.1:1", Targets: []Target{{Name: "t", Method: "GET", Path: "/", Weight: 1, ExpectStatus: []int{200}}}}
	_, err := Run(context.Background(), cfg, Options{Profile: Profile{Rate: 5, Duration: 100 * time.Millisecond, Concurrency: 1}, Getenv: func(string) string { return "" }})
	if err == nil {
		t.Fatal("a run that schedules no request was accepted")
	}
}

func TestRunSuppliedClientStillDoesNotFollowRedirects(t *testing.T) {
	var otherHits atomic.Int64
	other := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { otherHits.Add(1) }))
	defer other.Close()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.Redirect(w, r, other.URL, http.StatusFound)
	}))
	defer srv.Close()
	cfg := Config{BaseURL: srv.URL, Targets: []Target{{Name: "r", Method: "GET", Path: "/", Weight: 1, ExpectStatus: []int{200}}}}
	rep, err := Run(context.Background(), cfg, Options{
		Profile: Profile{Rate: 20, Duration: 200 * time.Millisecond, Concurrency: 2},
		Getenv:  func(string) string { return "" },
		Client:  &http.Client{CheckRedirect: func(*http.Request, []*http.Request) error { return nil }},
	})
	if err != nil {
		t.Fatal(err)
	}
	if otherHits.Load() != 0 || rep.Paths[0].ErrorRate != 1 {
		t.Fatalf("redirect followed: hits=%d %+v", otherHits.Load(), rep.Paths[0])
	}
}

func TestNewClientNeverUsesTheEnvironmentProxy(t *testing.T) {
	for name, supplied := range map[string]*http.Client{"none": nil, "bare": {}} {
		c := newClient(supplied)
		tr, ok := c.Transport.(*http.Transport)
		if !ok || tr.Proxy != nil {
			t.Fatalf("%s: transport %T keeps a proxy", name, c.Transport)
		}
	}
	own := &http.Transport{}
	if got := newClient(&http.Client{Transport: own}); got.Transport != http.RoundTripper(own) {
		t.Fatal("a supplied transport must be kept")
	}
}

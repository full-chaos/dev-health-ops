package loadharness

import (
	"context"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func noEnv(string) string { return "" }

func runOne(t *testing.T, h http.HandlerFunc, tg Target, rate float64, d time.Duration) Report {
	t.Helper()
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)
	tg.Name, tg.Weight = "p", 1
	if tg.Method == "" {
		tg.Method = "GET"
	}
	if tg.Path == "" {
		tg.Path = "/"
	}
	if len(tg.ExpectStatus) == 0 {
		tg.ExpectStatus = []int{200}
	}
	rep, err := Run(context.Background(), Config{BaseURL: srv.URL, Targets: []Target{tg}},
		Options{Profile: Profile{Name: "t", Rate: rate, Duration: d, Concurrency: 4}, Getenv: noEnv})
	if err != nil {
		t.Fatal(err)
	}
	return rep
}

func TestStatusHistogramAndUnexpected5xx(t *testing.T) {
	var n atomic.Int64
	rep := runOne(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1)%2 == 0 {
			w.WriteHeader(500)
			return
		}
		w.WriteHeader(401)
	}, Target{ExpectStatus: []int{401}}, 100, 400*time.Millisecond)
	p := rep.Paths[0]
	if p.Statuses["401"] != 20 || p.Statuses["500"] != 20 {
		t.Fatalf("histogram %+v", p.Statuses)
	}
	if p.Unexpected5xx != 20 {
		t.Fatalf("unexpected_5xx %d", p.Unexpected5xx)
	}
}

func TestExpectedFailClosed503IsNotUnexpected(t *testing.T) {
	rep := runOne(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(503) },
		Target{ExpectStatus: []int{503}}, 50, 200*time.Millisecond)
	if rep.Paths[0].Unexpected5xx != 0 || rep.Paths[0].Errors != 0 {
		t.Fatalf("%+v", rep.Paths[0])
	}
}

func TestFirstSeenRecordsLimiterOnset(t *testing.T) {
	var n atomic.Int64
	rep := runOne(t, func(w http.ResponseWriter, r *http.Request) {
		if n.Add(1) > 10 {
			w.WriteHeader(429)
			return
		}
		w.WriteHeader(401)
	}, Target{ExpectStatus: []int{401, 429}}, 100, 400*time.Millisecond)
	ms, ok := rep.Paths[0].FirstSeenMs["429"]
	if !ok || ms < 50 {
		t.Fatalf("first 429 at %v ok=%v", ms, ok)
	}
}

func TestForbiddenStatusFailsAssertion(t *testing.T) {
	rep := runOne(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) },
		Target{ExpectStatus: []int{200, 401}, ForbidStatus: []int{200}}, 50, 200*time.Millisecond)
	if rep.AssertOK || rep.Paths[0].Forbidden == 0 {
		t.Fatalf("200 on a bad-credential path passed: %+v", rep)
	}
	if len(rep.Assertions) == 0 || rep.Assertions[0].OK {
		t.Fatalf("%+v", rep.Assertions)
	}
}

func TestForbid5xxAndRequireStatus(t *testing.T) {
	rep := runOne(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) },
		Target{ExpectStatus: []int{401, 429}, Forbid5xx: true, RequireStatus: []int{429}}, 50, 200*time.Millisecond)
	if rep.AssertOK {
		t.Fatal("a required 429 never seen passed")
	}
	rep = runOne(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(401) },
		Target{ExpectStatus: []int{401}, Forbid5xx: true, ForbidStatus: []int{200}}, 50, 200*time.Millisecond)
	if !rep.AssertOK {
		t.Fatalf("clean abuse run failed: %+v", rep.Assertions)
	}
	rep = runOne(t, func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) },
		Target{ExpectStatus: []int{401, 500}, Forbid5xx: true}, 50, 200*time.Millisecond)
	if rep.AssertOK {
		t.Fatal("5xx passed under forbid_5xx")
	}
}

func TestPadPlaceholder(t *testing.T) {
	var got atomic.Int64
	rep := runOne(t, func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 1<<16)
		total := 0
		for {
			n, err := r.Body.Read(b)
			total += n
			if err != nil {
				break
			}
		}
		got.Store(int64(total))
	}, Target{Method: "POST", Body: "x${PAD:5000}"}, 20, 100*time.Millisecond)
	if got.Load() != 5001 || rep.Paths[0].Errors != 0 {
		t.Fatalf("body bytes %d %+v", got.Load(), rep.Paths[0])
	}
	if _, err := expand("${PAD:9999999999}", 0, noEnv); err == nil {
		t.Fatal("oversize pad accepted")
	}
	if _, err := expand("${PAD:-1}", 0, noEnv); err == nil {
		t.Fatal("negative pad accepted")
	}
	if _, err := ParseConfig([]byte(`{"targets":[{"name":"a","path":"/${PAD:5}"}]}`), noEnv); err == nil {
		t.Fatal("pad in path accepted")
	}
}

func TestTimelineBuckets(t *testing.T) {
	rep := runOne(t, func(w http.ResponseWriter, r *http.Request) {}, Target{}, 40, 2500*time.Millisecond)
	if len(rep.Timeline) < 2 {
		t.Fatalf("%+v", rep.Timeline)
	}
	var sum int
	for i, b := range rep.Timeline {
		if b.Second != i {
			t.Fatalf("bucket %d has second %d", i, b.Second)
		}
		sum += b.Count
	}
	if sum != 100 {
		t.Fatalf("timeline holds %d requests, want 100", sum)
	}
}

func bk(errs ...int) []Bucket {
	out := make([]Bucket, len(errs))
	for i, e := range errs {
		out[i] = Bucket{Second: i, Count: 10, Errors: e}
	}
	return out
}

func TestAnalyzeFault(t *testing.T) {
	// fault on at 3s, off at 8s; errors from 4s to 10s inclusive; clean from 11s
	b := bk(0, 0, 0, 0, 10, 10, 10, 10, 10, 10, 10, 0, 0, 0, 0)
	f := AnalyzeFault(b, 3*time.Second, 8*time.Second, 0.05, 3)
	if f.TimeToDegradeMs == nil || *f.TimeToDegradeMs != 1000 {
		t.Fatalf("degrade %+v", f)
	}
	if !f.Recovered || *f.TimeToRecoverMs != 3000 {
		t.Fatalf("recover %+v", f)
	}
}

func TestAnalyzeFaultNeverRecovers(t *testing.T) {
	f := AnalyzeFault(bk(0, 0, 10, 10, 10, 10, 10), 1*time.Second, 3*time.Second, 0.05, 2)
	if f.Recovered || f.TimeToRecoverMs != nil {
		t.Fatalf("%+v", f)
	}
}

func TestAnalyzeFaultNoDegrade(t *testing.T) {
	f := AnalyzeFault(bk(0, 0, 0, 0, 0, 0), 1*time.Second, 3*time.Second, 0.05, 2)
	if f.TimeToDegradeMs != nil || !f.Recovered || *f.TimeToRecoverMs != 0 {
		t.Fatalf("%+v", f)
	}
}

func TestRecoveryBoundAssertion(t *testing.T) {
	r := Report{AssertOK: true, Fault: &FaultResult{Recovered: true, TimeToRecoverMs: ptr(int64(30000))}}
	if got := ApplyRecoveryBound(r, 60*time.Second); !got.AssertOK {
		t.Fatal("30s recovery failed a 60s bound")
	}
	if got := ApplyRecoveryBound(r, 10*time.Second); got.AssertOK {
		t.Fatal("30s recovery passed a 10s bound")
	}
	r.Fault = &FaultResult{}
	if got := ApplyRecoveryBound(r, time.Minute); got.AssertOK {
		t.Fatal("no recovery passed")
	}
	if got := ApplyRecoveryBound(Report{AssertOK: true}, time.Minute); got.AssertOK {
		t.Fatal("a bound without a fault window passed")
	}
}

func ptr[T any](v T) *T { return &v }

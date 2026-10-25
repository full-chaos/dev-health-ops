package faultproxy

import (
	"context"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/loadharness"
)

// A real load run through the proxy: a blackhole window must show as a
// degradation and, once it ends, as a measured recovery inside a bound.
func TestSmokeBlackholeThroughProxy(t *testing.T) {
	up := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(200) }))
	defer up.Close()
	host := up.Listener.Addr().(*net.TCPAddr).String()
	p, err := New(Config{Listen: "127.0.0.1:0", Upstream: host, MaxConns: 64})
	if err != nil {
		t.Fatal(err)
	}
	defer p.Stop()
	steps, err := ParseSchedule("1s=blackhole,3s=pass")
	if err != nil {
		t.Fatal(err)
	}
	cfg := loadharness.Config{BaseURL: "http://" + p.Addr().String(), Targets: []loadharness.Target{
		{Name: "svc", Method: "GET", Path: "/", Weight: 1, ExpectStatus: []int{200}},
	}}
	p.Schedule(steps)
	rep, err := loadharness.Run(context.Background(), cfg, loadharness.Options{
		Profile: loadharness.Profile{Name: "smoke", Rate: 30, Duration: 7 * time.Second, Concurrency: 32},
		Getenv:  func(string) string { return "" },
		Client:  &http.Client{Timeout: 400 * time.Millisecond},
		Fault:   &loadharness.FaultWindow{Name: "blackhole", On: time.Second, Off: 3 * time.Second, MaxErr: 0.05, Sustain: 2},
	})
	if err != nil {
		t.Fatal(err)
	}
	rep = loadharness.ApplyRecoveryBound(rep, 4*time.Second)
	f := rep.Fault
	if f == nil || f.TimeToDegradeMs == nil || *f.TimeToDegradeMs > 1500 {
		t.Fatalf("blackhole did not degrade the run quickly: %+v", f)
	}
	if !f.Recovered || *f.TimeToRecoverMs > 4000 || !rep.AssertOK {
		t.Fatalf("no recovery inside 4s: %+v assertions=%+v", f, rep.Assertions)
	}
	if rep.Paths[0].Errors == 0 {
		t.Fatalf("a blackhole window produced no errors: %+v", rep.Paths[0])
	}
}

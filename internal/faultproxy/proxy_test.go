package faultproxy

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

var upstreamBytes atomic.Int64

func echo(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	upstreamBytes.Store(0)
	go func() {
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			go func() {
				defer c.Close()
				buf := make([]byte, 4096)
				for {
					n, err := c.Read(buf)
					upstreamBytes.Add(int64(n))
					if n > 0 {
						if _, werr := c.Write(buf[:n]); werr != nil {
							return
						}
					}
					if err != nil {
						return
					}
				}
			}()
		}
	}()
	t.Cleanup(func() { l.Close() })
	return l.Addr().String()
}

func startProxy(t *testing.T, cfg Config) *Proxy {
	t.Helper()
	cfg.Listen = "127.0.0.1:0"
	if cfg.Upstream == "" {
		cfg.Upstream = echo(t)
	}
	p, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(p.Stop)
	return p
}

func dial(t *testing.T, p *Proxy) net.Conn {
	t.Helper()
	c, err := net.Dial("tcp", p.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func roundTrip(c net.Conn, msg string, wait time.Duration) (string, error) {
	_ = c.SetDeadline(time.Now().Add(wait))
	if _, err := c.Write([]byte(msg)); err != nil {
		return "", err
	}
	buf := make([]byte, len(msg))
	_, err := io.ReadFull(c, buf)
	return string(buf), err
}

func isTimeout(err error) bool {
	var ne net.Error
	return errors.As(err, &ne) && ne.Timeout()
}

func TestPassForwardsBytes(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 8})
	got, err := roundTrip(dial(t, p), "hello", 2*time.Second)
	if err != nil || got != "hello" {
		t.Fatalf("%q %v", got, err)
	}
}

func TestDelayAddsLatency(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 8})
	p.Set(Setting{Mode: Delay, Latency: 150 * time.Millisecond})
	start := time.Now()
	got, err := roundTrip(dial(t, p), "ping", 3*time.Second)
	if err != nil || got != "ping" {
		t.Fatalf("%q %v", got, err)
	}
	if d := time.Since(start); d < 250*time.Millisecond {
		t.Fatalf("round trip took %v, want both directions delayed", d)
	}
}

func TestDropResetsNewConns(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 8})
	p.Set(Setting{Mode: Drop})
	c, err := net.Dial("tcp", p.Addr().String())
	if err == nil {
		defer c.Close()
		_ = c.SetReadDeadline(time.Now().Add(time.Second))
		_, err = c.Read(make([]byte, 1))
	}
	if err == nil || isTimeout(err) {
		t.Fatalf("a dropped conn was not reset: %v", err)
	}
	if p.Stats().Refused < 1 {
		t.Fatalf("%+v", p.Stats())
	}
}

func TestBlackholeNeverAnswers(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 8})
	p.Set(Setting{Mode: Blackhole})
	c := dial(t, p)
	_, err := roundTrip(c, "anyone?", 300*time.Millisecond)
	if !isTimeout(err) {
		t.Fatalf("want a read timeout, got %v", err)
	}
	if upstreamBytes.Load() != 0 {
		t.Fatalf("upstream saw %d bytes through a blackhole", upstreamBytes.Load())
	}
}

func TestBlackholeDiscardsWithoutBuffering(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 8})
	p.Set(Setting{Mode: Blackhole})
	c := dial(t, p)
	_ = c.SetWriteDeadline(time.Now().Add(5 * time.Second))
	if _, err := c.Write(make([]byte, 8<<20)); err != nil {
		t.Fatalf("8 MiB into a blackhole did not drain: %v", err)
	}
}

func TestBlackholeAppliesToLiveConns(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 8})
	c := dial(t, p)
	if _, err := roundTrip(c, "ok", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	p.Set(Setting{Mode: Blackhole})
	if _, err := roundTrip(c, "lost", 300*time.Millisecond); !isTimeout(err) {
		t.Fatalf("live conn still answered: %v", err)
	}
}

func TestCutClosesLiveConns(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 8})
	c := dial(t, p)
	if _, err := roundTrip(c, "ok", 2*time.Second); err != nil {
		t.Fatal(err)
	}
	p.Set(Setting{Mode: Cut})
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil || isTimeout(err) {
		t.Fatalf("live conn survived a cut: %v", err)
	}
	c2 := dial(t, p)
	_ = c2.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c2.Read(make([]byte, 1)); err == nil || isTimeout(err) {
		t.Fatalf("new conn survived a cut: %v", err)
	}
}

func TestSlowCapsRate(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 8})
	p.Set(Setting{Mode: Slow, BytesPerSec: 20000})
	start := time.Now()
	if _, err := roundTrip(dial(t, p), strings.Repeat("a", 10000), 10*time.Second); err != nil {
		t.Fatal(err)
	}
	if d := time.Since(start); d < 450*time.Millisecond {
		t.Fatalf("10000 bytes at 20000 B/s took %v", d)
	}
}

func TestConnCapRefusesOverLimit(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 2})
	a, b := dial(t, p), dial(t, p)
	for _, c := range []net.Conn{a, b} {
		if _, err := roundTrip(c, "x", 2*time.Second); err != nil {
			t.Fatal(err)
		}
	}
	c := dial(t, p)
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil || isTimeout(err) {
		t.Fatalf("third conn was served: %v", err)
	}
	if p.Stats().Refused < 1 || p.Stats().Live > 2 {
		t.Fatalf("%+v", p.Stats())
	}
}

func TestModeSwitchAtRuntimeViaControl(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 8})
	srv := httptest.NewServer(p.ControlHandler())
	defer srv.Close()
	post := func(q string) int {
		resp, err := http.Post(srv.URL+"/mode?"+q, "", nil)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	if code := post("name=blackhole"); code != 200 || p.Current().Mode != Blackhole {
		t.Fatalf("code=%d mode=%v", code, p.Current().Mode)
	}
	if code := post("name=delay&latency=50ms&jitter=10ms"); code != 200 || p.Current().Latency != 50*time.Millisecond {
		t.Fatalf("code=%d %+v", code, p.Current())
	}
	for _, bad := range []string{"name=nope", "name=delay&latency=-1s", "name=delay&latency=999h", "name=slow", "name=slow&bps=0", ""} {
		if code := post(bad); code != 400 {
			t.Fatalf("%q -> %d, want 400", bad, code)
		}
	}
	resp, err := http.Get(srv.URL + "/mode")
	if err != nil {
		t.Fatal(err)
	}
	var m struct{ Mode string }
	_ = json.NewDecoder(resp.Body).Decode(&m)
	resp.Body.Close()
	if m.Mode != "delay" {
		t.Fatalf("GET /mode = %+v", m)
	}
	resp, _ = http.Get(srv.URL + "/mode?name=pass")
	resp.Body.Close()
	if resp.StatusCode != 405 {
		t.Fatalf("GET with a mode change = %d, want 405", resp.StatusCode)
	}
}

type syncBuf struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (s *syncBuf) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.Write(p)
}

func (s *syncBuf) String() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.b.String()
}

func TestScheduleFlipsAndWritesEvents(t *testing.T) {
	var ev syncBuf
	p := startProxy(t, Config{MaxConns: 8, Events: &ev})
	steps, err := ParseSchedule("100ms=blackhole,300ms=pass")
	if err != nil {
		t.Fatal(err)
	}
	p.Schedule(steps)
	time.Sleep(150 * time.Millisecond)
	if p.Current().Mode != Blackhole {
		t.Fatalf("mode at 150ms: %v", p.Current().Mode)
	}
	time.Sleep(400 * time.Millisecond)
	if p.Current().Mode != Pass {
		t.Fatalf("mode at 550ms: %v", p.Current().Mode)
	}
	lines := strings.Split(strings.TrimSpace(ev.String()), "\n")
	if len(lines) != 3 {
		t.Fatalf("events: %q", ev.String())
	}
	var last int64 = -1
	for i, l := range lines {
		var e struct {
			Mode     string `json:"mode"`
			OffsetMs int64  `json:"offset_ms"`
			UnixMs   int64  `json:"unix_ms"`
		}
		if err := json.Unmarshal([]byte(l), &e); err != nil || e.UnixMs == 0 || e.OffsetMs < last {
			t.Fatalf("line %d %q %v", i, l, err)
		}
		last = e.OffsetMs
	}
	if !strings.Contains(lines[1], `"blackhole"`) || !strings.Contains(lines[2], `"pass"`) {
		t.Fatalf("events: %q", ev.String())
	}
}

func TestParseScheduleRefusals(t *testing.T) {
	for _, bad := range []string{
		"", "5s", "5s=explode", "-1s=pass", "5s=pass,3s=blackhole", "5s=pass,5s=cut",
		"x=pass", "5s=delay", "5s=delay:abc", "5s=slow", "5s=slow:0", "5s=slow:-4", "5s=pass:1", "5s=delay:-1s",
	} {
		if _, err := ParseSchedule(bad); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
	steps, err := ParseSchedule("1s=delay:200ms,2s=slow:5000,3s=cut,4s=drop,5s=blackhole,6s=pass")
	if err != nil || len(steps) != 6 || steps[0].Set.Latency != 200*time.Millisecond || steps[1].Set.BytesPerSec != 5000 {
		t.Fatalf("%v %+v", err, steps)
	}
}

func TestControlListensLoopbackOnly(t *testing.T) {
	h := http.NewServeMux()
	if _, _, err := StartControl("0.0.0.0:0", h); err == nil {
		t.Fatal("control bound to all interfaces")
	}
	if _, _, err := StartControl("192.0.2.1:0", h); err == nil {
		t.Fatal("control bound to a non-loopback address")
	}
	for _, ok := range []string{"127.0.0.1:0", "localhost:0", "[::1]:0"} {
		addr, stop, err := StartControl(ok, h)
		if err != nil {
			if ok == "[::1]:0" {
				continue
			}
			t.Fatalf("%s: %v", ok, err)
		}
		if !addr.(*net.TCPAddr).IP.IsLoopback() {
			t.Fatalf("%s bound to %v", ok, addr)
		}
		stop()
	}
}

func TestHostGuardRefusesOtherHosts(t *testing.T) {
	if err := CheckAddr("127.0.0.1:1", nil); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"0.0.0.0:1", "db.example.com:5432", "10.0.0.5:5432", "127.0.0.1", ":5432"} {
		if err := CheckAddr(bad, nil); err == nil {
			t.Fatalf("%q accepted", bad)
		}
	}
	if err := CheckAddr("postgres:5432", []string{"postgres"}); err != nil {
		t.Fatal(err)
	}
	if err := CheckAddr("auth-service:8081", nil); err != nil {
		t.Fatal(err)
	}
}

func TestStatsHaveNoPayload(t *testing.T) {
	const secret = "s3cr3t-credential-value"
	var ev syncBuf
	p := startProxy(t, Config{MaxConns: 8, Events: &ev})
	if _, err := roundTrip(dial(t, p), secret, 2*time.Second); err != nil {
		t.Fatal(err)
	}
	st, _ := json.Marshal(p.Stats())
	if strings.Contains(string(st)+ev.String(), secret) {
		t.Fatal("payload in stats or events")
	}
	deadline := time.Now().Add(2 * time.Second)
	for (p.Stats().BytesUp != int64(len(secret)) || p.Stats().BytesDown != int64(len(secret))) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	if p.Stats().BytesUp != int64(len(secret)) || p.Stats().BytesDown != int64(len(secret)) {
		t.Fatalf("%+v", p.Stats())
	}
}

func TestStopEndsEveryGoroutine(t *testing.T) {
	up := echo(t)
	time.Sleep(50 * time.Millisecond)
	base := runtime.NumGoroutine()
	p, err := New(Config{Listen: "127.0.0.1:0", Upstream: up, MaxConns: 8})
	if err != nil {
		t.Fatal(err)
	}
	steps, _ := ParseSchedule("10s=blackhole")
	p.Schedule(steps)
	var conns []net.Conn
	for i := 0; i < 3; i++ {
		c := dial(t, p)
		if _, err := roundTrip(c, "x", 2*time.Second); err != nil {
			t.Fatal(err)
		}
		conns = append(conns, c)
	}
	p.Stop()
	p.Stop()
	for _, c := range conns {
		c.Close()
	}
	if _, err := net.DialTimeout("tcp", p.Addr().String(), 300*time.Millisecond); err == nil {
		t.Fatal("proxy still accepts after Stop")
	}
	deadline := time.Now().Add(3 * time.Second)
	for runtime.NumGoroutine() > base && time.Now().Before(deadline) {
		time.Sleep(20 * time.Millisecond)
	}
	if n := runtime.NumGoroutine(); n > base {
		buf := make([]byte, 1<<16)
		t.Fatalf("goroutines %d > baseline %d\n%s", n, base, buf[:runtime.Stack(buf, true)])
	}
}

func TestBlackholedConnForwardsAgainAfterPass(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 8})
	p.Set(Setting{Mode: Blackhole})
	c := dial(t, p)
	if _, err := roundTrip(c, "lost", 300*time.Millisecond); !isTimeout(err) {
		t.Fatalf("blackholed conn answered: %v", err)
	}
	p.Set(Setting{Mode: Pass})
	got, err := roundTrip(c, "back", 2*time.Second)
	if err != nil || got != "back" {
		t.Fatalf("conn accepted during the blackhole never recovered: %q %v", got, err)
	}
	if upstreamBytes.Load() != 4 {
		t.Fatalf("upstream saw %d bytes, want only the post-recovery 4", upstreamBytes.Load())
	}
}

func TestBlackholedConnClosesOnCutWithoutData(t *testing.T) {
	p := startProxy(t, Config{MaxConns: 8})
	p.Set(Setting{Mode: Blackhole})
	c := dial(t, p)
	time.Sleep(50 * time.Millisecond)
	p.Set(Setting{Mode: Cut})
	_ = c.SetReadDeadline(time.Now().Add(time.Second))
	if _, err := c.Read(make([]byte, 1)); err == nil || isTimeout(err) {
		t.Fatalf("blackholed conn survived a cut: %v", err)
	}
}

//go:build integration

package authruntime

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/auth/authconfig"
	"github.com/full-chaos/dev-health-ops/internal/token"
)

// blackholeProxy forwards TCP to target until blackhole is set; after that it
// accepts and holds connections and forwards no byte either way.
type blackholeProxy struct {
	listener net.Listener
	target   string
	hole     atomic.Bool
	dropped  atomic.Int64 // bytes read from a client while the hole was open (never forwarded)
	mu       sync.Mutex
	conns    []net.Conn
}

func startBlackholeProxy(t *testing.T, target string) *blackholeProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	p := &blackholeProxy{listener: listener, target: target}
	t.Cleanup(p.close)
	go p.serve()
	return p
}

func (p *blackholeProxy) addr() string { return p.listener.Addr().String() }

func (p *blackholeProxy) close() {
	_ = p.listener.Close()
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, conn := range p.conns {
		_ = conn.Close()
	}
}

func (p *blackholeProxy) serve() {
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		p.mu.Lock()
		p.conns = append(p.conns, client)
		p.mu.Unlock()
		go p.handle(client)
	}
}

func (p *blackholeProxy) handle(client net.Conn) {
	if p.hole.Load() {
		buffer := make([]byte, 32<<10)
		for {
			n, err := client.Read(buffer)
			p.dropped.Add(int64(n))
			if err != nil {
				return
			}
		}
	}
	upstream, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	p.mu.Lock()
	p.conns = append(p.conns, upstream)
	p.mu.Unlock()
	go p.pipe(upstream, client)
	p.pipe(client, upstream)
}

func (p *blackholeProxy) pipe(dst, src net.Conn) {
	buffer := make([]byte, 32<<10)
	for {
		n, err := src.Read(buffer)
		if n > 0 && p.hole.Load() {
			p.dropped.Add(int64(n))
		}
		if n > 0 && !p.hole.Load() {
			if _, werr := dst.Write(buffer[:n]); werr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

// TestJWKSAnswersWhileTheDatabaseIsBlackholed drives the real stack (configure
// over live PostgreSQL and Valkey, the production middleware chain) with the
// database behind a TCP blackhole: login requests stall to the request
// timeout and hold in-flight slots while key-set requests run. The test asserts
// no absolute time. Precondition: the due JWKS request's key-state read reaches
// the blackhole (the proxy's dropped-bytes counter grows), so a run that never
// reaches the inline wait fails on its own line. Then: every JWKS request
// (the burst and the due request) is a 200; the burst's p50 is at most
// max(5 x the healthy burst's p50, 1 s) and the slowest request at most
// max(25 x the healthy burst's max, 1 s) -- a ratio against a healthy burst of
// this same test, the 1 s being only a floor under the ratio bounds.
func TestJWKSAnswersWhileTheDatabaseIsBlackholed(t *testing.T) {
	const (
		requestTimeout = 4 * time.Second
		stallers       = 3 * maxInFlightRequests
		probes         = 256
	)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()
	keyDir := t.TempDir()
	writeSigningKey(t, keyDir, "active.pem")
	var proxy *blackholeProxy
	e := newRoutesEnvDialing(t, ctx, map[string]string{
		authconfig.EnvSigningKeyFile:     filepath.Join(keyDir, "active.pem"),
		authconfig.EnvSigningKeyID:       "stall-test",
		authconfig.EnvRequestTimeout:     requestTimeout.String(),
		authconfig.EnvRateLimitPerSecond: "100000",
		authconfig.EnvRateLimitBurst:     "100000",
	}, func(dsn *url.URL) *url.URL {
		proxy = startBlackholeProxy(t, dsn.Host)
		out := *dsn
		out.Host = proxy.addr()
		return &out
	})
	transport := &http.Transport{MaxIdleConnsPerHost: 512}
	defer transport.CloseIdleConnections()
	// The 30 s client timeout is a failure-only ceiling: a JWKS request that waits on the dead database ends as a client
	// error (status 0), which the 200 count below refuses. A passing run never comes near it.
	client := &http.Client{Transport: transport, Timeout: 30 * time.Second}

	login := func(i int) (int, time.Duration) {
		body := fmt.Sprintf(`{"email":"stall-%d@example.com","password":"wrong"}`, i)
		request, err := http.NewRequest(http.MethodPost, e.front.URL+"/api/v1/auth/login", strings.NewReader(body))
		if err != nil {
			t.Error(err)
			return 0, 0
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("X-Forwarded-For", fmt.Sprintf("198.51.100.%d", i%250+1))
		start := time.Now()
		response, err := client.Do(request)
		if err != nil {
			return 0, time.Since(start)
		}
		_ = response.Body.Close()
		return response.StatusCode, time.Since(start)
	}
	if status, _ := login(0); status != http.StatusUnauthorized {
		t.Fatalf("login before the fault = %d, want 401 through the proxy", status)
	}

	// Prime the key-state snapshot, then let it become due for a refresh so
	// the first request after the fault is the one that reads the database.
	primed, err := client.Get(e.front.URL + "/.well-known/jwks.json")
	if err != nil || primed.StatusCode != http.StatusOK {
		t.Fatalf("JWKS before the fault = %v %v, want 200", primed, err)
	}
	_ = primed.Body.Close()
	time.Sleep(token.KeyStateRefresh + time.Second)

	// One burst of probes, with each request's own duration. The healthy burst below (same test, same process, same
	// concurrency, just before the fault) is the baseline the faulted burst is compared with by ratio.
	jwksBurst := func() ([]int, []time.Duration) {
		statuses := make([]int, probes)
		took := make([]time.Duration, probes)
		var probesDone sync.WaitGroup
		for i := range probes {
			probesDone.Add(1)
			go func() {
				defer probesDone.Done()
				start := time.Now()
				response, err := client.Get(e.front.URL + "/.well-known/jwks.json")
				took[i] = time.Since(start)
				if err == nil {
					statuses[i] = response.StatusCode
					_, _ = io.Copy(io.Discard, response.Body)
					_ = response.Body.Close()
				}
			}()
		}
		probesDone.Wait()
		sort.Slice(took, func(a, b int) bool { return took[a] < took[b] })
		return statuses, took
	}
	baselineStatuses, baselineTook := jwksBurst()
	for _, status := range baselineStatuses {
		if status != http.StatusOK {
			t.Fatalf("healthy JWKS burst answered %v, want %d x 200 before the fault", baselineStatuses, probes)
		}
	}
	baselineP50, baselineMax := baselineTook[probes/2], baselineTook[probes-1]

	// The healthy burst above started the key-state refresh and made the snapshot fresh (a refresh starts at most once per
	// KeyStateRefresh): let it become due again, so that the first JWKS request after the fault is the one that starts the
	// read and waits for it inline (the path this test guards). Without this the faulted burst would be served from a fresh
	// snapshot and never reach that path.
	time.Sleep(token.KeyStateRefresh + time.Second)

	proxy.hole.Store(true)
	// The due request goes first and alone, so that the only database traffic during the fault is its key-state read; the
	// test proves that read reached the blackhole (the inline wait is in progress) before it goes on.
	var dueStatus int
	var dueTook time.Duration
	dueDone := make(chan struct{})
	droppedBefore := proxy.dropped.Load()
	go func() {
		defer close(dueDone)
		start := time.Now()
		response, err := client.Get(e.front.URL + "/.well-known/jwks.json")
		dueTook = time.Since(start)
		if err == nil {
			dueStatus = response.StatusCode
			_, _ = io.Copy(io.Discard, response.Body)
			_ = response.Body.Close()
		}
	}()
	reachDeadline := time.Now().Add(15 * time.Second)
	for proxy.dropped.Load() == droppedBefore {
		if time.Now().After(reachDeadline) {
			t.Fatal("the due JWKS request's key-state read never reached the blackhole within the 15 s guard: the faulted burst cannot reach the inline-wait path")
		}
		time.Sleep(10 * time.Millisecond)
	}
	var wg sync.WaitGroup
	var held atomic.Int64
	for i := 1; i <= stallers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			//abstime:allow lower bound: counts the logins the blackhole held for at least half the request timeout (the fault precondition), never a speed check
			if _, took := login(i); took >= requestTimeout/2 {
				held.Add(1)
			}
		}()
	}
	time.Sleep(requestTimeout / 4)

	statuses, took := jwksBurst()
	<-dueDone
	wg.Wait()
	statuses = append(statuses, dueStatus)
	faultedMax := max(took[probes-1], dueTook)

	counts := map[int]int{}
	for _, status := range statuses {
		counts[status]++
	}
	t.Logf("load: %d stalled logins, %d held >= %s; %d JWKS GETs at concurrency %d; JWKS statuses=%v p50=%s p99=%s p100=%s due request=%s",
		stallers, held.Load(), requestTimeout/2, probes, probes, counts, took[probes/2], took[probes*99/100], took[probes-1], dueTook)
	if held.Load() < maxInFlightRequests {
		t.Fatalf("only %d logins were held by the blackhole, want >= %d: the fault did not stall the database routes", held.Load(), maxInFlightRequests)
	}
	if counts[http.StatusOK] != probes+1 {
		for _, line := range strings.Split(e.logs.String(), "\n") {
			if strings.Contains(line, "well-known") || strings.Contains(line, "jwks") {
				t.Log(line)
				break
			}
		}
		t.Errorf("JWKS answered %v, want %d x 200 (0 x 429)", counts, probes+1)
	}
	// The ratio against the healthy burst of this test (D6418: a ratio in the same process, not an absolute time): a JWKS
	// request that waits on the blackholed database is slower by orders of magnitude (its wait is the request timeout).
	// A check fails only when it is over BOTH its ratio bound AND 1 s (the floor keeps two tiny durations from a flaky ratio).
	p50Limit, maxLimit := max(5*baselineP50, time.Second), max(25*baselineMax, time.Second)
	t.Logf("JWKS ratio vs the healthy burst: p50 %s vs %s (%.1fx, bound 5x and 1s), max %s vs %s (%.1fx, bound 25x and 1s)",
		took[probes/2], baselineP50, float64(took[probes/2])/float64(baselineP50), faultedMax, baselineMax, float64(faultedMax)/float64(baselineMax))
	if took[probes/2] > p50Limit {
		t.Errorf("JWKS p50 ratio: p50 %s under the fault vs %s healthy: over 5x the baseline p50 and over 1 s: the JWKS answer waited on the dead database", took[probes/2], baselineP50)
	}
	if faultedMax > maxLimit {
		t.Errorf("JWKS max ratio: slowest %s under the fault vs %s healthy: over 25x the baseline max and over 1 s: the JWKS answer waited on the dead database", faultedMax, baselineMax)
	}
}

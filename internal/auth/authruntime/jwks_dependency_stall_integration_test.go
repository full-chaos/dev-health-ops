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
		_, _ = io.Copy(io.Discard, client)
		return
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
// timeout and hold in-flight slots while key-set requests run. Bounds: every
// JWKS request is a 200 and finishes within jwksBound, far under the request
// timeout.
func TestJWKSAnswersWhileTheDatabaseIsBlackholed(t *testing.T) {
	const (
		requestTimeout = 4 * time.Second
		jwksBound      = time.Second
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
	client := &http.Client{Transport: transport}

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

	proxy.hole.Store(true)
	var wg sync.WaitGroup
	var held atomic.Int64
	for i := 1; i <= stallers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, took := login(i); took >= requestTimeout/2 {
				held.Add(1)
			}
		}()
	}
	time.Sleep(requestTimeout / 4)

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
	wg.Wait()

	counts := map[int]int{}
	for _, status := range statuses {
		counts[status]++
	}
	sort.Slice(took, func(a, b int) bool { return took[a] < took[b] })
	t.Logf("load: %d stalled logins, %d held >= %s; %d JWKS GETs at concurrency %d; JWKS statuses=%v p50=%s p99=%s p100=%s",
		stallers, held.Load(), requestTimeout/2, probes, probes, counts, took[probes/2], took[probes*99/100], took[probes-1])
	if held.Load() < maxInFlightRequests {
		t.Fatalf("only %d logins were held by the blackhole, want >= %d: the fault did not stall the database routes", held.Load(), maxInFlightRequests)
	}
	if counts[http.StatusOK] != probes {
		for _, line := range strings.Split(e.logs.String(), "\n") {
			if strings.Contains(line, "well-known") || strings.Contains(line, "jwks") {
				t.Log(line)
				break
			}
		}
		t.Errorf("JWKS answered %v, want %d x 200 (0 x 429)", counts, probes)
	}
	if took[probes-1] > jwksBound {
		t.Errorf("slowest JWKS request took %s, want <= %s (request timeout %s)", took[probes-1], jwksBound, requestTimeout)
	}
}

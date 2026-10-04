//go:build integration

package ingressplanes

import (
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// These tests hold the two waits of the real-nginx test (CHAOS-8542). They
// start no container and no nginx: the "router" is a listener of this process
// that behaves like the mapped port of a container whose nginx is not up yet.

// earlyPort is a listener that ends the first `drops` connections with no
// answer, as the port forwarder of a container does while nothing listens
// behind it, and serves HTTP after that. drops < 0 means: never serve.
type earlyPort struct {
	net.Listener
	drops   int32
	dropped atomic.Int32
}

func (e *earlyPort) Accept() (net.Conn, error) {
	for {
		connection, err := e.Listener.Accept()
		if err != nil {
			return nil, err
		}
		if e.drops < 0 || e.dropped.Load() < e.drops {
			e.dropped.Add(1)
			_ = connection.Close()
			continue
		}
		return connection, nil
	}
}

// startEarlyPort serves handler behind an earlyPort and gives its base URL.
func startEarlyPort(t *testing.T, drops int32, handler http.Handler) (string, *earlyPort) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	port := &earlyPort{Listener: listener, drops: drops}
	server := &http.Server{Handler: handler}
	go func() { _ = server.Serve(port) }()
	t.Cleanup(func() { _ = server.Close() })
	return "http://" + listener.Addr().String(), port
}

// routerWithNoPlanes answers as the router does before its planes exist.
var routerWithNoPlanes = http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
	w.WriteHeader(http.StatusBadGateway)
})

// fatalRecorder is a testing.TB whose Fatalf is recorded and ends the calling
// goroutine, so a test can read what a helper says when it gives up.
type fatalRecorder struct {
	testing.TB
	fatal string
}

func (f *fatalRecorder) Helper()             {}
func (f *fatalRecorder) Logf(string, ...any) {}
func (f *fatalRecorder) Fatalf(format string, args ...any) {
	f.fatal = fmt.Sprintf(format, args...)
	runtime.Goexit()
}

// whatItSaysWhenItGivesUp runs a helper that must fail and gives its message.
func whatItSaysWhenItGivesUp(t *testing.T, run func(testing.TB)) string {
	t.Helper()
	recorder := &fatalRecorder{TB: t}
	done := make(chan struct{})
	go func() {
		defer close(done)
		run(recorder)
	}()
	<-done
	if recorder.fatal == "" {
		t.Fatal("the helper did not give up")
	}
	return recorder.fatal
}

const sentinelLog = "router log: SENTINEL-8542"

func diagnoseSentinel() string { return sentinelLog }

// The failure of CHAOS-8542: the port accepts, nothing answers yet. One request
// at once gets no answer (that is what the test did before), and
// answersWithoutPlanes now waits for the router and then holds its 5xx.
func TestAnswersWithoutPlanes_WaitsForARouterWhosePortAcceptsFirst(t *testing.T) {
	base, port := startEarlyPort(t, 3, routerWithNoPlanes)
	response, err := (&http.Client{Timeout: 10 * time.Second}).Get(base + "/")
	if err == nil {
		_ = response.Body.Close()
		t.Fatalf("a request before the router is up was answered (%d): this listener does not show the failure", response.StatusCode)
	}
	t.Logf("one request before the router is up: %v", err)

	answersWithoutPlanes(t, base, 30*time.Second, diagnoseSentinel)
	if got := port.dropped.Load(); got != 3 {
		t.Errorf("%d connections ended with no answer, want 3: the wait did not go through the phase it exists for", got)
	}
}

// A router that is up at once needs no wait: one request for the wait, one for
// the 5xx.
func TestAnswersWithoutPlanes_ARouterThatIsUpIsAskedTwice(t *testing.T) {
	var requests atomic.Int32
	base, _ := startEarlyPort(t, 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		routerWithNoPlanes(w, r)
	}))
	answersWithoutPlanes(t, base, 30*time.Second, diagnoseSentinel)
	if got := requests.Load(); got != 2 {
		t.Errorf("%d requests, want 2 (the wait, then the 5xx read once)", got)
	}
}

// The 5xx itself is not waited for: a router that answers, but answers as a
// plane (with any status) or with a 2xx, fails at once.
func TestAnswersWithoutPlanes_DoesNotWaitForTheRightAnswer(t *testing.T) {
	for name, handler := range map[string]http.Handler{
		"a plane answers": goStub(PlaneGoAPI),
		"the router answers 200": http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.WriteHeader(http.StatusOK)
		}),
		"a plane answers 503": http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set(stubPlane, PlaneGoAPI)
			w.WriteHeader(http.StatusServiceUnavailable)
		}),
	} {
		var requests atomic.Int32
		base, _ := startEarlyPort(t, 0, http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			requests.Add(1)
			handler.ServeHTTP(w, r)
		}))
		said := whatItSaysWhenItGivesUp(t, func(tb testing.TB) { answersWithoutPlanes(tb, base, 30*time.Second, diagnoseSentinel) })
		if !strings.Contains(said, "want its own 5xx") || !strings.Contains(said, sentinelLog) {
			t.Errorf("%s: it said %q, want the 5xx failure with the router log", name, said)
		}
		if got := requests.Load(); got != 2 {
			t.Errorf("%s: %d requests, want 2: the answer was asked for again", name, got)
		}
	}
}

// The wait is bounded and loud: a router that never answers fails with the
// bound, the last error and the router log.
func TestWaitForRouter_IsLoudWhenTheBoundIsHit(t *testing.T) {
	base, port := startEarlyPort(t, -1, routerWithNoPlanes)
	started := time.Now()
	said := whatItSaysWhenItGivesUp(t, func(tb testing.TB) { waitForRouter(tb, base, 700*time.Millisecond, diagnoseSentinel) })
	if took := time.Since(started); took < 700*time.Millisecond || took > 20*time.Second {
		t.Errorf("gave up after %s, want just above the bound of 700ms", took)
	}
	for _, want := range []string{"the router did not answer an HTTP request within 700ms", "attempts, last: ", sentinelLog} {
		if !strings.Contains(said, want) {
			t.Errorf("it said %q, want %q in it", said, want)
		}
	}
	if port.dropped.Load() < 2 {
		t.Errorf("%d attempts reached the port, want 2 or more inside the bound", port.dropped.Load())
	}
}

// lateRouter answers each plane's probe as that plane, after the first
// `late[plane]` requests for it got the router's own 502.
type lateRouter struct {
	late map[string]int32
	seen map[string]*atomic.Int32
}

func newLateRouter(late map[string]int32) *lateRouter {
	router := &lateRouter{late: late, seen: map[string]*atomic.Int32{}}
	for _, probe := range planeProbes {
		router.seen[probe.plane] = &atomic.Int32{}
	}
	return router
}

func (l *lateRouter) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	for _, probe := range planeProbes {
		if r.URL.Path != probe.path {
			continue
		}
		late := l.late[probe.plane]
		if n := l.seen[probe.plane].Add(1); late < 0 || n <= late {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		goStub(probe.plane).ServeHTTP(w, r)
		return
	}
	w.WriteHeader(http.StatusNotFound)
}

// The second half of CHAOS-8542: the wait returned when go-api answered, while
// the router still gave 502 for query-api. It now returns only when a request
// for EACH plane was answered by that plane.
func TestWaitForPlanes_WaitsForEveryPlaneNotForTheFirst(t *testing.T) {
	router := newLateRouter(map[string]int32{PlaneGoAPI: 0, PlaneQueryAPI: 3})
	server := httptest.NewServer(router)
	t.Cleanup(server.Close)

	waitForPlanes(t, server.URL, 30*time.Second, diagnoseSentinel)
	for _, probe := range planeProbes {
		response, err := http.Get(server.URL + probe.path)
		if err != nil {
			t.Fatal(err)
		}
		_ = response.Body.Close()
		if response.StatusCode != http.StatusOK || response.Header.Get(stubPlane) != probe.plane {
			t.Errorf("after the wait, GET %s = %d (%s %q), want 200 from %s", probe.path, response.StatusCode, stubPlane, response.Header.Get(stubPlane), probe.plane)
		}
	}
	if got := router.seen[PlaneQueryAPI].Load(); got != 5 {
		t.Errorf("%d requests for the query api, want 5 (three 502, the answer, the check above)", got)
	}
}

// Bounded and loud, and it names the plane that did not answer.
func TestWaitForPlanes_NamesThePlaneThatDidNotAnswer(t *testing.T) {
	for _, missing := range Planes {
		late := map[string]int32{PlaneGoAPI: 0, PlaneQueryAPI: 0}
		late[missing] = -1
		server := httptest.NewServer(newLateRouter(late))
		said := whatItSaysWhenItGivesUp(t, func(tb testing.TB) { waitForPlanes(tb, server.URL, 700*time.Millisecond, diagnoseSentinel) })
		server.Close()
		for _, want := range []string{"the " + missing + " stub plane did not answer", "within 700ms", "last: status 502", sentinelLog} {
			if !strings.Contains(said, want) {
				t.Errorf("%s missing: it said %q, want %q in it", missing, said, want)
			}
		}
	}
}

// An answer from the OTHER plane is not the answer: a router that sends the
// query api's path to the Go api must not pass the wait.
func TestWaitForPlanes_AnAnswerFromTheOtherPlaneIsNotTheAnswer(t *testing.T) {
	server := httptest.NewServer(goStub(PlaneGoAPI))
	t.Cleanup(server.Close)
	said := whatItSaysWhenItGivesUp(t, func(tb testing.TB) { waitForPlanes(tb, server.URL, 700*time.Millisecond, diagnoseSentinel) })
	for _, want := range []string{"the " + PlaneQueryAPI + " stub plane did not answer", "last: status 200, " + stubPlane + ` "` + PlaneGoAPI + `"`, sentinelLog} {
		if !strings.Contains(said, want) {
			t.Errorf("it said %q, want %q in it", said, want)
		}
	}
}

// One probe per plane, no plane left out.
func TestPlaneProbesNameEveryPlane(t *testing.T) {
	var probed []string
	for _, probe := range planeProbes {
		probed = append(probed, probe.plane)
	}
	if strings.Join(probed, ",") != strings.Join(Planes, ",") {
		t.Fatalf("the probes name %v, the planes are %v", probed, Planes)
	}
}

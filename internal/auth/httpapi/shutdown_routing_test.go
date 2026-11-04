package httpapi

import (
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

// routingCell is one request path that ends in the mux's own answer or in a
// wrapper in front of it instead of in a route handler: the cases a guard
// inside the route chain never sees. live is what the cell answers before the
// drain deadline, so a cell cannot pass by missing its path.
type routingCell struct {
	name, server, method, target string
	live                         int
}

var routingCells = []routingCell{
	{"known route", "plain", "GET", "/known", 200},
	{"explicit HEAD refusal", "plain", "HEAD", "/known", 405},
	{"known path, wrong method", "plain", "POST", "/known", 405},
	{"unknown path", "plain", "GET", "/nothing", 404},
	{"strict-path refusal", "plain", "GET", "/a/../known/../nothing", 404},
	{"slash redirect", "plain", "GET", "/known/", 307},
	{"catch-all method, unknown path", "catchall", "GET", "/nothing", 404},
	{"catch-all refusal of a method", "catchall", "TRACE", "/nothing", 405},
}

// routingServers builds the two servers the cells run on. Every route handler
// counts its calls; the middleware, when set, runs before the routing.
func routingServers(t *testing.T, calls *atomic.Int32, middleware func(http.Handler) http.Handler) map[string]*Server {
	t.Helper()
	counted := http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	build := func(options ServerOptions) *Server {
		options.StrictPaths = true
		options.ExplicitHead = true
		if middleware != nil {
			options.Middleware = []func(http.Handler) http.Handler{middleware}
		}
		server, err := NewServer(options)
		if err != nil {
			t.Fatalf("NewServer: %v", err)
		}
		return server
	}
	plain := testOptions(Route{Method: http.MethodGet, Pattern: "/known", Handler: counted})
	plain.RedirectSlashes = true
	catchall := testOptions(Route{Method: http.MethodGet, Pattern: "/known", Handler: counted})
	catchall.CatchAllMethods = []string{"GET", "HEAD"}
	return map[string]*Server{"plain": build(plain), "catchall": build(catchall)}
}

func requireDrainAnswer(t *testing.T, cell routingCell, rec *httptest.ResponseRecorder) {
	t.Helper()
	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("%s: answered %d after the drain deadline, want 503 (Allow %q, Location %q)",
			cell.name, rec.Code, rec.Header().Get("Allow"), rec.Header().Get("Location"))
		return
	}
	if got := rec.Header().Get("Retry-After"); got != "1" {
		t.Errorf("%s: Retry-After = %q, want 1", cell.name, got)
	}
	if got := rec.Header().Get("Connection"); got != "close" {
		t.Errorf("%s: Connection = %q, want close", cell.name, got)
	}
	if rec.Header().Get(RequestIDHeader) == "" {
		t.Errorf("%s: the 503 carries no request id", cell.name)
	}
}

// Before the drain deadline every cell answers as routed, so the cells below
// reach the path they name.
func TestRoutingCellsAnswerAsRoutedBeforeTheDrainDeadline(t *testing.T) {
	var calls atomic.Int32
	servers := routingServers(t, &calls, nil)
	for _, cell := range routingCells {
		rec := httptest.NewRecorder()
		servers[cell.server].Handler().ServeHTTP(rec, httptest.NewRequest(cell.method, cell.target, nil))
		if rec.Code != cell.live {
			t.Errorf("%s: answered %d before the drain deadline, want %d", cell.name, rec.Code, cell.live)
		}
	}
}

// A request that arrives after the drain deadline answered the others is
// answered 503 on every routing path -- a refusal, a fallback or a redirect as
// much as a route -- and nothing behind the guard runs.
func TestEveryRoutingPathAnswersTheDrain503AfterTheDeadline(t *testing.T) {
	var calls atomic.Int32
	servers := routingServers(t, &calls, nil)
	for _, server := range servers {
		server.answers.answerAll()
	}
	for _, cell := range routingCells {
		rec := httptest.NewRecorder()
		servers[cell.server].Handler().ServeHTTP(rec, httptest.NewRequest(cell.method, cell.target, nil))
		requireDrainAnswer(t, cell, rec)
	}
	if got := calls.Load(); got != 0 {
		t.Errorf("route handlers ran %d times after the drain deadline, want 0", got)
	}
}

// A request already inside the composed handler (held in middleware) when the
// drain deadline passes is answered 503 whichever routing path it was bound for.
func TestEveryRoutingPathAnswersTheDrain503WhenHeldInMiddlewareAtTheDeadline(t *testing.T) {
	var calls atomic.Int32
	entered := make(chan struct{})
	gate := make(chan struct{})
	hold := func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			entered <- struct{}{}
			<-gate
			next.ServeHTTP(w, r)
		})
	}
	for _, cell := range routingCells {
		server := routingServers(t, &calls, hold)[cell.server]
		rec := httptest.NewRecorder()
		done := make(chan struct{})
		go func() {
			defer close(done)
			server.Handler().ServeHTTP(rec, httptest.NewRequest(cell.method, cell.target, nil))
		}()
		select {
		case <-entered:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s: the request never reached the middleware", cell.name)
		}
		server.answers.answerAll()
		gate <- struct{}{}
		<-done
		requireDrainAnswer(t, cell, rec)
	}
}

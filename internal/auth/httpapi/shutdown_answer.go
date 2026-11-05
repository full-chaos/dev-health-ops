package httpapi

import (
	"errors"
	"maps"
	"net/http"
	"sync"
	"time"
)

// answerWriteTimeout bounds the write of one 503 at the drain deadline, so a
// client that does not read cannot hold the shutdown.
const answerWriteTimeout = time.Second

// errAnsweredAtShutdown is what a handler's write returns once Shutdown has
// answered its request 503 at the drain deadline.
var errAnsweredAtShutdown = errors.New("httpapi: the request was answered 503 at the shutdown deadline")

// shutdownAnswers is the set of requests inside a route chain that have not
// answered yet. When the drain budget runs out with a request still in its
// handler (a slow host, a password compare that ignores its context), Shutdown
// answers each one 503 with Retry-After: 1 before it closes the connections, so
// the client gets a clean, retryable answer instead of a dropped connection
// (CHAOS-12827). A request that has already sent its header keeps its own answer.
type shutdownAnswers struct {
	mu      sync.Mutex
	pending map[*answerGuard]struct{}
	// closed is set once answerAll ran: a request that reaches a route chain
	// after it is answered 503 at once and its handler does not run.
	closed bool
}

func newShutdownAnswers() *shutdownAnswers {
	return &shutdownAnswers{pending: make(map[*answerGuard]struct{})}
}

// guard wraps one route chain. A nil set is no guard.
func (a *shutdownAnswers) guard(next http.Handler) http.Handler {
	if a == nil {
		return next
	}
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		g := &answerGuard{w: w, header: w.Header().Clone()}
		a.mu.Lock()
		closed := a.closed
		if !closed {
			a.pending[g] = struct{}{}
		}
		a.mu.Unlock()
		if closed {
			g.answerUnavailable()
			return
		}
		defer func() {
			g.finish()
			a.mu.Lock()
			delete(a.pending, g)
			a.mu.Unlock()
		}()
		next.ServeHTTP(g, r)
	})
}

// answerAll answers 503 every pending request that has not sent its header, and
// every request that arrives later. It returns once each answer is written. It
// is not cut short by the shutdown deadline: Shutdown closes the connections
// right after, and an answer still in the buffer then would be lost. A client
// that does not read cannot hold it either: each answer has its own short write
// deadline (answerWriteTimeout).
func (a *shutdownAnswers) answerAll() {
	a.mu.Lock()
	a.closed = true
	guards := make([]*answerGuard, 0, len(a.pending))
	for g := range a.pending {
		guards = append(guards, g)
	}
	a.mu.Unlock()

	var wg sync.WaitGroup
	for _, g := range guards {
		wg.Go(g.answerUnavailable)
	}
	wg.Wait()
}

// answerGuard is the ResponseWriter a route chain sees. The handler's goroutine
// and Shutdown's both may answer the request; mu makes the first answer the only
// one. Until the handler answers, it works on a header map of its own (a copy of
// the header the outer middleware set), so Shutdown's 503 never touches a map the
// handler may be writing. Once the handler has answered, only its goroutine uses
// the writer, and its writes run without the lock.
type answerGuard struct {
	mu     sync.Mutex
	w      http.ResponseWriter
	header http.Header
	// answered: a final header was sent, by the handler or by the guard.
	answered bool
	// preempted: the guard sent the 503; the handler's writes are refused.
	preempted bool
	// done: the handler returned; net/http owns the writer again.
	done bool
}

func (g *answerGuard) Header() http.Header {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.answered && !g.preempted {
		return g.w.Header()
	}
	return g.header
}

// takeLocked moves the handler's header into the real one before its first
// answer. It reports false when the guard has answered the request already.
func (g *answerGuard) takeLocked() bool {
	if g.preempted {
		return false
	}
	if !g.answered {
		real := g.w.Header()
		clear(real)
		maps.Copy(real, g.header)
	}
	return true
}

func (g *answerGuard) WriteHeader(status int) {
	g.mu.Lock()
	if !g.takeLocked() {
		g.mu.Unlock()
		return
	}
	// An informational header (1xx other than 101) is not the answer: the
	// handler still owes one, and Shutdown can still give it.
	if status >= 200 || status == http.StatusSwitchingProtocols {
		g.answered = true
	}
	g.w.WriteHeader(status)
	g.mu.Unlock()
}

func (g *answerGuard) Write(data []byte) (int, error) {
	g.mu.Lock()
	if !g.takeLocked() {
		g.mu.Unlock()
		return 0, errAnsweredAtShutdown
	}
	if !g.answered {
		g.answered = true
		g.w.WriteHeader(http.StatusOK)
	}
	g.mu.Unlock()
	return g.w.Write(data)
}

// Flush passes a flush through to the connection (through writers that only
// Unwrap, such as the outer Recover's); a flush before any header sends the 200,
// as net/http does.
func (g *answerGuard) Flush() {
	g.mu.Lock()
	if !g.takeLocked() {
		g.mu.Unlock()
		return
	}
	if !g.answered {
		g.answered = true
		g.w.WriteHeader(http.StatusOK)
	}
	g.mu.Unlock()
	_ = http.NewResponseController(g.w).Flush()
}

// Unwrap lets http.ResponseController reach the connection (a write deadline)
// through the guard. Its Flush stops at the guard's own Flush.
func (g *answerGuard) Unwrap() http.ResponseWriter { return g.w }

// finish runs on the handler's goroutine when the handler returns. Taking the
// lock orders every write Shutdown made before net/http finishes the response.
func (g *answerGuard) finish() {
	g.mu.Lock()
	g.done = true
	g.mu.Unlock()
}

// answerUnavailable sends the 503 unless the request has answered or ended. The
// header the outer middleware set (the request id) stays; the body is empty:
// transport-error.v1 has no 503 code, and every client reads a 503 with
// Retry-After as retry later.
func (g *answerGuard) answerUnavailable() {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.answered || g.done {
		return
	}
	g.answered, g.preempted = true, true
	header := g.w.Header()
	header.Set("Retry-After", "1")
	header.Set("Cache-Control", "no-store")
	header.Set("Connection", "close")
	header.Set("Content-Length", "0")
	g.w.WriteHeader(http.StatusServiceUnavailable)
	// The flush must reach the socket before Shutdown closes it, or the answer
	// is lost in the buffer: ResponseController follows Unwrap to the connection.
	controller := http.NewResponseController(g.w)
	_ = controller.SetWriteDeadline(time.Now().Add(answerWriteTimeout))
	_ = controller.Flush()
}

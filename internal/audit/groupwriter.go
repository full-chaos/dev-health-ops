package audit

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const (
	defaultGroupFlushers = 2
	defaultGroupMaxRows  = 512
	defaultGroupTimeout  = 5 * time.Second
)

// GroupWriterConfig configures a GroupWriter. Zero values take the defaults.
type GroupWriterConfig struct {
	// Begin opens a write transaction (pgxpool.Pool.Begin).
	Begin  func(context.Context) (pgx.Tx, error)
	Schema authschema.ValidatedIdentifier
	// Flushers is the most transactions in flight at once.
	Flushers int
	// MaxRows bounds the rows of one transaction; a call is never split.
	MaxRows int
	// Timeout bounds one transaction.
	Timeout time.Duration
}

// GroupWriter writes the rows of append-only event types with AppendAll's
// contract per call: every row of a call or none, and a call returns only
// after the commit that holds its rows.
type GroupWriter struct {
	begin    func(context.Context) (pgx.Tx, error)
	schema   authschema.ValidatedIdentifier
	flushers int
	maxRows  int
	timeout  time.Duration

	mu      sync.Mutex
	pending []*groupCall
	active  int
}

type groupCall struct {
	ctx    context.Context
	events []AuditEvent
	done   chan error
	lead   chan struct{}
}

// NewGroupWriter builds a writer over cfg.Begin.
func NewGroupWriter(cfg GroupWriterConfig) (*GroupWriter, error) {
	if cfg.Begin == nil {
		return nil, errors.New("audit: a group writer needs a transaction opener")
	}
	w := &GroupWriter{begin: cfg.Begin, schema: cfg.Schema, flushers: cfg.Flushers, maxRows: cfg.MaxRows, timeout: cfg.Timeout}
	if w.flushers <= 0 {
		w.flushers = defaultGroupFlushers
	}
	if w.maxRows <= 0 {
		w.maxRows = defaultGroupMaxRows
	}
	if w.timeout <= 0 {
		w.timeout = defaultGroupTimeout
	}
	return w, nil
}

// Append writes events and returns once they are committed, or the error that
// kept them out.
func (w *GroupWriter) Append(ctx context.Context, events ...AuditEvent) (err error) {
	defer func() {
		if err != nil {
			slog.ErrorContext(ctx, "audit: group append failed; the call's rows are not confirmed",
				"audit_rows", len(events), logging.ErrorAttr(err))
		}
	}()
	if err := validateAppendOnly(events); err != nil {
		return err
	}
	c := &groupCall{ctx: ctx, events: events, done: make(chan error, 1), lead: make(chan struct{}, 1)}
	w.mu.Lock()
	w.pending = append(w.pending, c)
	if w.active < w.flushers {
		w.active++
		c.lead <- struct{}{}
	}
	w.mu.Unlock()
	for {
		select {
		case err := <-c.done:
			return err
		case <-c.lead:
			w.flush(ctx)
		case <-ctx.Done():
			w.abandon(c)
			return ctx.Err()
		}
	}
}

// flush is run by a call that holds a flusher slot: it writes the oldest
// waiting calls in one transaction, then hands the slot to a waiting call or
// gives it up. The transaction outlives the leading call's cancellation, as
// it holds other calls' rows, but not w.timeout.
func (w *GroupWriter) flush(ctx context.Context) {
	w.mu.Lock()
	group := w.take()
	w.mu.Unlock()
	if len(group) > 0 {
		wctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), w.timeout)
		w.write(wctx, group)
		cancel()
	}
	w.mu.Lock()
	w.passLead()
	w.mu.Unlock()
}

// take removes the oldest waiting calls, up to maxRows rows (a first call
// above it alone). A waiting call whose context has ended is removed and never
// taken: its caller returns the context error. A removed call that was handed
// a slot it has not started gives it up. w.mu is held.
func (w *GroupWriter) take() []*groupCall {
	var group []*groupCall
	rows, n := 0, 0
	for ; n < len(w.pending); n++ {
		c := w.pending[n]
		if c.ctx.Err() == nil {
			if len(group) > 0 && rows+len(c.events) > w.maxRows {
				break
			}
			rows += len(c.events)
			group = append(group, c)
		}
		select {
		case <-c.lead:
			w.active--
		default:
		}
	}
	w.pending = append(w.pending[:0], w.pending[n:]...)
	return group
}

// passLead hands the caller's slot to a waiting call that holds none, or gives
// it up. w.mu is held.
func (w *GroupWriter) passLead() {
	for _, c := range w.pending {
		if len(c.lead) == 0 {
			c.lead <- struct{}{}
			return
		}
	}
	w.active--
}

// abandon removes a call whose context ended while it waited. A call already
// taken is in a transaction and may still be committed; its caller is answered
// with the context error either way.
func (w *GroupWriter) abandon(c *groupCall) {
	w.mu.Lock()
	defer w.mu.Unlock()
	for i, p := range w.pending {
		if p != c {
			continue
		}
		w.pending = append(w.pending[:i], w.pending[i+1:]...)
		select {
		case <-c.lead:
			w.passLead()
		default:
		}
		return
	}
}

// write commits the group in one transaction. When that fails, each call is
// written alone, so one call's refused row fails only that call.
func (w *GroupWriter) write(ctx context.Context, group []*groupCall) {
	var events []AuditEvent
	for _, c := range group {
		events = append(events, c.events...)
	}
	err := appendTx(ctx, w.begin, w.schema, events)
	if err == nil || len(group) == 1 {
		for _, c := range group {
			c.done <- err
		}
		return
	}
	for _, c := range group {
		c.done <- appendTx(ctx, w.begin, w.schema, c.events)
	}
}

func (w *GroupWriter) queued() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending)
}

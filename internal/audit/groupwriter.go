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
	return appendTx(ctx, w.begin, w.schema, events)
}

func (w *GroupWriter) queued() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.pending)
}

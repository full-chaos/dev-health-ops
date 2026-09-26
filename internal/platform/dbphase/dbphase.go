// Package dbphase records WHICH database phase consumed a bounded call's
// budget: waiting for a pool connection, opening the transaction, or a named
// statement (CHAOS-6936).
//
// A reconciler stage that fails at exactly its budget with an empty step told
// the operator only that the budget was spent. Whether the time went to a
// starved pool (a queue of work behind two connections) or to one slow
// statement (a catalog scan) needs opposite fixes, and the two were
// indistinguishable in the log.
//
// The recorder travels on the context. The pgx tracers attached to each pool
// (internal/storage/postgres) write into whichever Trace the call's context
// carries, so no stage has to be edited to be observed: every Acquire, BEGIN,
// statement and COMMIT issued with a derived context lands in the Trace.
//
// It never records statement arguments. A phase name is the statement's first
// comment line or its first characters, whitespace collapsed.
package dbphase

import (
	"context"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Kind is the closed vocabulary of phase kinds.
type Kind string

const (
	// KindAcquire is the wait for a pool connection.
	KindAcquire Kind = "acquire"
	// KindStatement is one statement round trip, including BEGIN and COMMIT,
	// which pgx issues as statements.
	KindStatement Kind = "statement"
)

// maxPhases bounds a Trace so a long-lived context cannot grow it without
// limit. Phases past the bound still count toward the acquire totals.
const maxPhases = 64

// maxNameRunes bounds a statement phase name.
const maxNameRunes = 80

// Phase is one recorded phase. Elapsed is final once Done is true.
type Phase struct {
	Kind    Kind
	Name    string
	Pool    string
	Elapsed time.Duration
	Done    bool
	Err     error
}

type traceKey struct{}

// Trace is safe for concurrent use.
type Trace struct {
	mu      sync.Mutex
	phases  []*phase
	dropped int
	// acquireWaits keeps every finished acquire wait, beyond maxPhases.
	acquireWaits []time.Duration
}

type phase struct {
	kind  Kind
	name  string
	pool  string
	start time.Time
	done  bool
	err   error
	took  time.Duration
}

// With returns a context carrying a fresh Trace, and that Trace.
func With(ctx context.Context) (context.Context, *Trace) {
	trace := &Trace{}
	return context.WithValue(ctx, traceKey{}, trace), trace
}

// From returns the Trace a context carries, or nil.
func From(ctx context.Context) *Trace {
	trace, _ := ctx.Value(traceKey{}).(*Trace)
	return trace
}

// Handle ends one started phase.
type Handle struct {
	trace *Trace
	p     *phase
}

// Start begins a phase on the Trace the context carries and returns a Handle
// to end it. A context with no Trace yields a Handle whose End is a no-op.
func Start(ctx context.Context, kind Kind, pool, name string) Handle {
	trace := From(ctx)
	if trace == nil {
		return Handle{}
	}
	entry := &phase{kind: kind, pool: pool, name: SummarizeStatement(name), start: time.Now()}
	if kind == KindAcquire {
		entry.name = pool
	}
	trace.mu.Lock()
	if len(trace.phases) < maxPhases {
		trace.phases = append(trace.phases, entry)
	} else {
		trace.dropped++
	}
	trace.mu.Unlock()
	return Handle{trace: trace, p: entry}
}

// End finishes the phase with the error the operation returned, if any.
func (handle Handle) End(err error) {
	if handle.trace == nil || handle.p == nil {
		return
	}
	took := time.Since(handle.p.start)
	handle.trace.mu.Lock()
	handle.p.done = true
	handle.p.err = err
	handle.p.took = took
	if handle.p.kind == KindAcquire {
		handle.trace.acquireWaits = append(handle.trace.acquireWaits, took)
	}
	handle.trace.mu.Unlock()
}

// Phases returns a snapshot in start order. An unfinished phase reports the
// time elapsed so far.
func (trace *Trace) Phases() []Phase {
	if trace == nil {
		return nil
	}
	now := time.Now()
	trace.mu.Lock()
	defer trace.mu.Unlock()
	out := make([]Phase, 0, len(trace.phases))
	for _, entry := range trace.phases {
		elapsed := entry.took
		if !entry.done {
			elapsed = now.Sub(entry.start)
		}
		out = append(out, Phase{
			Kind: entry.kind, Name: entry.name, Pool: entry.pool,
			Elapsed: elapsed, Done: entry.done, Err: entry.err,
		})
	}
	return out
}

// AcquireWaits returns every finished acquire wait, oldest first. An acquire
// still in flight is included at its elapsed time so far: a pool that never
// answers must show as a long wait, not as no wait.
func (trace *Trace) AcquireWaits() []time.Duration {
	if trace == nil {
		return nil
	}
	now := time.Now()
	trace.mu.Lock()
	defer trace.mu.Unlock()
	out := append([]time.Duration(nil), trace.acquireWaits...)
	for _, entry := range trace.phases {
		if entry.kind == KindAcquire && !entry.done {
			out = append(out, now.Sub(entry.start))
		}
	}
	return out
}

// Culprit names the phase that consumed the budget of a failed call: the
// phase still in flight when the call returned, else the last phase that
// ended in an error, else the longest phase. ok is false when nothing ran.
func (trace *Trace) Culprit() (Phase, bool) {
	phases := trace.Phases()
	if len(phases) == 0 {
		return Phase{}, false
	}
	for _, candidate := range phases {
		if !candidate.Done {
			return candidate, true
		}
	}
	for index := len(phases) - 1; index >= 0; index-- {
		if phases[index].Err != nil {
			return phases[index], true
		}
	}
	longest := phases[0]
	for _, candidate := range phases[1:] {
		if candidate.Elapsed > longest.Elapsed {
			longest = candidate
		}
	}
	return longest, true
}

// Summary renders the phases as "kind:name=<ms>ms[!]" joined by ";", `!`
// marking a phase that ended in an error and `?` one still in flight.
func (trace *Trace) Summary() string {
	phases := trace.Phases()
	if len(phases) == 0 {
		return ""
	}
	var builder strings.Builder
	for index, entry := range phases {
		if index > 0 {
			builder.WriteByte(';')
		}
		builder.WriteString(string(entry.Kind))
		builder.WriteByte(':')
		builder.WriteString(entry.Name)
		builder.WriteByte('=')
		builder.WriteString(formatMillis(entry.Elapsed))
		switch {
		case !entry.Done:
			builder.WriteByte('?')
		case entry.Err != nil:
			builder.WriteByte('!')
		}
	}
	trace.mu.Lock()
	dropped := trace.dropped
	trace.mu.Unlock()
	if dropped > 0 {
		builder.WriteString(";+")
		builder.WriteString(strconv.Itoa(dropped))
		builder.WriteString("more")
	}
	return builder.String()
}

func formatMillis(duration time.Duration) string {
	return strconv.Itoa(int(duration.Milliseconds())) + "ms"
}

// SummarizeStatement returns the statement's first comment line when it has
// one, else its first characters, whitespace collapsed, bounded. It never
// includes arguments: pgx passes them separately.
func SummarizeStatement(statement string) string {
	trimmed := strings.TrimSpace(statement)
	if trimmed == "" {
		return "unnamed"
	}
	if strings.HasPrefix(trimmed, "--") {
		line, _, _ := strings.Cut(trimmed, "\n")
		trimmed = strings.TrimSpace(strings.TrimLeft(line, "-"))
		if trimmed == "" {
			return "unnamed"
		}
	}
	collapsed := strings.Join(strings.Fields(trimmed), " ")
	runes := []rune(collapsed)
	if len(runes) > maxNameRunes {
		runes = runes[:maxNameRunes]
	}
	return strings.NewReplacer(";", ",", "=", ":").Replace(string(runes))
}

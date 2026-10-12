// Package dbspan emits slow-call database client spans (CHAOS-9364) for the
// ClickHouse and Postgres drivers, so a trace backend that keys its database
// views on db.system has something to show.
//
// Contract (every line is pinned by a test):
//
//   - A span is created only when the call context carries a span that is
//     recording. A call with no parent, or a parent the sampler dropped, costs
//     one context lookup and creates nothing. Sampling is never changed and a
//     database call never becomes a new trace root.
//   - A span is emitted only when the call took at least the threshold
//     (OTEL_DB_SPAN_MIN_MS, default 50, 0 = every call) or the call failed.
//     The span is created at the END of the call with explicit start and end
//     timestamps, so a fast call allocates no span.
//   - The statement text, the arguments and the error text are NEVER recorded:
//     driver error texts can embed values. The span carries a bounded
//     operation, a "<op> <table>" summary parsed with a strict identifier
//     pattern, a hash of the statement, and a bounded error class.
//
// Error classing: context.Canceled is the caller walking away, not a database
// fault. It is emitted only when slow (like a success), without an error
// status, with error.type "canceled". context.DeadlineExceeded and every other
// error are emitted regardless of duration, with an error status.
// sql.ErrNoRows / pgx.ErrNoRows are an empty result, not a failure: they are
// treated as success.
package dbspan

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"hash/fnv"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/trace"
)

const (
	// EnvMinMS names the threshold variable, in milliseconds.
	EnvMinMS = "OTEL_DB_SPAN_MIN_MS"
	// DefaultMin is the threshold when the variable is unset or invalid.
	DefaultMin = 50 * time.Millisecond

	SystemPostgres   = "postgresql"
	SystemClickHouse = "clickhouse"

	// OpBatch is the operation name of a pgx batch or a ClickHouse
	// PrepareBatch..Send insert.
	OpBatch = "BATCH"

	tracerName = "github.com/full-chaos/dev-health-ops/internal/platform/dbspan"
)

var (
	envMinOnce sync.Once
	envMin     time.Duration
)

// ParseMin parses a threshold value in milliseconds. ok is false for an
// empty, non-numeric or negative value.
func ParseMin(raw string) (d time.Duration, ok bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return DefaultMin, true
	}
	ms, err := strconv.ParseInt(raw, 10, 64)
	if err != nil || ms < 0 || ms > int64(24*time.Hour/time.Millisecond) {
		return DefaultMin, false
	}
	return time.Duration(ms) * time.Millisecond, true
}

// EnvMin is the process-wide threshold, read once. An invalid value falls back
// to DefaultMin. EnvMin never logs (binaries classified silent import this
// package); the process's tracing initialisation checks InvalidEnv once.
func EnvMin() time.Duration {
	envMinOnce.Do(func() {
		envMin, _ = ParseMin(os.Getenv(EnvMinMS))
	})
	return envMin
}

// InvalidEnv reports whether OTEL_DB_SPAN_MIN_MS is set to an invalid value
// (the default applies). The process's tracing initialisation logs ONE warning
// when it is true; this package never logs.
func InvalidEnv() bool {
	_, ok := ParseMin(os.Getenv(EnvMinMS))
	return !ok
}

// Emitter emits the spans of one driver (and, for Postgres, one pool).
type Emitter struct {
	system string
	pool   string
	min    time.Duration
}

// NewEmitter returns an emitter with the process-wide threshold. pool may be
// empty (omitted from the span).
func NewEmitter(system, pool string) Emitter {
	return Emitter{system: system, pool: pool, min: EnvMin()}
}

// WithMin returns a copy with an explicit threshold (tests).
func (e Emitter) WithMin(d time.Duration) Emitter {
	e.min = d
	return e
}

// Probe is the start of one database call. The zero value is inactive.
type Probe struct {
	e      Emitter
	start  time.Time
	active bool
}

// Start begins a call. It is active only when ctx carries a recording span.
func (e Emitter) Start(ctx context.Context) Probe {
	if ctx == nil || !trace.SpanFromContext(ctx).IsRecording() {
		return Probe{}
	}
	return Probe{e: e, start: time.Now(), active: true}
}

// Active reports whether the call is being measured.
func (p Probe) Active() bool { return p.active }

// Finish ends the call and emits the span when the contract says so. op, when
// non-empty, overrides the operation parsed from stmt (OpBatch). errClass is
// the bounded class of err, from ErrorClass or a driver-specific classifier.
func (p Probe) Finish(ctx context.Context, stmt, op string, err error, errClass string) {
	if !p.active {
		return
	}
	end := time.Now()
	if errors.Is(err, sql.ErrNoRows) || errors.Is(err, pgx.ErrNoRows) {
		err, errClass = nil, ""
	}
	canceled := errors.Is(err, context.Canceled)
	failed := err != nil && !canceled
	if !failed && end.Sub(p.start) < p.e.min {
		return
	}
	parent := trace.SpanFromContext(ctx)
	if !parent.SpanContext().IsValid() {
		return
	}
	if op == "" {
		op = Operation(stmt)
	}
	summary := Summary(stmt, op)
	attrs := make([]attribute.KeyValue, 0, 7)
	attrs = append(attrs,
		attribute.String("db.system", p.e.system),
		attribute.String("db.system.name", p.e.system),
		attribute.String("db.operation.name", op),
		attribute.String("db.query.summary", summary),
		attribute.String("db.query.fingerprint", Fingerprint(stmt)),
	)
	if p.e.pool != "" {
		attrs = append(attrs, attribute.String("dh.db.pool", p.e.pool))
	}
	if err != nil {
		if errClass == "" {
			errClass = ErrorClass(err)
		}
		attrs = append(attrs, attribute.String("error.type", errClass))
	}
	_, span := parent.TracerProvider().Tracer(tracerName).Start(ctx, summary,
		trace.WithSpanKind(trace.SpanKindClient),
		trace.WithTimestamp(p.start),
		trace.WithAttributes(attrs...),
	)
	if failed {
		// The description is empty on purpose: error text can carry values.
		span.SetStatus(codes.Error, "")
	}
	span.End(trace.WithTimestamp(end))
}

// ErrorClass maps an error to a bounded, value-free class.
func ErrorClass(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return "canceled"
	case errors.Is(err, context.DeadlineExceeded):
		return "deadline_exceeded"
	default:
		return "driver_error"
	}
}

// Fingerprint identifies the statement TEMPLATE: the first 16 hex characters of
// the FNV-1a 64-bit hash of the statement with every string and numeric
// literal replaced by "?". Hashing the raw text would let anyone with the
// trace guess a low-entropy literal against known query templates, so the
// literals never enter the hash; the fingerprint also groups the same query
// shape across different literal values. It is a grouping key, not a secret
// (a non-cryptographic hash on purpose).
func Fingerprint(stmt string) string {
	h := fnv.New64a()
	_, _ = h.Write(normalize(stmt))
	var sum [8]byte
	return hex.EncodeToString(h.Sum(sum[:0]))
}

// normalize drops comments (their text can hold anything), replaces
// TRUE/FALSE/NULL words, single-quoted strings (with ” and backslash escapes),
// dollar-quoted bodies and numeric literals by "?". Identifiers that contain
// digits are left alone. An unterminated quote swallows the rest of the text.
func normalize(stmt string) []byte {
	in := stmt
	out := make([]byte, 0, len(in))
	for i := 0; i < len(in); {
		c := in[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			if len(out) > 0 && out[len(out)-1] != ' ' {
				out = append(out, ' ')
			}
			i++
		case c == '-' && i+1 < len(in) && in[i+1] == '-':
			for i < len(in) && in[i] != '\n' {
				i++
			}
			if len(out) > 0 && out[len(out)-1] != ' ' {
				out = append(out, ' ')
			}
		case c == '/' && i+1 < len(in) && in[i+1] == '*':
			end := strings.Index(in[i+2:], "*/")
			if end < 0 {
				i = len(in)
			} else {
				i += 2 + end + 2
			}
			if len(out) > 0 && out[len(out)-1] != ' ' {
				out = append(out, ' ')
			}
		case isIdentStart(c) && (len(out) == 0 || !isIdentPart(out[len(out)-1])):
			j := i
			for j < len(in) && isIdentPart(in[j]) {
				j++
			}
			if w := in[i:j]; strings.EqualFold(w, "true") || strings.EqualFold(w, "false") || strings.EqualFold(w, "null") {
				out = append(out, '?')
			} else {
				out = append(out, w...)
			}
			i = j
		case c == '\'':
			i++
			for i < len(in) {
				if in[i] == '\\' && i+1 < len(in) {
					i += 2
					continue
				}
				if in[i] == '\'' {
					if i+1 < len(in) && in[i+1] == '\'' {
						i += 2
						continue
					}
					i++
					break
				}
				i++
			}
			out = append(out, '?')
		case c == '$' && i+1 < len(in) && (in[i+1] == '$' || isIdentStart(in[i+1])):
			// $tag$ ... $tag$ (dollar quoting) or $$ ... $$; a parameter like $1
			// is not a quote and falls through to the default branch below.
			j := i + 1
			for j < len(in) && isIdentPart(in[j]) {
				j++
			}
			if j < len(in) && in[j] == '$' {
				tag := in[i : j+1]
				end := strings.Index(in[j+1:], tag)
				if end < 0 {
					i = len(in)
				} else {
					i = j + 1 + end + len(tag)
				}
				out = append(out, '?')
				continue
			}
			out = append(out, c)
			i++
		case c >= '0' && c <= '9' && (len(out) == 0 || !isIdentPart(out[len(out)-1])):
			for i < len(in) && (in[i] >= '0' && in[i] <= '9' || in[i] == '.' || in[i] == 'e' || in[i] == 'E' || in[i] == 'x' || in[i] == 'X' || (in[i] >= 'a' && in[i] <= 'f') || (in[i] >= 'A' && in[i] <= 'F')) {
				i++
			}
			out = append(out, '?')
		default:
			out = append(out, c)
			i++
		}
	}
	return out
}

func isIdentStart(c byte) bool {
	return c == '_' || c >= 'a' && c <= 'z' || c >= 'A' && c <= 'Z'
}

func isIdentPart(c byte) bool { return isIdentStart(c) || c >= '0' && c <= '9' }

var operations = map[string]struct{}{
	"SELECT": {}, "INSERT": {}, "UPDATE": {}, "DELETE": {}, "WITH": {}, "COPY": {},
	"ALTER": {}, "CREATE": {}, "DROP": {}, "TRUNCATE": {}, "SET": {}, "SHOW": {},
}

// maxScan bounds the statement prefix the parsers read.
const maxScan = 4096

// Operation is the closed-set operation of a statement: its first SQL keyword
// (leading whitespace and comments skipped) or OTHER.
func Operation(stmt string) string {
	if len(stmt) > maxScan {
		stmt = stmt[:maxScan]
	}
	word, _ := nextWord(stmt)
	up := strings.ToUpper(word)
	if _, ok := operations[up]; ok {
		return up
	}
	return "OTHER"
}

var identPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)?$`)

// Summary is "<op> <first table identifier>" where the identifier follows
// FROM, INTO, UPDATE or TABLE and matches a strict identifier pattern, else op
// alone.
func Summary(stmt, op string) string {
	if len(stmt) > maxScan {
		stmt = stmt[:maxScan]
	}
	rest := stmt
	for {
		var word string
		word, rest = nextWord(rest)
		if word == "" {
			return op
		}
		if strings.EqualFold(word, "FROM") || strings.EqualFold(word, "INTO") ||
			strings.EqualFold(word, "UPDATE") || strings.EqualFold(word, "TABLE") {
			// A subquery ("FROM (SELECT ...") is not a table.
			if trimmed := strings.TrimLeft(rest, " \t\r\n"); strings.HasPrefix(trimmed, "(") {
				return op
			}
			ident, _ := nextWord(rest)
			if identPattern.MatchString(ident) {
				return op + " " + ident
			}
			// First candidate decides: a subquery or a quoted name stays op-only.
			return op
		}
	}
}

// nextWord returns the next token of s and the remainder. Tokens split on
// whitespace, parentheses, commas and semicolons; "--" and "/* */" comments are
// skipped. A token that starts with a quote or a bracket is returned as is (it
// will not match the identifier pattern).
func nextWord(s string) (string, string) {
	i := 0
	for i < len(s) {
		switch c := s[i]; {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r' || c == '(' || c == ')' || c == ',' || c == ';':
			i++
		case c == '-' && i+1 < len(s) && s[i+1] == '-':
			for i < len(s) && s[i] != '\n' {
				i++
			}
		case c == '/' && i+1 < len(s) && s[i+1] == '*':
			end := strings.Index(s[i+2:], "*/")
			if end < 0 {
				return "", ""
			}
			i += 2 + end + 2
		default:
			j := i
			for j < len(s) {
				d := s[j]
				if d == ' ' || d == '\t' || d == '\n' || d == '\r' || d == '(' || d == ')' || d == ',' || d == ';' {
					break
				}
				j++
			}
			return s[i:j], s[j:]
		}
	}
	return "", ""
}

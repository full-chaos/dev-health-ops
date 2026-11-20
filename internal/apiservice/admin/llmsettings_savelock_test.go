package admin

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// reachedSaveWrite is the panic of a saveLockTx at the first statement after the save lock.
type reachedSaveWrite struct{}

// saveLockDB begins a saveLockTx.
type saveLockDB struct {
	pgDB
	tx *saveLockTx
}

func (d *saveLockDB) Begin(context.Context) (pgx.Tx, error) { return d.tx, nil }

// saveLockTx records the save lock statements; any other statement panics
// with reachedSaveWrite.
type saveLockTx struct {
	pgx.Tx
	statements []string
	args       [][]any
	lockErr    error
}

func (x *saveLockTx) Exec(_ context.Context, sql string, args ...any) (pgconn.CommandTag, error) {
	switch sql {
	case llmSettingsSaveLockTimeoutSQL, llmSettingsSaveLockSQL, llmSettingsSaveLockTimeoutRestoreSQL:
	default:
		panic(reachedSaveWrite{})
	}
	x.statements, x.args = append(x.statements, sql), append(x.args, args)
	if sql == llmSettingsSaveLockSQL && x.lockErr != nil {
		return pgconn.CommandTag{}, x.lockErr
	}
	return pgconn.NewCommandTag("SELECT 1"), nil
}

func (x *saveLockTx) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	if sql != llmSettingsSaveLockTimeoutReadSQL {
		panic(reachedSaveWrite{})
	}
	x.statements, x.args = append(x.statements, sql), append(x.args, args)
	return lockTimeoutRow("7s")
}

// lockTimeoutRow is the session's lock_timeout as current_setting reads it.
type lockTimeoutRow string

func (r lockTimeoutRow) Scan(dest ...any) error {
	*dest[0].(*string) = string(r)
	return nil
}

func (x *saveLockTx) Query(context.Context, string, ...any) (pgx.Rows, error) {
	panic(reachedSaveWrite{})
}

func (x *saveLockTx) Rollback(context.Context) error { return nil }

func saveLockTimeoutCounts(t *testing.T) map[string]int64 {
	t.Helper()
	deprovisionSkipCounts(t) // installs the one reader
	var collected metricdata.ResourceMetrics
	if err := deprovisionReader.Collect(context.Background(), &collected); err != nil {
		t.Fatal(err)
	}
	counts := map[string]int64{}
	for _, scope := range collected.ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != "devhealth_api_llm_settings_save_lock_timeout_total" {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				label, _ := point.Attributes.Value("scope")
				counts[label.AsString()] += point.Value
			}
		}
	}
	return counts
}

// Every save transaction, default and purpose, starts with the bounded save
// lock of the org and nothing else before it, and gives the session's own
// lock_timeout back right after it.
func TestLLMSettingsSaveTakesTheOrgSaveLockFirst(t *testing.T) {
	want := []string{llmSettingsSaveLockTimeoutReadSQL, llmSettingsSaveLockTimeoutSQL, llmSettingsSaveLockSQL, llmSettingsSaveLockTimeoutRestoreSQL}
	if llmSettingsSaveLockTimeoutSQL != `SET LOCAL lock_timeout = '2000ms'` {
		t.Fatalf("lock timeout statement %q", llmSettingsSaveLockTimeoutSQL)
	}
	org := uuid.MustParse("550e8400-e29b-41d4-a716-446655440000")
	for _, scope := range llmWriteScopes {
		tx := &saveLockTx{}
		h := baseURLHandlers(&resolverSpy{valid: true}, "", "")
		h.store.Pool = &saveLockDB{tx: tx}
		refusal, reached, err := keysetWriteAs(h, scope, org, strPtr("sk-fresh-"+"0123456789"), strPtr(readerCleanURL))
		if err != nil || refusal != nil || !reached {
			t.Fatalf("%s: refusal=%v reached write=%v err=%v", scopeName(scope), refusal, reached, err)
		}
		if !slices.Equal(tx.statements, want) {
			t.Fatalf("%s: statements %q, want %q", scopeName(scope), tx.statements, want)
		}
		if got := tx.args[2]; len(got) != 1 || got[0] != "llm-settings-save:550e8400-e29b-41d4-a716-446655440000" {
			t.Fatalf("%s: lock args %v", scopeName(scope), got)
		}
		if got := tx.args[3]; len(got) != 1 || got[0] != "7s" {
			t.Fatalf("%s: restore args %v, want the read value", scopeName(scope), got)
		}
	}
}

// Every spelling of one org id the access gate accepts (upper case, no hyphens,
// braces) takes the save lock of the canonical id, in every scope: the raw id
// the caller gave never reaches the lock key.
func TestLLMSettingsSaveLockKeyIsTheCanonicalOrgID(t *testing.T) {
	const canonical = "550e8400-e29b-41d4-a716-446655440000"
	for _, scope := range llmWriteScopes {
		for _, alias := range []string{"550E8400-E29B-41D4-A716-446655440000", "550e8400e29b41d4a716446655440000", "{550e8400-e29b-41d4-a716-446655440000}"} {
			org, err := pythonparity.ParseUUID(alias)
			if err != nil {
				t.Fatalf("%s: the access gate refuses %q: %v", scopeName(scope), alias, err)
			}
			tx := &saveLockTx{}
			h := baseURLHandlers(&resolverSpy{valid: true}, "", "")
			h.store.Pool = &saveLockDB{tx: tx}
			if refusal, reached, err := keysetWriteAs(h, scope, org, strPtr("sk-fresh-"+"0123456789"), strPtr(readerCleanURL)); err != nil || refusal != nil || !reached {
				t.Fatalf("%s %q: refusal=%v reached write=%v err=%v", scopeName(scope), alias, refusal, reached, err)
			}
			if got := tx.args[2]; len(got) != 1 || got[0] != "llm-settings-save:"+canonical {
				t.Errorf("%s %q: lock key %v, want the canonical %q", scopeName(scope), alias, got, "llm-settings-save:"+canonical)
			}
		}
	}
}

// A key another save committed between this save's first check and its lock
// is seen by the check inside the lock: the save is refused before any write.
func TestLLMSettingsSaveRechecksTheKeysUnderTheLock(t *testing.T) {
	for _, writer := range llmWriteScopes {
		for _, other := range llmWriteScopes {
			if other == writer {
				continue
			}
			name := scopeName(writer) + " url, " + scopeName(other) + " key"
			baseURL := "https://p.example.invalid/" + scopeKeyText(other)
			spy := &resolverSpy{valid: true}
			h := keysetHandlers(spy, "")
			reads := 0
			committed := func() bool { reads++; return reads > 1 }
			h.storedKey = func(context.Context, string) (string, error) {
				if other == "" && committed() {
					return scopeKeyText(""), nil
				}
				return "", nil
			}
			h.storedPurposeKey = func(_ context.Context, _ string, purpose llmorgsettings.Purpose) (string, error) {
				if purpose == other && committed() {
					return scopeKeyText(other), nil
				}
				return "", nil
			}
			refusal, reached, err := keysetWrite(h, writer, nil, &baseURL)
			if err != nil || reached || refusal == nil || refusal.status != 400 || refusal.message() != llmorgsettings.BaseURLKeyRefusal {
				t.Errorf("%s: refusal=%v reached write=%v err=%v", name, refusal, reached, err)
			}
			if spy.calls != 1 {
				t.Errorf("%s: resolver calls=%d, want 1 (before the lock only)", name, spy.calls)
			}

			fresh := "sk-fresh-" + "0123456789"
			h = baseURLHandlers(&resolverSpy{valid: true}, "", "")
			reads = 0
			h.storedScopeBaseURL = func(_ context.Context, _ string, purpose llmorgsettings.Purpose) (string, error) {
				if purpose == other && committed() {
					return "https://p.example.invalid/" + fresh, nil
				}
				return "", nil
			}
			refusal, reached, err = keysetWrite(h, writer, &fresh, nil)
			if err != nil || reached || refusal == nil || refusal.status != 400 {
				t.Errorf("%s, carried key: refusal=%v reached write=%v err=%v", name, refusal, reached, err)
			}
		}
	}
}

// A save that waits on the org's save lock past the timeout answers the 409
// setting_changed, logs one WARN line with the org and scope only, counts it
// by scope, and writes nothing.
func TestLLMSettingsSaveLockTimeoutIsTheSettingChangedConflict(t *testing.T) {
	fresh := "sk-fresh-" + "0123456789"
	for _, scope := range llmWriteScopes {
		before := saveLockTimeoutCounts(t)[scopeName(scope)]
		var logs bytes.Buffer
		tx := &saveLockTx{lockErr: &pgconn.PgError{Code: "55P03", Message: "canceling statement due to lock timeout"}}
		h := baseURLHandlers(&resolverSpy{valid: true}, "", "")
		h.store.Pool = &saveLockDB{tx: tx}
		h.logger = slog.New(slog.NewTextHandler(&logs, nil))
		refusal, reached, err := keysetWrite(h, scope, &fresh, strPtr(readerCleanURL))
		if err != nil || reached || refusal == nil || refusal.status != 409 {
			t.Fatalf("%s: refusal=%v reached write=%v err=%v", scopeName(scope), refusal, reached, err)
		}
		rec := httptest.NewRecorder()
		refusal.write(rec)
		if got := strings.TrimSpace(rec.Body.String()); got != `{"detail":{"error":"setting_changed","message":"The setting changed while saving. Reload and try again."}}` {
			t.Fatalf("%s: body %s", scopeName(scope), got)
		}
		line := strings.TrimSpace(logs.String())
		if strings.Count(line, "\n") != 0 || !strings.Contains(line, "level=WARN") ||
			!strings.Contains(line, "org_id="+uuid.Nil.String()) || !strings.Contains(line, "scope="+scopeName(scope)) {
			t.Fatalf("%s: log %q", scopeName(scope), line)
		}
		for _, leak := range []string{fresh, "example.invalid"} {
			if strings.Contains(line, leak) {
				t.Fatalf("%s: log carries %q: %s", scopeName(scope), leak, line)
			}
		}
		if got := saveLockTimeoutCounts(t)[scopeName(scope)] - before; got != 1 {
			t.Fatalf("%s: counter delta %d, want 1", scopeName(scope), got)
		}
	}
	tx := &saveLockTx{lockErr: errors.New("conn reset")}
	h := baseURLHandlers(&resolverSpy{valid: true}, "", "")
	h.store.Pool = &saveLockDB{tx: tx}
	if refusal, _, err := keysetWrite(h, "", &fresh, nil); refusal != nil || err == nil {
		t.Fatalf("other lock error: refusal=%v err=%v, want the error", refusal, err)
	}
}

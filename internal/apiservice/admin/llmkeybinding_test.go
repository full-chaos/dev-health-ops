package admin

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/llmorgsettings"
)

var (
	bindingKeyAt   = time.Date(2026, 11, 1, 10, 0, 0, 0, time.UTC)
	bindingLaterAt = bindingKeyAt.Add(time.Hour)
)

// scopeRows are one scope's stored rows as llmorgsettings.ReadScopeKey selects
// them; a nil time is an absent row.
type scopeRows struct {
	provider, baseURL, key       string
	providerAt, baseURLAt, keyAt *time.Time
}

func (r scopeRows) Exec(context.Context, string, ...any) (pgconn.CommandTag, error) {
	return pgconn.CommandTag{}, nil
}
func (r scopeRows) QueryRow(context.Context, string, ...any) pgx.Row { return r }
func (r scopeRows) Scan(dest ...any) error {
	text := func(v string, at *time.Time) *string {
		if at == nil {
			return nil
		}
		return &v
	}
	plain := false
	*dest[0].(**string), *dest[1].(**time.Time) = text(r.provider, r.providerAt), r.providerAt
	*dest[2].(**string), *dest[3].(**time.Time) = text(r.baseURL, r.baseURLAt), r.baseURLAt
	*dest[4].(**string), *dest[5].(**bool), *dest[6].(**time.Time) = text(r.key, r.keyAt), &plain, r.keyAt
	return nil
}

func at(t time.Time) *time.Time { return &t }

func readKey(t *testing.T, rows scopeRows) llmorgsettings.ScopeKey {
	t.Helper()
	key, err := (&handlers{}).scopeKey(context.Background(), rows, "org", "", false, true)
	if err != nil {
		t.Fatal(err)
	}
	return key
}

// boundRows: provider, base_url and key written together (key last).
func boundRows(provider, baseURL string) scopeRows {
	return scopeRows{provider: provider, baseURL: baseURL, key: "sk-stored-" + "0123456789",
		providerAt: at(bindingKeyAt), baseURLAt: at(bindingKeyAt), keyAt: at(bindingKeyAt)}
}

// The write table: purpose x stored provider x stored base_url x new provider x
// new base_url x api_key in the body, over a scope that stores a bound key.
// Purposes see a default row on anthropic with no base_url and a bound key. The
// expectation is the rule as CHAOS-13096 states it, written out here.
func TestTargetChangeKeyStepTable(t *testing.T) {
	const defaultProvider = "anthropic"
	needsKey := map[string]bool{"openai": true, "anthropic": true}
	knownKeyless := map[string]bool{"ollama": true}
	def := readKey(t, boundRows(defaultProvider, ""))
	rows := 0
	for _, purpose := range []string{"", "investigate", "explain", "categorize"} {
		for _, old := range []keyTarget{{"openai", ""}, {"anthropic", ""}, {"anthropic", "https://a.example/v1"}, {"ollama", "http://gpu.example/v1"}, {"", ""}} {
			for _, next := range []keyTarget{{"openai", ""}, {" OpenAI ", ""}, {"anthropic", ""}, {"anthropic", "https://a.example/v1"}, {"anthropic", "https://b.example/v1"}, {"ollama", "http://gpu.example/v1"}, {"none", ""}, {"   ", ""}} {
				for _, bodyKey := range []bool{true, false} {
					rows++
					stored := readKey(t, boundRows(old.provider, old.baseURL))
					inherits := purpose != "" && suppliesKeyFor(def, next)
					name := strings.ToLower(strings.TrimSpace(next.provider))
					want := keyStepKeep
					switch {
					case bodyKey:
						want = keyStepWrite
					case old.provider == name && old.baseURL == next.baseURL:
						want = keyStepRebind
					case purpose != "" && name == defaultProvider && next.baseURL == "":
						want = keyStepDelete
					case needsKey[name]:
						want = keyStepRefuse
					case knownKeyless[name]:
						want = keyStepDelete
					}
					if got := targetChangeKeyStep(stored, next, bodyKey, inherits); got != want {
						t.Errorf("purpose=%q %+v -> %+v body_key=%v: step=%d, want %d", purpose, old, next, bodyKey, got, want)
					}
				}
			}
		}
	}
	t.Logf("rows=%d", rows)
}

// No stored key: nothing can cross. An unbound key on an unchanged target is
// kept as it is (still not served), never re-stamped into a bound one.
func TestTargetChangeWithNoOrAnUnboundKey(t *testing.T) {
	if got := targetChangeKeyStep(readKey(t, scopeRows{provider: "openai", providerAt: at(bindingKeyAt)}), keyTarget{"anthropic", ""}, false, false); got != keyStepKeep {
		t.Errorf("no stored key: step=%d, want keep", got)
	}
	unbound := readKey(t, scopeRows{provider: "anthropic", key: "sk-x-0123456789", providerAt: at(bindingLaterAt), keyAt: at(bindingKeyAt)})
	if got := targetChangeKeyStep(unbound, keyTarget{"anthropic", ""}, false, false); got != keyStepKeep {
		t.Errorf("unbound key, same target: step=%d, want keep", got)
	}
}

// The cell cag asked for: anthropic+key -> "" (no BYO) with no key keeps the row
// unserved, and "" -> anthropic with no key is refused.
func TestNoBYOAndBackWithoutAKeyIsRefused(t *testing.T) {
	stored := readKey(t, boundRows("anthropic", ""))
	if got := targetChangeKeyStep(stored, keyTarget{"", ""}, false, false); got != keyStepKeep {
		t.Fatalf("anthropic -> no BYO: step=%d, want keep", got)
	}
	between := readKey(t, scopeRows{provider: "", key: "sk-ant-0123456789", providerAt: at(bindingLaterAt), keyAt: at(bindingKeyAt)})
	t.Logf("between: stored=%v served=%q", between.Stored(), between.Served())
	if between.Served() != "" {
		t.Errorf("the kept key is served while no BYO is saved")
	}
	if got := targetChangeKeyStep(between, keyTarget{"anthropic", ""}, false, false); got != keyStepRefuse {
		t.Errorf("no BYO -> anthropic without a key: step=%d, want refuse", got)
	}
}

// An override inherits the default key only for the default's provider and
// base_url, and only while that key is bound.
func TestTheDefaultSuppliesItsKeyOnlyWhenBoundAndSameTarget(t *testing.T) {
	for _, c := range []struct {
		name string
		def  scopeRows
		next keyTarget
		want bool
	}{
		{"bound, same", boundRows("anthropic", ""), keyTarget{"Anthropic", ""}, true},
		{"bound, other provider", boundRows("anthropic", ""), keyTarget{"openai", ""}, false},
		{"bound, other base_url", boundRows("anthropic", ""), keyTarget{"anthropic", "https://8.8.8.8/v1"}, false},
		{"unbound by provider", scopeRows{provider: "anthropic", key: "sk-x-0123456789", providerAt: at(bindingLaterAt), keyAt: at(bindingKeyAt)}, keyTarget{"anthropic", ""}, false},
		{"unbound by base_url", scopeRows{provider: "anthropic", key: "sk-x-0123456789", providerAt: at(bindingKeyAt), baseURLAt: at(bindingLaterAt), keyAt: at(bindingKeyAt)}, keyTarget{"anthropic", ""}, false},
		{"no key", scopeRows{provider: "anthropic", providerAt: at(bindingKeyAt)}, keyTarget{"anthropic", ""}, false},
	} {
		if got := suppliesKeyFor(readKey(t, c.def), c.next); got != c.want {
			t.Errorf("%s: %v, want %v", c.name, got, c.want)
		}
	}
}

// The refusal names its code and carries no key.
func TestTargetChangeRefusalShape(t *testing.T) {
	rec := httptest.NewRecorder()
	targetChangeRefusal().write(rec)
	var body struct {
		Detail map[string]string `json:"detail"`
	}
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if rec.Code != 400 || body.Detail["error"] != "provider_or_base_url_change_requires_api_key" || body.Detail["feature"] != byoLLMFeature {
		t.Fatalf("status=%d body=%s", rec.Code, rec.Body.String())
	}
}

// The readers that send the default key out (live model list, readiness probe)
// get no key older than the provider or base_url row; the base_url leak check
// still sees it.
func TestAnUnboundDefaultKeyReachesNoVendor(t *testing.T) {
	key := "sk-openai-" + "0123456789"
	for name, rows := range map[string]scopeRows{
		"provider changed after the key": {provider: "anthropic", key: key, providerAt: at(bindingLaterAt), keyAt: at(bindingKeyAt)},
		"base_url changed after the key": {provider: "openai", baseURL: "https://8.8.8.8/v1", key: key, providerAt: at(bindingKeyAt), baseURLAt: at(bindingLaterAt), keyAt: at(bindingKeyAt)},
	} {
		stored := readKey(t, rows)
		cfg := readinessBYOConfig{provider: rows.provider, model: "m", baseURL: "https://gateway.example.invalid/v1"}.bindDefaultKey(stored)
		h, doer := newModelListHandlers()
		out, err := h.modelList(context.Background(), "org", cfg, "")
		if err != nil {
			t.Fatal(err)
		}
		view, raw := viewModelList(t, out)
		t.Logf("%s: model list source=%s vendor_requests=%d readiness_usable_key=%v", name, view.source, len(doer.requests), cfg.apiKey.Configured())
		if len(doer.requests) != 0 || strings.Contains(raw, key) || cfg.apiKey.Configured() {
			t.Errorf("%s: an unbound key reached a vendor reader", name)
		}
		spy := &resolverSpy{valid: true}
		keyed := cfg
		keyed.baseURL = "https://" + key + ".example.invalid/v1"
		if valid, err := readerHandlers(spy).readinessURLValid(context.Background(), keyed); err != nil || valid || spy.calls != 0 {
			t.Errorf("%s: a base_url holding the unbound key passed: valid=%v err=%v resolver calls=%d", name, valid, err, spy.calls)
		}
		if strings.Contains(fmt.Sprintf("%v %+v %#v", cfg, cfg, cfg), key) {
			t.Errorf("%s: a printed config carries the unbound key", name)
		}
	}
}

// A bound key is served whole to the same readers.
func TestABoundDefaultKeyIsServed(t *testing.T) {
	stored := readKey(t, boundRows("openai", ""))
	if !stored.Bound() || stored.Served() == "" {
		t.Fatalf("bound=%v served=%v", stored.Bound(), stored.Served() != "")
	}
	if cfg := (readinessBYOConfig{provider: "openai"}).bindDefaultKey(stored); !cfg.apiKey.Configured() || cfg.staleKey.Configured() {
		t.Fatalf("a bound key is not served")
	}
}

// A direct read of a BYO key row is refused before any database call: every
// key read goes through scopeKey.
func TestADirectKeyRowReadIsRefused(t *testing.T) {
	for _, key := range []string{"api_key", "purpose:explain:api_key", "purpose:categorize:api_key"} {
		if _, err := settingByKeyOn(context.Background(), nil, "org", llmCategory, key); err != errLLMKeyRowNeedsAccessor {
			t.Errorf("%s: err=%v, want the accessor refusal", key, err)
		}
	}
}

// writeDB records the writes of upsertSetting over an empty table.
type writeDB struct{ execs *[]string }

func (db writeDB) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	*db.execs = append(*db.execs, sql)
	return pgconn.CommandTag{}, nil
}
func (db writeDB) QueryRow(context.Context, string, ...any) pgx.Row { return absentRow{} }

type absentRow struct{}

func (absentRow) Scan(...any) error { return pgx.ErrNoRows }

// The write of a key row is not a read: upsertSetting stores api_key rows
// whatever the accessor guard refuses to readers.
func TestTheWriteOfAKeyRowIsNotRefused(t *testing.T) {
	var execs []string
	value := "placeholder-key-value"
	for _, key := range []string{"api_key", "purpose:explain:api_key"} {
		if _, err := (&handlers{store: pgStore{}}).upsertSetting(context.Background(), writeDB{&execs}, "org", llmCategory, key, &value, false, nil); err != nil {
			t.Fatalf("%s: upsert refused: %v", key, err)
		}
	}
	if len(execs) != 2 || !strings.Contains(execs[0], "INSERT INTO settings") {
		t.Fatalf("writes=%q", execs)
	}
}

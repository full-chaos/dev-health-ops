package integrationsadmin

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// The integration PATCH replaces the config whole; the CI artifact exclude list stored in it survives a new config
// that does not carry the key, a carried value replaces it after validation, and a refused value is a 422 without it.
func TestIntegrationUpdateKeepsTheArtifactExcludeList(t *testing.T) {
	current := &integration{Provider: "github", Config: `{"fetch_comments": true, "ci_artifact_exclude_prefixes": ["ci-x-"]}`}
	next := pyjson.NewObject()
	next.Set("fetch_comments", false)
	if err := keepArtifactExclude(current, next); err != nil {
		t.Fatal(err)
	}
	if got, _ := pyjson.Dumps(next); got != `{"fetch_comments": false, "ci_artifact_exclude_prefixes": ["ci-x-"]}` {
		t.Fatalf("a config without the key: %s, want the stored list kept", got)
	}
	replaced := pyjson.NewObject()
	replaced.Set("ci_artifact_exclude_prefixes", []pyjson.Value{"ci-z-"})
	if err := keepArtifactExclude(current, replaced); err != nil {
		t.Fatal(err)
	}
	if got, _ := pyjson.Dumps(replaced); got != `{"ci_artifact_exclude_prefixes": ["ci-z-"]}` {
		t.Fatalf("a carried list: %s, want it as sent", got)
	}
	bad := pyjson.NewObject()
	bad.Set("ci_artifact_exclude_prefixes", []pyjson.Value{"zz9 x"})
	err := keepArtifactExclude(current, bad)
	var denied refusal
	if !errors.As(err, &denied) || denied.status != http.StatusUnprocessableEntity || strings.Contains(denied.message, "zz9") {
		t.Fatalf("an invalid list: %v, want a 422 without the value", err)
	}
	untouched := &integration{Provider: "gitlab", Config: `{}`}
	plain := pyjson.NewObject()
	plain.Set("a", int64(1))
	if err := keepArtifactExclude(untouched, plain); err != nil {
		t.Fatal(err)
	}
	if got, _ := pyjson.Dumps(plain); got != `{"a": 1}` {
		t.Fatalf("no list anywhere: %s, want the config as sent", got)
	}
}

// configLockRecorder records the update's config step and answers the locked read with the config another writer
// committed after the handler's first (unlocked) read.
type configLockRecorder struct {
	log   []string
	fresh string
}

func (r *configLockRecorder) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	r.log = append(r.log, sql)
	return pgconn.CommandTag{}, nil
}

func (r *configLockRecorder) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	r.log = append(r.log, sql)
	return freshRow{text: r.fresh}
}

type freshRow struct{ text string }

func (f freshRow) Scan(dest ...any) error {
	*(dest[0].(*string)) = f.text
	return nil
}

// The update takes the integration's selection lock and reads the config FOR NO KEY UPDATE before it keeps the list:
// a list committed after the handler's first read is the one kept (no lost update), and the lock comes first.
func TestIntegrationUpdateLocksBeforeItReadsTheConfig(t *testing.T) {
	current := &integration{ID: uuid.New(), Provider: "github", Config: `{"fetch_comments": true}`}
	tx := &configLockRecorder{fresh: `{"fetch_comments": true, "ci_artifact_exclude_prefixes": ["ci-x-"]}`}
	next := pyjson.NewObject()
	next.Set("fetch_comments", false)
	if _, err := keepArtifactExcludeAndDecode(context.Background(), tx, "org", current, next); err != nil {
		t.Fatal(err)
	}
	if got, _ := pyjson.Dumps(next); got != `{"fetch_comments": false, "ci_artifact_exclude_prefixes": ["ci-x-"]}` {
		t.Fatalf("kept %s, want the list committed before the locked read", got)
	}
	if len(tx.log) != 2 || !strings.Contains(tx.log[0], "pg_advisory_xact_lock") || !strings.Contains(tx.log[1], "FOR NO KEY UPDATE") {
		t.Fatalf("statements %q: want the selection lock, then the locked read of the config", tx.log)
	}
}

// The create validates and clears the list by the update's rule (artifactexclude.Parse): every provider but github is a
// 422, every bound is a 422, null or [] removes the key, a valid list and an absent key pass as sent. No value is named.
func TestIntegrationCreateValidatesTheArtifactExcludeList(t *testing.T) {
	config := func(value pyjson.Value, present bool) *pyjson.Object {
		object := pyjson.NewObject()
		object.Set("fetch_comments", true)
		if present {
			object.Set("ci_artifact_exclude_prefixes", value)
		}
		return object
	}
	valid := []pyjson.Value{"ci-zz9-"}
	if text, err := createConfigText("github", config(valid, true)); err != nil || text != `{"fetch_comments": true, "ci_artifact_exclude_prefixes": ["ci-zz9-"]}` {
		t.Errorf("github valid: %s %v", text, err)
	}
	if text, err := createConfigText("gitlab", config(nil, false)); err != nil || text != `{"fetch_comments": true}` {
		t.Errorf("no key: %s %v", text, err)
	}
	for _, clear := range []pyjson.Value{nil, []pyjson.Value{}} {
		if text, err := createConfigText("github", config(clear, true)); err != nil || text != `{"fetch_comments": true}` {
			t.Errorf("clear %#v: %s %v, want the key absent", clear, text, err)
		}
	}
	refusedCells := map[string]*pyjson.Object{}
	for _, provider := range []string{"gitlab", "jira", "linear", "launchdarkly", "pagerduty"} {
		refusedCells[provider] = config(valid, true)
	}
	seventeen := make([]pyjson.Value, 17)
	for index := range seventeen {
		seventeen[index] = "zz9" + strings.Repeat("x", index)
	}
	for name, value := range map[string]pyjson.Value{
		"string": "ci-zz9-", "object": pyjson.NewObject(), "17 entries": seventeen, "empty entry": []pyjson.Value{""},
		"101 bytes": []pyjson.Value{strings.Repeat("z", 101)}, "charset": []pyjson.Value{"zz9 x"},
		"duplicate": []pyjson.Value{"zz9-", "ZZ9-"}, "not a string": []pyjson.Value{pyjson.IntOf(1)},
	} {
		refusedCells["github "+name] = config(value, true)
	}
	for name, body := range refusedCells {
		provider := strings.Fields(name)[0]
		_, err := createConfigText(provider, body)
		var denied refusal
		if !errors.As(err, &denied) || denied.status != http.StatusUnprocessableEntity || strings.Contains(denied.message, "zz9") {
			t.Errorf("%s: %v, want a 422 without the value", name, err)
		}
	}
}

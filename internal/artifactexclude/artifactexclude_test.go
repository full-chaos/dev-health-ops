package artifactexclude

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

func listOf(items ...pyjson.Value) []pyjson.Value { return items }

func TestArtifactExcludeParseBounds(t *testing.T) {
	// The bounds are literals here, not the constants, so a changed constant fails this cell.
	sixteen := make([]pyjson.Value, 16)
	seventeen := make([]pyjson.Value, 17)
	for index := range seventeen {
		seventeen[index] = "p" + strings.Repeat("x", index)
		if index < 16 {
			sixteen[index] = seventeen[index]
		}
	}
	longest := strings.Repeat("a", 100)
	cells := []struct {
		name      string
		value     pyjson.Value
		want      []string
		clear     bool
		refuseAt  int
		refusal   bool
		forbidden string
	}{
		{name: "three real-shaped prefixes", value: listOf("ci-images-", "ci-timings-", "ci-part-"), want: []string{"ci-images-", "ci-timings-", "ci-part-"}},
		{name: "null clears", value: nil, clear: true},
		{name: "empty list clears", value: listOf(), clear: true},
		{name: "max entries", value: sixteen, want: func() []string {
			out := make([]string, 16)
			for index, item := range sixteen {
				out[index] = item.(string)
			}
			return out
		}()},
		{name: "one entry over max", value: seventeen, refusal: true, refuseAt: -1},
		{name: "max entry bytes", value: listOf(longest), want: []string{longest}},
		{name: "one byte over max", value: listOf("ok", longest+"b"), refusal: true, refuseAt: 1, forbidden: longest},
		{name: "empty entry", value: listOf("ok", ""), refusal: true, refuseAt: 1},
		{name: "space", value: listOf("ci images"), refusal: true, refuseAt: 0, forbidden: "ci images"},
		{name: "slash", value: listOf("zz9/images"), refusal: true, refuseAt: 0, forbidden: "zz9/images"},
		{name: "glob", value: listOf("ci-*"), refusal: true, refuseAt: 0, forbidden: "ci-*"},
		{name: "non-ascii look-alike", value: listOf("ci-іmages"), refusal: true, refuseAt: 0, forbidden: "іmages"},
		{name: "control character", value: listOf("ci\n"), refusal: true, refuseAt: 0},
		{name: "case-insensitive duplicate", value: listOf("ci-images-", "CI-IMAGES-"), refusal: true, refuseAt: 1, forbidden: "CI-IMAGES-"},
		{name: "not a string", value: listOf("ok", pyjson.IntOf(3)), refusal: true, refuseAt: 1},
		{name: "not a list", value: "ci-images-", refusal: true, refuseAt: -1, forbidden: "ci-images-"},
		{name: "an object", value: pyjson.NewObject(), refusal: true, refuseAt: -1},
	}
	for _, cell := range cells {
		change, err := Parse("github", cell.value)
		if cell.refusal {
			var refused *Refusal
			if !errors.As(err, &refused) {
				t.Errorf("%s: err=%v, want a refusal", cell.name, err)
				continue
			}
			if refused.Index != cell.refuseAt {
				t.Errorf("%s: refusal index %d, want %d", cell.name, refused.Index, cell.refuseAt)
			}
			if cell.forbidden != "" && strings.Contains(refused.Message, cell.forbidden) {
				t.Errorf("%s: the refusal %q names the submitted value", cell.name, refused.Message)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s: err=%v, want accepted", cell.name, err)
			continue
		}
		if change.Clear != cell.clear || !slices.Equal(change.Prefixes, cell.want) {
			t.Errorf("%s: change=%+v, want clear=%v prefixes=%v", cell.name, change, cell.clear, cell.want)
		}
	}
}

func TestArtifactExcludeParseRefusesEveryNonGitHubProvider(t *testing.T) {
	value := listOf("ci-images-")
	for _, provider := range []string{"github", "GitHub"} {
		if _, err := Parse(provider, value); err != nil {
			t.Errorf("%s: err=%v, want accepted", provider, err)
		}
	}
	for _, provider := range []string{"gitlab", "jira", "linear", "launchdarkly", "pagerduty", ""} {
		_, err := Parse(provider, value)
		var refused *Refusal
		if !errors.As(err, &refused) {
			t.Errorf("%q: err=%v, want a refusal: the field does nothing outside the GitHub tests sync", provider, err)
			continue
		}
		if strings.Contains(refused.Message, "ci-images-") {
			t.Errorf("%q: the refusal %q names the submitted value", provider, refused.Message)
		}
	}
}

func TestArtifactExcludeParseDecodedMatchesParse(t *testing.T) {
	for _, cell := range []struct {
		decoded any
		value   pyjson.Value
	}{
		{decoded: []any{"ci-images-", "ci-part-"}, value: listOf("ci-images-", "ci-part-")},
		{decoded: nil, value: nil},
		{decoded: []any{}, value: listOf()},
		{decoded: []any{""}, value: listOf("")},
		{decoded: []any{"a b"}, value: listOf("a b")},
		{decoded: []any{float64(3)}, value: listOf(pyjson.IntOf(3))},
		{decoded: "ci-images-", value: "ci-images-"},
	} {
		fromDecoded, errDecoded := ParseDecoded(cell.decoded)
		fromAPI, errAPI := Parse("github", cell.value)
		if (errDecoded == nil) != (errAPI == nil) || fromDecoded.Clear != fromAPI.Clear || !slices.Equal(fromDecoded.Prefixes, fromAPI.Prefixes) {
			t.Errorf("%#v: decoded=(%+v, %v) api=(%+v, %v), want the same answer", cell.decoded, fromDecoded, errDecoded, fromAPI, errAPI)
		}
	}
}

func TestArtifactExcludeMatchesIsACaseInsensitivePrefix(t *testing.T) {
	prefixes := []string{"ci-images-", "CI-Timings-"}
	for name, want := range map[string]bool{
		"ci-images-amd64-1": true, "CI-IMAGES-x": true, "ci-timings-3": true, "ci-images": false, "test-results": false, "x-ci-images-1": false,
	} {
		if got := Matches(name, prefixes); got != want {
			t.Errorf("Matches(%q)=%v, want %v", name, got, want)
		}
	}
	if Matches("anything", nil) {
		t.Error("an empty list matched a name")
	}
}

func TestArtifactExcludeApplyTouchesOnlyItsKey(t *testing.T) {
	stored := `{"fetch_comments": true, "comments_limit": 500, "ci_artifact_exclude_prefixes": ["old-"], "all_repos": true}`
	text, changed, err := apply(&stored, Change{Prefixes: []string{"ci-images-"}})
	if err != nil || !changed {
		t.Fatalf("apply: changed=%v err=%v", changed, err)
	}
	if want := `{"fetch_comments": true, "comments_limit": 500, "ci_artifact_exclude_prefixes": ["ci-images-"], "all_repos": true}`; text != want {
		t.Fatalf("set: %s, want %s (the key keeps its place, no other key moves)", text, want)
	}
	text, changed, err = apply(&text, Change{Clear: true})
	if err != nil || !changed {
		t.Fatalf("clear: changed=%v err=%v", changed, err)
	}
	if want := `{"fetch_comments": true, "comments_limit": 500, "all_repos": true}`; text != want {
		t.Fatalf("clear: %s, want %s", text, want)
	}
	again, changed, err := apply(&text, Change{Clear: true})
	if err != nil || changed || again != text {
		t.Fatalf("a second clear: changed=%v err=%v text=%s, want no change", changed, err, again)
	}
	added, changed, err := apply(&text, Change{Prefixes: []string{"ci-images-"}})
	if err != nil || !changed || !strings.HasSuffix(added, `"all_repos": true, "ci_artifact_exclude_prefixes": ["ci-images-"]}`) {
		t.Fatalf("add: changed=%v err=%v text=%s, want the key appended", changed, err, added)
	}
	same, changed, err := apply(&added, Change{Prefixes: []string{"ci-images-"}})
	if err != nil || changed || same != added {
		t.Fatalf("the same list again: changed=%v err=%v, want no change", changed, err)
	}
}

// A write replaces the whole list, never merges it: [x, y] then [z] stores [z]; a clear or an empty list removes it.
func TestArtifactExcludeWriteReplacesTheWholeList(t *testing.T) {
	stored := `{"all_repos": true}`
	first, _, err := apply(&stored, Change{Prefixes: []string{"ci-x-", "ci-y-"}})
	if err != nil {
		t.Fatal(err)
	}
	second, changed, err := apply(&first, Change{Prefixes: []string{"ci-z-"}})
	if err != nil || !changed {
		t.Fatalf("second write: changed=%v err=%v", changed, err)
	}
	if want := `{"all_repos": true, "ci_artifact_exclude_prefixes": ["ci-z-"]}`; second != want {
		t.Fatalf("[x, y] then [z] stored %s, want %s (last writer wins, no merge)", second, want)
	}
	for _, clear := range []pyjson.Value{nil, listOf()} {
		change, err := Parse("github", clear)
		if err != nil {
			t.Fatal(err)
		}
		cleared, changed, err := apply(&second, change)
		if err != nil || !changed || cleared != `{"all_repos": true}` {
			t.Fatalf("clear %#v: %s changed=%v err=%v, want the key gone", clear, cleared, changed, err)
		}
	}
}

// Overlay shows the integration's list in a configuration's options. With no list anywhere the options are byte-equal
// to what they were; a copy the configuration itself holds is never shown.
func TestArtifactExcludeOverlayShowsTheIntegrationList(t *testing.T) {
	render := func(options *pyjson.Object) string {
		text, err := pyjson.Dumps(options)
		if err != nil {
			t.Fatal(err)
		}
		return text
	}
	decode := func(text string) *pyjson.Object {
		value, err := pyjson.DecodeString(text)
		if err != nil {
			t.Fatal(err)
		}
		return value.(*pyjson.Object)
	}
	plain := `{"all_repos": true, "schedule_cron": "0 * * * *"}`
	options := decode(plain)
	Overlay(options, nil, false)
	if got := render(options); got != plain {
		t.Errorf("no list: %s, want the options byte-equal %s", got, plain)
	}
	options = decode(plain)
	Overlay(options, listOf("ci-z-"), true)
	if got, want := render(options), `{"all_repos": true, "schedule_cron": "0 * * * *", "ci_artifact_exclude_prefixes": ["ci-z-"]}`; got != want {
		t.Errorf("integration list: %s, want %s", got, want)
	}
	stale := `{"ci_artifact_exclude_prefixes": ["old-"], "all_repos": true}`
	options = decode(stale)
	Overlay(options, nil, false)
	if got := render(options); got != `{"all_repos": true}` {
		t.Errorf("a copy in the configuration, none on the integration: %s, want it not shown", got)
	}
	options = decode(stale)
	Overlay(options, listOf("ci-z-"), true)
	if got := render(options); got != `{"all_repos": true, "ci_artifact_exclude_prefixes": ["ci-z-"]}` {
		t.Errorf("a copy in the configuration and a list on the integration: %s, want the integration's", got)
	}
}

// The integrations PATCH replaces the config whole: a new config without the key keeps the stored list, one with the
// key is validated and replaces it (null or [] removes it), and a provider the field does nothing for is refused.
func TestArtifactExcludeKeepOnReplace(t *testing.T) {
	stored := `{"fetch_comments": true, "ci_artifact_exclude_prefixes": ["ci-x-", "ci-y-"]}`
	none := `{"fetch_comments": true}`
	object := func(text string) *pyjson.Object {
		value, err := pyjson.DecodeString(text)
		if err != nil {
			t.Fatal(err)
		}
		return value.(*pyjson.Object)
	}
	for _, cell := range []struct {
		name, provider, stored, next, want string
		refused                            bool
	}{
		{name: "kept when not carried", provider: "github", stored: stored, next: `{"fetch_comments": false}`, want: `{"fetch_comments": false, "ci_artifact_exclude_prefixes": ["ci-x-", "ci-y-"]}`},
		{name: "replaced whole when carried", provider: "github", stored: stored, next: `{"ci_artifact_exclude_prefixes": ["ci-z-"]}`, want: `{"ci_artifact_exclude_prefixes": ["ci-z-"]}`},
		{name: "null removes", provider: "github", stored: stored, next: `{"fetch_comments": true, "ci_artifact_exclude_prefixes": null}`, want: none},
		{name: "empty removes", provider: "github", stored: stored, next: `{"fetch_comments": true, "ci_artifact_exclude_prefixes": []}`, want: none},
		{name: "nothing stored, nothing carried", provider: "gitlab", stored: none, next: `{"a": 1}`, want: `{"a": 1}`},
		{name: "invalid value", provider: "github", stored: stored, next: `{"ci_artifact_exclude_prefixes": ["a b"]}`, refused: true},
		{name: "another provider carrying it", provider: "gitlab", stored: none, next: `{"ci_artifact_exclude_prefixes": ["ci-x-"]}`, refused: true},
	} {
		next := object(cell.next)
		text := cell.stored
		err := KeepOnReplace(cell.provider, &text, next)
		var refused *Refusal
		if cell.refused {
			if !errors.As(err, &refused) {
				t.Errorf("%s: err=%v, want a refusal", cell.name, err)
			}
			continue
		}
		got, _ := pyjson.Dumps(next)
		if err != nil || got != cell.want {
			t.Errorf("%s: %s (err %v), want %s", cell.name, got, err, cell.want)
		}
	}
}

// recordingTx is a pgx.Tx that records the statements it is sent and answers the reads WriteTx makes.
type recordingTx struct {
	pgx.Tx
	log         []string
	integration uuid.UUID
}

type recordedRow struct{ values []any }

func (r recordedRow) Scan(dest ...any) error {
	for index, target := range dest {
		switch typed := target.(type) {
		case *string:
			*typed = r.values[index].(string)
		case **string:
			text := r.values[index].(string)
			*typed = &text
		case **uuid.UUID:
			id := r.values[index].(uuid.UUID)
			*typed = &id
		}
	}
	return nil
}

func (r *recordingTx) QueryRow(_ context.Context, sql string, _ ...any) pgx.Row {
	r.log = append(r.log, sql)
	switch {
	case strings.Contains(sql, "FROM integrations"):
		return recordedRow{values: []any{`{"fetch_comments": true}`}}
	case strings.Contains(sql, "integration_id, sync_options::text"):
		return recordedRow{values: []any{r.integration, `{"all_repos": true}`}}
	default:
		return recordedRow{values: []any{"github", r.integration}}
	}
}

func (r *recordingTx) Exec(_ context.Context, sql string, _ ...any) (pgconn.CommandTag, error) {
	r.log = append(r.log, sql)
	return pgconn.CommandTag{}, nil
}

// One lock order for every writer of an integration's config: the selection lock, then the integration row, then the
// configuration row, each row read under its lock; the old order (configuration row first) deadlocks against a PATCH.
func TestWriteTxLocksTheIntegrationBeforeTheConfig(t *testing.T) {
	tx := &recordingTx{integration: uuid.New()}
	lock := func(_ context.Context, locked pgx.Tx, _ string, id uuid.UUID) error {
		if id != tx.integration {
			t.Errorf("locked integration %s, want %s", id, tx.integration)
		}
		tx.log = append(tx.log, "SELECTION LOCK")
		return nil
	}
	if _, err := WriteTx(context.Background(), tx, lock, "org", uuid.New(), Change{Prefixes: []string{"ci-x-"}}, time.Now()); err != nil {
		t.Fatal(err)
	}
	position := func(want string) int {
		for index, sql := range tx.log {
			if strings.Contains(sql, want) {
				return index
			}
		}
		t.Fatalf("no statement holds %q in %q", want, tx.log)
		return -1
	}
	selection := position("SELECTION LOCK")
	integrationRow := position("FROM integrations WHERE org_id = $1 AND id = $2 FOR NO KEY UPDATE")
	configRow := position("FROM sync_configurations\nWHERE org_id = $1 AND id = $2 FOR NO KEY UPDATE")
	if !(selection < integrationRow && integrationRow < configRow) {
		t.Fatalf("lock order %d, %d, %d in %q: want the selection lock, then the integration row, then the configuration row",
			selection, integrationRow, configRow, tx.log)
	}
	for index, sql := range tx.log[:selection] {
		if strings.Contains(sql, "FOR ") {
			t.Errorf("statement %d locks a row before the selection lock: %s", index, sql)
		}
	}
}

package providersync

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

// realJiraProjectMoveChangelog is the ACTUAL changelog Jira returned for a
// real project Move captured live during CHAOS-4193's build (chris moved
// issue OPS-9 to project API, becoming API-24, 2026-08-24). Personal data
// (accountId/email/displayName) is replaced with placeholders; every other
// byte -- field names, field/fieldId/fieldtype values, the from/to id and
// string shapes, the shared history entry id -- is verbatim. This replaces
// the synthetic fixture the earlier drafts of this producer would have used:
// a Move logs "Key" (the issue's own key, no project data) and "project"
// (internal numeric project ids; fromString/toString are the project NAME,
// not necessarily its key) together on ONE history entry sharing one id.
const realJiraProjectMoveChangelog = `{
  "histories": [
    {
      "id": "10894",
      "author": {"accountId": "redacted-account-id", "emailAddress": "redacted@example.com", "displayName": "redacted"},
      "created": "2026-08-24T12:23:43.954-0700",
      "items": [
        {
          "field": "Key",
          "fieldtype": "jira",
          "from": null,
          "fromString": "OPS-9",
          "to": null,
          "toString": "API-24"
        },
        {
          "field": "project",
          "fieldId": "project",
          "fieldtype": "jira",
          "from": "10114",
          "fromString": "Operations",
          "to": "10111",
          "toString": "API"
        }
      ]
    }
  ]
}`

func TestJiraProjectMoveItemsExtractsTheRealMoveShape(t *testing.T) {
	var changelog map[string]any
	if err := json.Unmarshal([]byte(realJiraProjectMoveChangelog), &changelog); err != nil {
		t.Fatal(err)
	}
	fallbackCreated := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	items := jiraProjectMoveItems(changelog, fallbackCreated, false, false, nil)
	if len(items) != 1 {
		t.Fatalf("items=%+v, want exactly one project-membership fact (the \"Key\" item carries no project data and must not produce a second)", items)
	}
	got := items[0]
	if got.FromProjectID != "10114" || got.ToProjectID != "10111" {
		t.Fatalf("ids: from=%q to=%q, want from=10114 to=10111", got.FromProjectID, got.ToProjectID)
	}
	// fromString/toString are the project NAME, confirmed live -- API's name
	// happens to equal its key, which is exactly why the producer must
	// resolve the real key by id rather than trusting these strings as keys.
	if got.FromProjectName != "Operations" || got.ToProjectName != "API" {
		t.Fatalf("names: from=%q to=%q, want from=Operations to=API", got.FromProjectName, got.ToProjectName)
	}
	if got.EventID != "jira:10894" {
		t.Fatalf("event_id=%q, want jira:10894 (native history id, prefixed)", got.EventID)
	}
	wantOccurred := time.Date(2026, 8, 24, 19, 23, 43, 954000000, time.UTC) // -0700 normalized to UTC
	if !got.OccurredAt.Equal(wantOccurred) {
		t.Fatalf("occurred_at=%v, want %v (UTC-normalized from the -0700 offset)", got.OccurredAt, wantOccurred)
	}
}

func TestJiraProjectMoveItemsIgnoresStatusOnlyChangelog(t *testing.T) {
	var changelog map[string]any
	if err := json.Unmarshal([]byte(`{"histories":[{"id":"1","created":"2026-07-20T09:00:00Z","items":[{"field":"status","fromString":"To Do","toString":"Done"}]}]}`), &changelog); err != nil {
		t.Fatal(err)
	}
	items := jiraProjectMoveItems(changelog, time.Now().UTC(), false, false, nil)
	if len(items) != 0 {
		t.Fatalf("items=%+v, want none -- a pure status transition carries no project-membership fact", items)
	}
}

func TestJiraProjectMoveItemsSkipsAMalformedProjectItemWithNoDestination(t *testing.T) {
	var changelog map[string]any
	if err := json.Unmarshal([]byte(`{"histories":[{"id":"1","created":"2026-07-20T09:00:00Z","items":[{"field":"project","from":"10114","fromString":"Operations"}]}]}`), &changelog); err != nil {
		t.Fatal(err)
	}
	items := jiraProjectMoveItems(changelog, time.Now().UTC(), false, false, nil)
	if len(items) != 0 {
		t.Fatalf("items=%+v, want none -- a \"project\" item naming no destination is malformed and must not mint a row", items)
	}
}

func TestJiraProjectMembershipDropsAndCountsOnlyWhenTheIDItselfIsBlank(t *testing.T) {
	if entry, ok := resolveJiraProjectCatalogForTest("", ""); ok {
		t.Fatalf("entry=%+v ok=%v, want a blank id to resolve to nothing", entry, ok)
	}
}

// resolveJiraProjectCatalogForTest exercises resolveJiraProjectCatalog's
// pure decision (blank id -> unresolved) without a live HTTP client, mirroring
// how the function is actually called from Collect.
func resolveJiraProjectCatalogForTest(id, changelogName string) (jiraProjectCatalogEntry, bool) {
	cache := make(map[string]jiraProjectCatalogEntry)
	requests := 0
	return resolveJiraProjectCatalog(nil, nil, cache, &requests, id, changelogName, "", "")
}

// When the id being resolved is the issue's OWN current project (already
// known for free from fields.project, never the changelog), a failing live
// lookup must not blank the key: project_membership_presence's work_items
// fallback arm and Jira ownership records both key off it
// (team_autoimport_jira.py), so losing a key already on hand would silently
// break attribution (codex review finding, CHAOS-4193).
func TestResolveJiraProjectCatalogPreservesTheKnownKeyOnALookupFailure(t *testing.T) {
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{}`)), Request: request,
		}, nil
	})
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", doer,
		func(request *http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	cache := make(map[string]jiraProjectCatalogEntry)
	requests := 0
	entry, ok := resolveJiraProjectCatalog(
		context.Background(), client, cache, &requests,
		"10001", "Operations (changelog name)", "10001", "OPS",
	)
	if !ok {
		t.Fatal("want resolved (changelog name still names the project)")
	}
	if entry.Key != "OPS" {
		t.Fatalf("key=%q, want the issue's own known key OPS preserved despite the failed live lookup", entry.Key)
	}
	if entry.Name != "Operations (changelog name)" {
		t.Fatalf("name=%q, want the changelog fallback name", entry.Name)
	}
}

// A failing lookup for a project id that is NOT the issue's current project
// gets no such shortcut -- there is nothing already known to fall back to,
// so the key stays blank (the changelog name alone still resolves the row).
func TestResolveJiraProjectCatalogLeavesTheKeyBlankWhenNoCurrentProjectIsKnown(t *testing.T) {
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{}`)), Request: request,
		}, nil
	})
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", doer,
		func(request *http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	cache := make(map[string]jiraProjectCatalogEntry)
	requests := 0
	entry, ok := resolveJiraProjectCatalog(
		context.Background(), client, cache, &requests,
		"10114", "Operations (changelog name)", "10001", "OPS",
	)
	if !ok {
		t.Fatal("want resolved (changelog name still names the project)")
	}
	if entry.Key != "" {
		t.Fatalf("key=%q, want blank -- no known key exists for this id, only for the current project", entry.Key)
	}
}

// A cache hit must not permanently freeze an incomplete (key-less) entry.
// Reproduces the exact ordering codex flagged: one issue resolves project id
// X while X is only historical for it (no known key, live lookup fails), a
// LATER issue in the same Collect call has X as its OWN current project --
// that second call must still reach the currentProjectKey fallback rather
// than short-circuiting on the first call's incomplete cached entry
// (codex review finding, CHAOS-4193).
func TestResolveJiraProjectCatalogDoesNotFreezeAnIncompleteCacheEntry(t *testing.T) {
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{}`)), Request: request,
		}, nil
	})
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", doer,
		func(request *http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	cache := make(map[string]jiraProjectCatalogEntry)
	requests := 0

	// First issue: id "10001" is only its FROM side, not its current
	// project -- no fallback applies, and the live lookup fails.
	first, ok := resolveJiraProjectCatalog(
		context.Background(), client, cache, &requests,
		"10001", "Operations (changelog name)", "10999", "OTHER",
	)
	if !ok || first.Key != "" {
		t.Fatalf("first=%+v ok=%v, want resolved with a blank key", first, ok)
	}

	// Second issue: id "10001" IS its current project -- must still get the
	// known-key fallback, not the first call's cached blank-key entry.
	second, ok := resolveJiraProjectCatalog(
		context.Background(), client, cache, &requests,
		"10001", "Operations (changelog name)", "10001", "OPS",
	)
	if !ok {
		t.Fatal("want resolved")
	}
	if second.Key != "OPS" {
		t.Fatalf("key=%q, want OPS -- the first call's incomplete cache entry must not have frozen this id's key", second.Key)
	}
}

// A known id with no name anywhere (live lookup failed, changelog names
// nothing) is still resolved, not dropped: to_project_id is what
// project_membership_presence actually resolves on, and an empty name here
// is the same shape Linear's own from-side catalog rows already carry
// (codex review finding, CHAOS-4193).
func TestResolveJiraProjectCatalogResolvesAKnownIDWithNoNameAnywhere(t *testing.T) {
	entry, ok := resolveJiraProjectCatalogForTest("10114", "")
	if !ok {
		t.Fatal("want resolved -- the id itself is known, dropping loses to_project_id")
	}
	if entry.Name != "" || entry.Key != "" {
		t.Fatalf("entry=%+v, want blank key and name -- nothing names this project, but the row is still kept", entry)
	}
}

// A persistently unavailable project must not turn one outage into one live
// request per move that references it. Every resolution -- complete or not
// -- is cached; a cache hit for a later issue whose own current project is
// this same id still upgrades the stored entry via the known-key fallback,
// so caching failures never regresses the round-3 ordering fix (codex review
// finding, CHAOS-4193).
func TestResolveJiraProjectCatalogCachesAndUpgradesAFallbackEntry(t *testing.T) {
	doer := jiraWorkItemsDoerFunc(func(request *http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusInternalServerError,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(strings.NewReader(`{}`)), Request: request,
		}, nil
	})
	client, err := providerfoundation.NewHTTPClient(
		"jira", "https://acme.atlassian.net", doer,
		func(request *http.Request) error { return nil },
		providerfoundation.RetryPolicy{MaxAttempts: 1, InitialWait: time.Millisecond, MaxWait: time.Millisecond},
		providerfoundation.LeaseGuardFunc(func(context.Context) error { return nil }),
	)
	if err != nil {
		t.Fatal(err)
	}
	cache := make(map[string]jiraProjectCatalogEntry)
	requests := 0

	for i := 0; i < 3; i++ {
		if _, ok := resolveJiraProjectCatalog(
			context.Background(), client, cache, &requests,
			"10001", "Operations (changelog name)", "10999", "OTHER",
		); !ok {
			t.Fatalf("iteration %d: want resolved", i)
		}
	}
	if requests != 1 {
		t.Fatalf("requests=%d, want exactly 1 -- the failing entry must be cached after its first lookup", requests)
	}

	upgraded, ok := resolveJiraProjectCatalog(
		context.Background(), client, cache, &requests,
		"10001", "Operations (changelog name)", "10001", "OPS",
	)
	if !ok || upgraded.Key != "OPS" {
		t.Fatalf("upgraded=%+v ok=%v, want key OPS -- a cache hit must still upgrade via the known-key fallback", upgraded, ok)
	}
	if requests != 1 {
		t.Fatalf("requests=%d, want still exactly 1 -- upgrading a cache hit must not issue a new live lookup", requests)
	}
}

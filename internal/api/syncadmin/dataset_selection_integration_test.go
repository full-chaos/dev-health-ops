//go:build integration

package syncadmin

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"reflect"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/integrationsadmin"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/containers"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// selectionVenue is the sync admin routes and the integration dataset
// endpoint, as the api mounts them, over one migrated Postgres. orgOn has
// the canonical-incident feature, orgOff does not.
type selectionVenue struct {
	t             *testing.T
	ctx           context.Context
	pool          *pgxpool.Pool
	base          string
	orgOn, orgOff string
	sequence      int
}

func startSelectionVenue(t *testing.T) *selectionVenue {
	t.Helper()
	ctx := context.Background()
	instance, err := containers.StartPostgres(ctx)
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	t.Cleanup(func() { _ = instance.Close(context.Background()) })
	pool, err := pgxpool.New(ctx, instance.URI)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	pgschema.Apply(ctx, t, pool)
	v := &selectionVenue{t: t, ctx: ctx, pool: pool, orgOn: uuid.NewString(), orgOff: uuid.NewString()}
	var feature string
	if err := pool.QueryRow(ctx, `SELECT id::text FROM feature_flags WHERE key = $1`, canonicalIncidentFeatureKey).Scan(&feature); err != nil {
		t.Fatalf("the migrated schema has no %s feature flag: %v", canonicalIncidentFeatureKey, err)
	}
	for org, enabled := range map[string]bool{v.orgOn: true, v.orgOff: false} {
		pgseed.Org(ctx, t, pool, org, "enterprise")
		pgseed.OrgOverride(ctx, t, pool, org, feature, enabled)
	}
	guard := testGuard(t)
	mux := http.NewServeMux()
	mount := func(routes []httpapi.Route) {
		for _, route := range routes {
			mux.Handle(route.Method+" "+route.Pattern, route.Handler)
		}
	}
	mount(Routes(Deps{Pool: pool, Guard: guard, Logger: quiet()}))
	mount(integrationsadmin.Routes(integrationsadmin.Deps{Pool: pool, Guard: guard, Logger: quiet()}))
	server := httptest.NewServer(mux)
	t.Cleanup(server.Close)
	v.base = server.URL
	// The harness must tell the two orgs apart, or every gate case below
	// proves nothing: the gated targets are offered to orgOn only.
	_, on := v.call(v.orgOn, "GET", "/api/v1/admin/sync-targets", "")
	_, off := v.call(v.orgOff, "GET", "/api/v1/admin/sync-targets", "")
	if !strings.Contains(on, `"incidents"`) || strings.Contains(off, `"incidents"`) || strings.Contains(off, `"operational"`) {
		t.Fatalf("harness: the canonical-incident feature is not on for orgOn and off for orgOff:\non  %s\noff %s", on, off)
	}
	return v
}

func (v *selectionVenue) exec(sql string, args ...any) {
	v.t.Helper()
	if _, err := v.pool.Exec(v.ctx, sql, args...); err != nil {
		v.t.Fatalf("seed: %v\n%s", err, sql)
	}
}

func (v *selectionVenue) call(org, method, path, body string) (int, string) {
	v.t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewReader([]byte(body))
	}
	request, err := http.NewRequest(method, v.base+path, reader)
	if err != nil {
		v.t.Fatal(err)
	}
	request.Header.Set("Authorization", "Bearer "+sign(v.t, "admin", org))
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		v.t.Fatal(err)
	}
	defer response.Body.Close()
	text, err := io.ReadAll(response.Body)
	if err != nil {
		v.t.Fatal(err)
	}
	return response.StatusCode, string(text)
}

// integration seeds one integration of the org with its dataset rows: on are
// enabled rows, off are rows that exist and are disabled.
func (v *selectionVenue) integration(org, provider string, on, off []string) uuid.UUID {
	v.t.Helper()
	v.sequence++
	id := uuid.New()
	v.exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, $3, $4, '{}'::json, true, now(), now())`, id, org, provider, fmt.Sprintf("integration %d", v.sequence))
	for _, key := range on {
		v.row(org, id, key, true)
	}
	for _, key := range off {
		v.row(org, id, key, false)
	}
	return id
}

func (v *selectionVenue) row(org string, integration uuid.UUID, key string, enabled bool) {
	v.t.Helper()
	v.exec(`INSERT INTO integration_datasets (id, org_id, integration_id, dataset_key, is_enabled, options) VALUES ($1, $2, $3, $4, $5, '{}'::json)`,
		uuid.New(), org, integration, key, enabled)
}

// config seeds a sync configuration: a whole-integration one, or a child
// pinned to a new source of the integration when child is true.
func (v *selectionVenue) config(org, provider string, integration uuid.UUID, stored string, child bool) uuid.UUID {
	v.t.Helper()
	v.sequence++
	id := uuid.New()
	var source any
	if child {
		sourceID := uuid.New()
		external := fmt.Sprintf("acme/r%d", v.sequence)
		v.exec(`INSERT INTO integration_sources (id, org_id, integration_id, provider, source_type, external_id, name, full_name, metadata,
is_enabled, discovered_at, last_seen_at) VALUES ($1, $2, $3, $4, 'repository', $5, $5, $5, '{}'::json, true, now(), now())`,
			sourceID, org, integration, strings.ToLower(provider), external)
		source = sourceID
	}
	v.exec(`INSERT INTO sync_configurations (id, org_id, name, provider, sync_targets, sync_options, is_active, planner_managed,
integration_id, source_id, created_at, updated_at) VALUES ($1, $2, $3, $4, $5::json, '{}'::json, true, $6, $7, $8, now(), now())`,
		id, org, fmt.Sprintf("config %d", v.sequence), provider, stored, !child, integration, source)
	return id
}

// rows is the state of every dataset row of the integration: id, key and
// is_enabled, in key order. Equal strings = no row was inserted, deleted or
// switched.
func (v *selectionVenue) rows(integration uuid.UUID) string {
	v.t.Helper()
	result, err := v.pool.Query(v.ctx, `SELECT id::text || ' ' || dataset_key || '=' || CASE WHEN is_enabled THEN 'on' ELSE 'off' END
FROM integration_datasets WHERE integration_id = $1 ORDER BY dataset_key COLLATE "C"`, integration)
	if err != nil {
		v.t.Fatal(err)
	}
	lines, err := pgx.CollectRows(result, pgx.RowTo[string])
	if err != nil {
		v.t.Fatal(err)
	}
	return strings.Join(lines, "\n")
}

// states is key -> on/off for the integration's rows (no ids).
func (v *selectionVenue) states(integration uuid.UUID) map[string]bool {
	v.t.Helper()
	result, err := v.pool.Query(v.ctx, `SELECT dataset_key, is_enabled FROM integration_datasets WHERE integration_id = $1`, integration)
	if err != nil {
		v.t.Fatal(err)
	}
	defer result.Close()
	out := map[string]bool{}
	for result.Next() {
		var key string
		var enabled bool
		if err := result.Scan(&key, &enabled); err != nil {
			v.t.Fatal(err)
		}
		out[key] = enabled
	}
	if err := result.Err(); err != nil {
		v.t.Fatal(err)
	}
	return out
}

func (v *selectionVenue) stored(config uuid.UUID) string {
	v.t.Helper()
	var text string
	if err := v.pool.QueryRow(v.ctx, `SELECT sync_targets::text FROM sync_configurations WHERE id = $1`, config).Scan(&text); err != nil {
		v.t.Fatal(err)
	}
	return text
}

// configRow is the whole stored config row, for "nothing was written".
func (v *selectionVenue) configRow(config uuid.UUID) string {
	v.t.Helper()
	var text string
	if err := v.pool.QueryRow(v.ctx, `SELECT to_jsonb(c)::text FROM sync_configurations c WHERE id = $1`, config).Scan(&text); err != nil {
		v.t.Fatal(err)
	}
	return text
}

func targetsOf(t *testing.T, body string) []string {
	t.Helper()
	var decoded struct {
		SyncTargets *[]string `json:"sync_targets"`
	}
	if err := json.Unmarshal([]byte(body), &decoded); err != nil || decoded.SyncTargets == nil {
		t.Fatalf("no sync_targets list in %s (%v)", body, err)
	}
	return *decoded.SyncTargets
}

func (v *selectionVenue) get(org string, config uuid.UUID) []string {
	v.t.Helper()
	status, body := v.call(org, "GET", "/api/v1/admin/sync-configs/"+config.String(), "")
	if status != http.StatusOK {
		v.t.Fatalf("GET config: %d %s", status, body)
	}
	return targetsOf(v.t, body)
}

func (v *selectionVenue) patch(org string, config uuid.UUID, body string) (int, string) {
	v.t.Helper()
	return v.call(org, "PATCH", "/api/v1/admin/sync-configs/"+config.String(), body)
}

// save PATCHes and requires 200; it returns the response's list.
func (v *selectionVenue) save(org string, config uuid.UUID, body string) []string {
	v.t.Helper()
	status, text := v.patch(org, config, body)
	if status != http.StatusOK {
		v.t.Fatalf("PATCH %s: %d %s", body, status, text)
	}
	return targetsOf(v.t, text)
}

// datasetEndpoint is PATCH /integrations/{id}/datasets, the route that
// switches one dataset row directly.
func (v *selectionVenue) datasetEndpoint(org string, integration uuid.UUID, key string, enabled bool) {
	v.t.Helper()
	status, body := v.call(org, "PATCH", "/api/v1/admin/integrations/"+integration.String()+"/datasets",
		fmt.Sprintf(`{"datasets":[{"dataset_key":%q,"is_enabled":%v}]}`, key, enabled))
	if status != http.StatusOK {
		v.t.Fatalf("dataset endpoint %s=%v: %d %s", key, enabled, status, body)
	}
}

// explicitRequestInsert is the row an explicitly requested dataset key gets
// at plan time when it has none (the scheduler's
// ensureExplicitlyRequestedDataset): enabled, and nothing when a row exists.
// The statement is the scheduler's own; the writer census pins that function.
func (v *selectionVenue) explicitRequestInsert(org string, integration uuid.UUID, key string) {
	v.t.Helper()
	v.exec(`
INSERT INTO public.integration_datasets (id,org_id,integration_id,dataset_key,is_enabled,options)
VALUES ($1::uuid,$2,$3::uuid,$4,TRUE,'{}'::jsonb)
ON CONFLICT (org_id,integration_id,dataset_key) DO NOTHING`, uuid.NewString(), org, integration.String(), key)
}

func listBody(targets []string, extra string) string {
	encoded, _ := json.Marshal(targets)
	if targets == nil {
		encoded = []byte("[]")
	}
	return `{"sync_targets":` + string(encoded) + extra + `}`
}

func withBase(base []string) string {
	encoded, _ := json.Marshal(base)
	if base == nil {
		encoded = []byte("[]")
	}
	return `,"sync_targets_base":` + string(encoded)
}

func targetKeys(t *testing.T, provider string, targets ...string) []string {
	t.Helper()
	keys, err := providersync.PlannerDatasetKeys(provider, targets)
	if err != nil || len(keys) == 0 {
		t.Fatalf("%s %v: keys %v err %v", provider, targets, keys, err)
	}
	return keys
}

func listWithout(all []string, drop ...string) []string {
	dropped := map[string]bool{}
	for _, item := range drop {
		dropped[item] = true
	}
	out := []string{}
	for _, item := range all {
		if !dropped[item] {
			out = append(out, item)
		}
	}
	return out
}

func sameSet(left, right []string) bool {
	a, b := append([]string{}, left...), append([]string{}, right...)
	sort.Strings(a)
	sort.Strings(b)
	return reflect.DeepEqual(a, b)
}

// selectionSeed is one provider's realistic row state: some controlled rows
// on, some present and OFF, rows no checkbox reaches, and a stored list that
// does NOT agree with the rows.
type selectionSeed struct {
	provider string
	on, off  []string
	stored   string
	// shown is the list the config must show: the stored targets a row is ON
	// for (or that have no dataset), then the targets only the ON rows give.
	shown []string
}

var selectionSeeds = []selectionSeed{
	// The production shape of a GitHub integration: git, prs, cicd, tests on;
	// the five work-item rows present and OFF (the migration 0108 shape); the
	// automatic security row; an "incidents" row GitHub has no dataset for;
	// the stored list names work-items (off) and not cicd/tests (on).
	{provider: "github",
		on:     []string{"repo-metadata", "commits", "commit-stats", "files", "blame", "prs", "pr-reviews", "pr-comments", "cicd", "tests", "security", "incidents"},
		off:    []string{"work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments", "deployments"},
		stored: `["git", "prs", "work-items", "incidents"]`,
		shown:  []string{"git", "prs", "incidents", "cicd", "tests"}},
	// GitLab: a mixed git family (no blame row), a mixed prs family (the
	// canonical row off, a member on), feature-flags on, incidents off.
	{provider: "gitlab",
		on:     []string{"repo-metadata", "commits", "commit-stats", "files", "pr-comments", "feature-flags", "security"},
		off:    []string{"prs", "pr-reviews", "incidents", "cicd"},
		stored: `["git", "cicd"]`,
		shown:  []string{"git", "prs", "feature-flags"}},
	// Jira: the work-item family on, and an incidents row on (target
	// "operational", which the Jira form has no checkbox for).
	{provider: "jira",
		on:     []string{"work-items", "work-item-labels", "work-item-projects", "work-item-history", "incidents"},
		off:    []string{"work-item-comments"},
		stored: `["work-items"]`,
		shown:  []string{"work-items", "operational"}},
	// Linear: every row present and OFF, the stored list still names the
	// target: the config shows nothing.
	{provider: "linear",
		off:    []string{"work-items", "work-item-labels", "work-item-projects", "work-item-history", "work-item-comments"},
		stored: `["work-items"]`,
		shown:  []string{}},
	{provider: "launchdarkly", on: []string{"feature-flags"}, stored: `[]`, shown: []string{"feature-flags"}},
}

func (v *selectionVenue) seeded(org string, seed selectionSeed) (integration, config uuid.UUID) {
	v.t.Helper()
	integration = v.integration(org, seed.provider, seed.on, seed.off)
	return integration, v.config(org, seed.provider, integration, seed.stored, false)
}

// TestDatasetRowsOwnTheSyncSelection drives the real PATCH and GET
// /sync-configs handlers, and the integration dataset endpoint, over a
// migrated Postgres. Every case asserts the dataset rows and the stored
// config after the request, never only the response (CHAOS-8816).
func TestDatasetRowsOwnTheSyncSelection(t *testing.T) {
	v := startSelectionVenue(t)

	// GET shows the state of the rows: the stored targets an ENABLED row
	// speaks for (or that have no dataset), then the targets only the enabled
	// rows give. A stored target whose rows are all off is not shown.
	t.Run("the config shows the state of its rows", func(t *testing.T) {
		v.t = t
		for _, seed := range selectionSeeds {
			_, config := v.seeded(v.orgOn, seed)
			if got := v.get(v.orgOn, config); !reflect.DeepEqual(got, seed.shown) {
				t.Errorf("%s: GET shows %v, want %v", seed.provider, got, seed.shown)
			}
		}
	})

	// Saving the list the config shows, with other settings changed, changes
	// no dataset row. A target the list shows only because a row is on is
	// never stored, and a stored target the list does not show (its rows are
	// off) leaves the stored list: the rest of the stored list stays.
	t.Run("a save of the shown list with other settings changes no row", func(t *testing.T) {
		v.t = t
		for _, seed := range selectionSeeds {
			integration, config := v.seeded(v.orgOn, seed)
			before := v.rows(integration)
			shown := v.get(v.orgOn, config)
			for _, extra := range []string{``, `,"schedule_cron":"0 */6 * * *","timezone":"Europe/Paris"`, `,"initial_sync_depth":30`, `,"is_active":false`} {
				got := v.save(v.orgOn, config, listBody(shown, extra))
				if after := v.rows(integration); after != before {
					t.Fatalf("%s: a save of the shown list%s changed dataset rows\nbefore:\n%s\nafter:\n%s", seed.provider, extra, before, after)
				}
				if !reflect.DeepEqual(got, seed.shown) || !reflect.DeepEqual(v.get(v.orgOn, config), seed.shown) {
					t.Errorf("%s: the save answers %v, want %v", seed.provider, got, seed.shown)
				}
			}
			var before2, after2 []string
			if err := json.Unmarshal([]byte(seed.stored), &before2); err != nil {
				t.Fatal(err)
			}
			want := []string{}
			for _, target := range before2 {
				if slices.Contains(seed.shown, target) {
					want = append(want, target)
				}
			}
			if err := json.Unmarshal([]byte(v.stored(config)), &after2); err != nil || !reflect.DeepEqual(after2, want) {
				t.Errorf("%s: the stored list is %s, want the items of %s that the config shows: %v", seed.provider, v.stored(config), seed.stored, want)
			}
		}
	})

	// A row a side path switched on (the dataset endpoint; an explicit
	// dataset request at plan time) survives every kind of save, for the four
	// work-tracking and code providers.
	t.Run("a row a side path enabled survives a save", func(t *testing.T) {
		v.t = t
		for _, testCase := range []struct {
			provider        string
			on, off         []string
			stored          string
			endpointKey     string // switched on through the dataset endpoint
			explicitKey     string // inserted enabled as an explicit request does
			mustStayOff     string // a present-and-off row of the same family
			wantShownToHold []string
		}{
			{"github", targetKeys(t, "github", "git"), []string{"prs", "pr-reviews", "cicd"}, `["git"]`, "cicd", "pr-comments", "prs", []string{"git", "cicd", "prs"}},
			{"gitlab", targetKeys(t, "gitlab", "git"), []string{"work-items", "deployments"}, `["git"]`, "deployments", "work-item-labels", "work-items", []string{"git", "deployments", "work-items"}},
			{"jira", []string{"work-items"}, []string{"work-item-history", "incidents"}, `["work-items"]`, "incidents", "work-item-comments", "work-item-history", []string{"work-items", "operational"}},
			{"linear", nil, []string{"work-items", "work-item-labels"}, `[]`, "work-item-labels", "work-item-projects", "work-items", []string{"work-items"}},
		} {
			integration := v.integration(v.orgOn, testCase.provider, testCase.on, testCase.off)
			config := v.config(v.orgOn, testCase.provider, integration, testCase.stored, false)
			v.datasetEndpoint(v.orgOn, integration, testCase.endpointKey, true)
			v.explicitRequestInsert(v.orgOn, integration, testCase.explicitKey)
			before := v.rows(integration)
			if states := v.states(integration); !states[testCase.endpointKey] || !states[testCase.explicitKey] || states[testCase.mustStayOff] {
				t.Fatalf("%s harness: side-path rows not as seeded: %v", testCase.provider, states)
			}
			shown := v.get(v.orgOn, config)
			if !sameSet(shown, testCase.wantShownToHold) {
				t.Errorf("%s: GET shows %v, want %v", testCase.provider, shown, testCase.wantShownToHold)
			}
			for _, body := range []string{`{"is_active":false}`, `{"is_active":true,"initial_sync_depth":14}`, `{"schedule_cron":"0 3 * * *"}`,
				listBody(shown, ``), listBody(shown, `,"timezone":"UTC"`), listBody(shown, withBase(shown))} {
				v.save(v.orgOn, config, body)
				if after := v.rows(integration); after != before {
					t.Fatalf("%s: save %s changed dataset rows\nbefore:\n%s\nafter:\n%s", testCase.provider, body, before, after)
				}
			}
		}
	})

	// Uncheck one target: exactly the enabled rows of that target go off.
	// Check it again: exactly its keys are on, also a row that was present
	// and off, and a row that did not exist. Every provider, every target.
	t.Run("uncheck and check write exactly the rows of the target", func(t *testing.T) {
		v.t = t
		cases := 0
		for _, provider := range rowOwnedProviders {
			all := registryKeys(provider)
			for _, target := range providersync.SupportedLegacyTargets(provider) {
				if !providersync.OperatorSelectableSyncTarget(target) {
					continue
				}
				cases++
				keys := targetKeys(t, provider, target)
				// Every registry row on, plus a row for a key the provider has no dataset for.
				integration := v.integration(v.orgOn, provider, append(append([]string{}, all...), "not-a-dataset"), nil)
				config := v.config(v.orgOn, provider, integration, `[]`, false)
				shown := v.get(v.orgOn, config)
				before := v.states(integration)
				got := v.save(v.orgOn, config, listBody(listWithout(shown, target), ``))
				after := v.states(integration)
				for key, was := range before {
					want := was
					for _, own := range keys {
						if key == own {
							want = false
						}
					}
					if after[key] != want {
						t.Errorf("%s uncheck %s: row %s is %v, want %v", provider, target, key, after[key], want)
					}
				}
				if len(after) != len(before) || !sameSet(got, listWithout(shown, target)) {
					t.Errorf("%s uncheck %s: %d rows (was %d), answer %v", provider, target, len(after), len(before), got)
				}
				// Check it again from the list now shown.
				got = v.save(v.orgOn, config, listBody(append(append([]string{}, got...), target), ``))
				if again := v.states(integration); !reflect.DeepEqual(again, before) || !sameSet(got, shown) {
					t.Errorf("%s check %s again: rows %v, want %v; answer %v, want %v", provider, target, again, before, got, shown)
				}

				// A fresh integration: the first key of the target present and
				// OFF, its other keys missing, one key of another target on.
				other := ""
				for _, key := range all {
					if !strings.Contains(","+strings.Join(keys, ",")+",", ","+key+",") {
						other = key
						break
					}
				}
				on := []string{}
				if other != "" {
					on = append(on, other)
				}
				integration = v.integration(v.orgOn, provider, on, keys[:1])
				config = v.config(v.orgOn, provider, integration, `[]`, false)
				shown = v.get(v.orgOn, config)
				v.save(v.orgOn, config, listBody(append(append([]string{}, shown...), target), ``))
				states := v.states(integration)
				want := map[string]bool{}
				for _, key := range keys {
					want[key] = true
				}
				if other != "" {
					want[other] = true
				}
				if !reflect.DeepEqual(states, want) {
					t.Errorf("%s check %s on a fresh integration: rows %v, want %v", provider, target, states, want)
				}
			}
		}
		if cases == 0 {
			t.Fatal("no provider/target case ran")
		}
	})

	// A child config (pinned to one source): its list narrows, it is stored
	// as submitted, it is shown as stored, and its save writes no row.
	t.Run("a child config keeps its stored list and writes no row", func(t *testing.T) {
		v.t = t
		for _, testCase := range []struct{ provider, submitted string }{
			{"github", `["cicd"]`}, {"gitlab", `["prs","deployments"]`}, {"jira", `[]`}, {"linear", `[]`},
		} {
			seed := selectionSeeds[0]
			for _, candidate := range selectionSeeds {
				if candidate.provider == testCase.provider {
					seed = candidate
				}
			}
			integration := v.integration(v.orgOn, seed.provider, seed.on, seed.off)
			child := v.config(v.orgOn, seed.provider, integration, seed.stored, true)
			before := v.rows(integration)
			var storedList []string
			_ = json.Unmarshal([]byte(seed.stored), &storedList)
			if got := v.get(v.orgOn, child); !reflect.DeepEqual(got, storedList) {
				t.Errorf("%s child: GET shows %v, want the stored list %v", seed.provider, got, storedList)
			}
			var submitted []string
			_ = json.Unmarshal([]byte(testCase.submitted), &submitted)
			got := v.save(v.orgOn, child, `{"sync_targets":`+testCase.submitted+`,"sync_targets_base":["git"]}`)
			if after := v.rows(integration); after != before {
				t.Errorf("%s child: the save changed dataset rows\nbefore:\n%s\nafter:\n%s", seed.provider, before, after)
			}
			var mirror []string
			_ = json.Unmarshal([]byte(v.stored(child)), &mirror)
			if !reflect.DeepEqual(got, submitted) || !reflect.DeepEqual(mirror, submitted) {
				t.Errorf("%s child: answer %v stored %v, want the submitted list %v", seed.provider, got, mirror, submitted)
			}
		}
	})

	// PagerDuty: the platform owns the rows. A save writes no dataset row,
	// the list is stored as submitted and shown as stored.
	t.Run("a pagerduty save writes no dataset row", func(t *testing.T) {
		v.t = t
		keys := targetKeys(t, "pagerduty", "operational")
		// One operational row is off (through the dataset endpoint): a save
		// that ran the row rule would switch it on again.
		integration := v.integration(v.orgOn, "pagerduty", keys, nil)
		config := v.config(v.orgOn, "pagerduty", integration, `["operational"]`, false)
		v.datasetEndpoint(v.orgOn, integration, "services", false)
		before := v.rows(integration)
		for _, submitted := range [][]string{{"operational"}, {}} {
			got := v.save(v.orgOn, config, listBody(submitted, withBase([]string{"operational"})))
			if after := v.rows(integration); after != before {
				t.Fatalf("pagerduty save %v changed dataset rows\nbefore:\n%s\nafter:\n%s", submitted, before, after)
			}
			var mirror []string
			_ = json.Unmarshal([]byte(v.stored(config)), &mirror)
			if !reflect.DeepEqual(got, submitted) || !reflect.DeepEqual(mirror, submitted) || !reflect.DeepEqual(v.get(v.orgOn, config), submitted) {
				t.Errorf("pagerduty save %v: answer %v stored %v", submitted, got, mirror)
			}
		}
	})

	// The stored column after a save holds only what requests asked for: the
	// submitted targets that were stored or that the save adds, in the
	// submitted order; never a target the list shows because a row is on.
	t.Run("the stored list after a save is what requests asked for", func(t *testing.T) {
		v.t = t
		integration := v.integration(v.orgOn, "github", append(targetKeys(t, "github", "git"), "cicd"), []string{"prs"})
		config := v.config(v.orgOn, "github", integration, `["git", "incidents"]`, false)
		storedList := func() []string {
			var out []string
			if err := json.Unmarshal([]byte(v.stored(config)), &out); err != nil {
				t.Fatal(err)
			}
			return out
		}
		if got, want := v.get(v.orgOn, config), []string{"git", "incidents", "cicd"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("harness: GET shows %v, want %v", got, want)
		}
		// The shown list sent back: cicd (a row-only target) is not stored.
		got := v.save(v.orgOn, config, listBody([]string{"git", "incidents", "cicd"}, ``))
		if want := []string{"git", "incidents"}; !reflect.DeepEqual(got, []string{"git", "incidents", "cicd"}) || !reflect.DeepEqual(storedList(), want) {
			t.Errorf("the shown list: answer %v stored %v, want stored %v", got, storedList(), want)
		}
		// GitHub "incidents" has no dataset: it stays until it is unchecked.
		// cicd is sent back and stays a row-only target.
		got = v.save(v.orgOn, config, listBody([]string{"git", "cicd"}, ``))
		if want := []string{"git"}; !reflect.DeepEqual(got, []string{"git", "cicd"}) || !reflect.DeepEqual(storedList(), want) {
			t.Errorf("incidents unchecked: answer %v stored %v, want stored %v", got, storedList(), want)
		}
		// The save adds incidents and prs: both are stored, in the submitted order.
		got = v.save(v.orgOn, config, listBody([]string{"incidents", "cicd", "prs", "git"}, ``))
		if want := []string{"incidents", "prs", "git"}; !reflect.DeepEqual(storedList(), want) || !reflect.DeepEqual(got, []string{"incidents", "prs", "git", "cicd"}) {
			t.Errorf("incidents and prs checked: answer %v stored %v, want stored %v (the submitted order)", got, storedList(), want)
		}
		if states := v.states(integration); !states["prs"] || !states["cicd"] {
			t.Errorf("incidents and prs checked: rows %v, want prs on and cicd on", states)
		}
		// A save with no sync_targets does not write the column, and still
		// answers the list the rows show.
		v.datasetEndpoint(v.orgOn, integration, "deployments", true)
		storedBefore := v.stored(config)
		status, body := v.patch(v.orgOn, config, `{"initial_sync_depth":7}`)
		if status != http.StatusOK || v.stored(config) != storedBefore {
			t.Fatalf("a save with no sync_targets: %d, stored %s (was %s)", status, v.stored(config), storedBefore)
		}
		if got, want := targetsOf(t, body), []string{"incidents", "prs", "git", "cicd", "deployments"}; !reflect.DeepEqual(got, want) {
			t.Errorf("a save with no sync_targets answers %v, want %v", got, want)
		}
	})

	// Two forms hold one baseline. Form 1 unchecks prs. Form 2 changes only
	// the schedule and sends the list it holds: form 2 re-applies its list
	// (as on main, whose save rebuilt every row from the list). A base list in
	// the body changes nothing. Pinned so that a change of it fails here.
	t.Run("two saves from one baseline", func(t *testing.T) {
		v.t = t
		prs := targetKeys(t, "github", "prs")
		for _, sendBase := range []bool{true, false} {
			integration := v.integration(v.orgOn, "github", append(targetKeys(t, "github", "git"), prs...), nil)
			config := v.config(v.orgOn, "github", integration, `["git", "prs"]`, false)
			formOne, formTwo := v.get(v.orgOn, config), v.get(v.orgOn, config)
			if !reflect.DeepEqual(formOne, []string{"git", "prs"}) || !reflect.DeepEqual(formTwo, formOne) {
				t.Fatalf("harness: both forms must hold [git prs]: %v %v", formOne, formTwo)
			}
			base := func(list []string) string {
				if sendBase {
					return withBase(list)
				}
				return ``
			}
			v.save(v.orgOn, config, listBody([]string{"git"}, base(formOne)))
			for _, key := range prs {
				if v.states(integration)[key] {
					t.Fatalf("harness: save 1 did not switch %s off", key)
				}
			}
			got := v.save(v.orgOn, config, listBody(formTwo, base(formTwo)+`,"initial_sync_depth":21`))
			states := v.states(integration)
			for _, key := range prs {
				if !states[key] {
					t.Errorf("base sent=%v: after save 2 the %s row is off, want on (form 2 adds prs to the list the server shows)", sendBase, key)
				}
			}
			if want := []string{"git", "prs"}; !reflect.DeepEqual(got, want) || v.stored(config) != `["git", "prs"]` {
				t.Errorf("base sent=%v: save 2 answers %v stored %s, want %v", sendBase, got, v.stored(config), want)
			}
		}
	})

	// A side path changes rows while the form is open: the dataset endpoint
	// switches X off and Y on. A save of the form's list is read against the
	// list the server shows NOW: a target the form holds and the server no
	// longer shows is added (its rows go on), a target the server shows and
	// the form does not hold is removed (its rows go off), and a row whose
	// target is shown before and after keeps its state. A base list in the
	// body changes nothing.
	t.Run("a form loaded before a side-path change re-applies its list", func(t *testing.T) {
		v.t = t
		for _, testCase := range []struct {
			provider, x, y string
			on, off        []string
			wantX, wantY   bool
		}{
			{"github", "cicd", "deployments", append(targetKeys(t, "github", "git"), "cicd"), []string{"deployments"}, true, false},
			{"gitlab", "feature-flags", "incidents", append(targetKeys(t, "gitlab", "git"), "feature-flags"), nil, true, false},
			// work-item-comments is a member of the shown work-items target.
			{"jira", "incidents", "work-item-comments", []string{"work-items", "incidents"}, nil, true, true},
			// Both rows are members of the shown work-items target.
			{"linear", "work-item-labels", "work-item-history", []string{"work-items", "work-item-labels"}, []string{"work-item-history"}, false, true},
		} {
			for _, sendBase := range []bool{false, true} {
				integration := v.integration(v.orgOn, testCase.provider, testCase.on, testCase.off)
				config := v.config(v.orgOn, testCase.provider, integration, `[]`, false)
				form := v.get(v.orgOn, config)
				v.datasetEndpoint(v.orgOn, integration, testCase.x, false)
				v.datasetEndpoint(v.orgOn, integration, testCase.y, true)
				extra := `,"initial_sync_depth":3`
				if sendBase {
					extra += withBase(form)
				}
				v.save(v.orgOn, config, listBody(form, extra))
				if states := v.states(integration); states[testCase.x] != testCase.wantX || states[testCase.y] != testCase.wantY {
					t.Errorf("%s base sent=%v: %s on=%v (want %v), %s on=%v (want %v)", testCase.provider, sendBase,
						testCase.x, states[testCase.x], testCase.wantX, testCase.y, states[testCase.y], testCase.wantY)
				}
			}
		}
	})

	// The canonical-incident gate, for an org WITHOUT the feature: it reads
	// the targets the save stores (what the user adds and what was stored),
	// never a target the list shows only because a row is on.
	t.Run("the incident gate reads the targets the save stores", func(t *testing.T) {
		v.t = t
		refused := func(what string, integration, config uuid.UUID, body string) {
			t.Helper()
			rows, row := v.rows(integration), v.configRow(config)
			status, text := v.patch(v.orgOff, config, body)
			if status != http.StatusForbidden || !strings.Contains(text, "feature_disabled") {
				t.Errorf("%s: %d %s, want 403 feature_disabled", what, status, text)
			}
			if v.rows(integration) != rows || v.configRow(config) != row {
				t.Errorf("%s: the refused save wrote something", what)
			}
		}
		// Jira: an incidents row is on; the form echoes "operational".
		jira := v.integration(v.orgOff, "jira", []string{"work-items", "incidents"}, nil)
		jiraConfig := v.config(v.orgOff, "jira", jira, `["work-items"]`, false)
		shown := v.get(v.orgOff, jiraConfig)
		if !reflect.DeepEqual(shown, []string{"work-items", "operational"}) {
			t.Fatalf("harness: jira shows %v", shown)
		}
		before := v.rows(jira)
		v.save(v.orgOff, jiraConfig, listBody(shown, `,"initial_sync_depth":30`))
		v.save(v.orgOff, jiraConfig, `{"is_active":false}`)
		if v.rows(jira) != before || v.stored(jiraConfig) != `["work-items"]` {
			t.Errorf("jira: a save with the incidents row on changed rows or stored the row's target: stored %s", v.stored(jiraConfig))
		}
		// Jira: the user ADDS operational (the row is off): refused.
		jiraOff := v.integration(v.orgOff, "jira", []string{"work-items"}, []string{"incidents"})
		jiraOffConfig := v.config(v.orgOff, "jira", jiraOff, `["work-items"]`, false)
		refused("jira adds operational", jiraOff, jiraOffConfig, `{"sync_targets":["work-items","operational"]}`)

		// GitLab: an incidents row is on: a save of the shown list passes;
		// with the row off, adding incidents is refused.
		gitlab := v.integration(v.orgOff, "gitlab", []string{"commits", "incidents"}, nil)
		gitlabConfig := v.config(v.orgOff, "gitlab", gitlab, `["git"]`, false)
		before = v.rows(gitlab)
		v.save(v.orgOff, gitlabConfig, listBody(v.get(v.orgOff, gitlabConfig), `,"initial_sync_depth":30`))
		if v.rows(gitlab) != before {
			t.Errorf("gitlab: a save with the incidents row on changed rows")
		}
		gitlabOff := v.integration(v.orgOff, "gitlab", []string{"commits"}, []string{"incidents"})
		gitlabOffConfig := v.config(v.orgOff, "gitlab", gitlabOff, `["git"]`, false)
		refused("gitlab adds incidents", gitlabOff, gitlabOffConfig, `{"sync_targets":["git","incidents"]}`)
		refused("gitlab adds incidents, base sent", gitlabOff, gitlabOffConfig, `{"sync_targets":["git","incidents"],"sync_targets_base":["git"]}`)

		// GitHub: "incidents" is stored and has no dataset. Unchanged list, and
		// a save with no list: refused (as before this change). Unchecked: 200.
		github := v.integration(v.orgOff, "github", []string{"commits"}, nil)
		githubConfig := v.config(v.orgOff, "github", github, `["git", "incidents"]`, false)
		refused("github stored incidents, list unchanged", github, githubConfig, `{"sync_targets":["git","incidents"],"initial_sync_depth":30}`)
		refused("github stored incidents, no list", github, githubConfig, `{"is_active":false}`)
		if got := v.save(v.orgOff, githubConfig, `{"sync_targets":["git"]}`); !reflect.DeepEqual(got, []string{"git"}) {
			t.Errorf("github incidents unchecked: answer %v", got)
		}
		refused("github adds incidents", github, githubConfig, `{"sync_targets":["git","incidents"]}`)
		// The gate is on, not off, for the same requests in an org with the feature.
		githubOn := v.integration(v.orgOn, "github", []string{"commits"}, nil)
		githubOnConfig := v.config(v.orgOn, "github", githubOn, `["git"]`, false)
		if got := v.save(v.orgOn, githubOnConfig, `{"sync_targets":["git","incidents"]}`); !reflect.DeepEqual(got, []string{"git", "incidents"}) {
			t.Errorf("org with the feature adds incidents: answer %v", got)
		}
	})

	// sync_targets_base has no meaning: whatever its value the save answers
	// 200 and does what the same body without the field does.
	t.Run("a base list in the body is ignored", func(t *testing.T) {
		v.t = t
		git := targetKeys(t, "github", "git")
		integration := v.integration(v.orgOn, "github", append(append([]string{}, git...), "cicd"), []string{"prs"})
		config := v.config(v.orgOn, "github", integration, `["git"]`, false)
		rows, stored := v.rows(integration), v.stored(config)
		for _, base := range []string{`null`, `"git"`, `["git",7]`, `{"git":true}`, `3`, `[null]`, `[]`, `["git","prs"]`} {
			if got := v.save(v.orgOn, config, `{"sync_targets":["git","cicd"],"sync_targets_base":`+base+`}`); !reflect.DeepEqual(got, []string{"git", "cicd"}) {
				t.Errorf("base %s: the save of the shown list answers %v", base, got)
			}
			if v.rows(integration) != rows || v.stored(config) != stored {
				t.Fatalf("base %s: the save of the shown list changed rows or the stored list (%s)", base, v.stored(config))
			}
		}
		// A base with no sync_targets: no row write.
		v.save(v.orgOn, config, `{"sync_targets_base":["git","prs","cicd"],"initial_sync_depth":9}`)
		if v.rows(integration) != rows {
			t.Errorf("a base with no sync_targets changed dataset rows")
		}
		// A base that names what the request drops and adds does not hide the
		// change: cicd is dropped from the shown list (row off) and prs is
		// added to it (rows on, target stored).
		v.save(v.orgOn, config, `{"sync_targets":["git","prs"],"sync_targets_base":["git","prs"]}`)
		states := v.states(integration)
		if states["cicd"] || !states["prs"] || !states["commits"] || v.stored(config) != `["git", "prs"]` {
			t.Errorf("a base equal to the submitted list: rows %v stored %s, want cicd off, prs on, commits on, [git prs] stored", states, v.stored(config))
		}
	})

	// The integration dataset endpoint switches exactly the row it names: it
	// creates a missing row with the given state (a registered key only),
	// switches an existing row both ways, answers 404 for a key the provider
	// has no dataset for, and a key repeated in one request ends in its last
	// state. No other row changes.
	t.Run("the dataset endpoint creates and switches exactly one row", func(t *testing.T) {
		v.t = t
		for _, provider := range []string{"github", "gitlab", "jira", "linear"} {
			integration := v.integration(v.orgOn, provider, []string{"work-items", "not-a-dataset"}, []string{"work-item-labels"})
			path := "/api/v1/admin/integrations/" + integration.String() + "/datasets"
			want := map[string]bool{"work-items": true, "not-a-dataset": true, "work-item-labels": false}
			check := func(what string) {
				t.Helper()
				if got := v.states(integration); !reflect.DeepEqual(got, want) {
					t.Fatalf("%s %s: rows %v, want %v", provider, what, got, want)
				}
			}
			v.datasetEndpoint(v.orgOn, integration, "work-item-labels", true) // an existing row, off -> on
			want["work-item-labels"] = true
			check("switch on")
			v.datasetEndpoint(v.orgOn, integration, "work-items", false) // an existing row, on -> off
			want["work-items"] = false
			check("switch off")
			v.datasetEndpoint(v.orgOn, integration, "work-item-history", false) // a missing row, created off
			want["work-item-history"] = false
			check("create off")
			v.datasetEndpoint(v.orgOn, integration, "work-item-comments", true) // a missing row, created on
			want["work-item-comments"] = true
			check("create on")
			v.datasetEndpoint(v.orgOn, integration, "not-a-dataset", false) // an existing row of an unsupported key
			want["not-a-dataset"] = false
			check("switch an unsupported key's row")
			status, body := v.call(v.orgOn, "PATCH", path, `{"datasets":[{"dataset_key":"work-items","is_enabled":true},{"dataset_key":"no-such-dataset","is_enabled":true}]}`)
			if status != http.StatusNotFound {
				t.Errorf("%s unsupported key with no row: %d %s, want 404", provider, status, body)
			}
			check("a refused request writes nothing")
			status, body = v.call(v.orgOn, "PATCH", path, `{"datasets":[{"dataset_key":"work-items","is_enabled":true},{"dataset_key":"work-items","is_enabled":false},{"dataset_key":"work-item-projects","is_enabled":false},{"dataset_key":"work-item-projects","is_enabled":true}]}`)
			if status != http.StatusOK {
				t.Fatalf("%s repeated key: %d %s", provider, status, body)
			}
			want["work-item-projects"] = true
			check("a repeated key ends in its last state")
		}
	})

	// Two whole-integration configs on one integration (the old shape): the
	// rows are per integration, so an uncheck in one is the setting of both.
	// The other config does not show the target any more (the rows are off),
	// though its stored list still names it, and a save of the list it shows
	// keeps the rows off and drops the target from its stored list. A check
	// of the target turns the rows on. (Main showed the stored list, and its
	// save of that list switched the rows on.)
	t.Run("sibling configs share one selection", func(t *testing.T) {
		v.t = t
		prs := targetKeys(t, "github", "prs")
		integration := v.integration(v.orgOn, "github", append(targetKeys(t, "github", "git"), prs...), nil)
		a := v.config(v.orgOn, "github", integration, `["git", "prs"]`, false)
		b := v.config(v.orgOn, "github", integration, `["git", "prs"]`, false)
		v.save(v.orgOn, a, `{"sync_targets":["git"]}`)
		for _, key := range prs {
			if v.states(integration)[key] {
				t.Errorf("the uncheck in config A left %s on (config B's stored list names prs)", key)
			}
		}
		shown := v.get(v.orgOn, b)
		if !reflect.DeepEqual(shown, []string{"git"}) || v.stored(b) != `["git", "prs"]` {
			t.Errorf("config B shows %v with the stored list %s, want [git] shown (the prs rows are off) and the stored list not changed by a read", shown, v.stored(b))
		}
		rows := v.rows(integration)
		v.save(v.orgOn, b, listBody(shown, `,"initial_sync_depth":5`))
		var storedB []string
		if err := json.Unmarshal([]byte(v.stored(b)), &storedB); err != nil || v.rows(integration) != rows || !reflect.DeepEqual(storedB, []string{"git"}) {
			t.Errorf("a save of the list config B shows: rows changed = %v, stored list %s; want no row write and [git] stored", v.rows(integration) != rows, v.stored(b))
		}
		// A check of the target: the rows go on again.
		v.save(v.orgOn, b, `{"sync_targets":["git","prs"]}`)
		for _, key := range prs {
			if !v.states(integration)[key] {
				t.Errorf("config B checked prs: the %s row is off", key)
			}
		}
	})

	// The list endpoint shows the same lists as the single read.
	t.Run("the list endpoint shows the same lists", func(t *testing.T) {
		v.t = t
		org := uuid.NewString()
		pgseed.Org(v.ctx, t, v.pool, org, "enterprise")
		want := map[string][]string{}
		for _, seed := range selectionSeeds {
			_, config := v.seeded(org, seed)
			want[config.String()] = seed.shown
		}
		pagerduty := v.integration(org, "pagerduty", targetKeys(t, "pagerduty", "operational"), nil)
		want[v.config(org, "pagerduty", pagerduty, `[]`, false).String()] = []string{}
		child := v.integration(org, "github", []string{"cicd"}, nil)
		want[v.config(org, "github", child, `["git"]`, true).String()] = []string{"git"}
		status, body := v.call(org, "GET", "/api/v1/admin/sync-configs", "")
		if status != http.StatusOK {
			t.Fatalf("list: %d %s", status, body)
		}
		var listed []struct {
			ID          string   `json:"id"`
			SyncTargets []string `json:"sync_targets"`
		}
		if err := json.Unmarshal([]byte(body), &listed); err != nil || len(listed) != len(want) {
			t.Fatalf("list has %d configs, want %d (%v)", len(listed), len(want), err)
		}
		for _, item := range listed {
			if !reflect.DeepEqual(item.SyncTargets, want[item.ID]) {
				t.Errorf("list: config %s shows %v, want %v", item.ID, item.SyncTargets, want[item.ID])
			}
		}
	})
}

//go:build integration

package syncadmin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/artifactexclude"
)

// artifactExcludeRuntime is the GitHub work-item runtime options as the PATCH snapshots them with no environment
// override, in the text the writers store (json.dumps).
const artifactExcludeRuntime = `"fetch_comments": true, "fetch_milestones": true, "comments_limit": 500`

type artifactExcludeTexts struct {
	options, config, workItems string
}

// seedArtifactExclude is a GitHub configuration whose stored runtime options already equal the PATCH's snapshot, so a
// PATCH that carries only ci_artifact_exclude_prefixes changes nothing else.
func (v *cascadeVenue) seedArtifactExclude(provider string) cascadeSeeded {
	v.t.Helper()
	one := v.seed(provider, `["tests"]`, []string{"tests", "work-items"})
	v.exec(`UPDATE sync_configurations SET sync_options = $2::json WHERE id = $1`, one.config,
		`{"schedule_cron": "0 * * * *", `+artifactExcludeRuntime+`}`)
	v.exec(`UPDATE integrations SET config = $2::json WHERE id = $1`, one.integration, `{`+artifactExcludeRuntime+`}`)
	v.exec(`UPDATE integration_datasets SET options = $2::json WHERE integration_id = $1 AND dataset_key = 'work-items'`,
		one.integration, `{`+artifactExcludeRuntime+`}`)
	return one
}

func (v *cascadeVenue) artifactExcludeTexts(one cascadeSeeded) artifactExcludeTexts {
	v.t.Helper()
	var texts artifactExcludeTexts
	if err := v.pool.QueryRow(context.Background(), `SELECT config.sync_options::text, integration.config::text, dataset.options::text
FROM sync_configurations AS config
JOIN integrations AS integration ON integration.id = config.integration_id
JOIN integration_datasets AS dataset ON dataset.integration_id = integration.id AND dataset.dataset_key = 'work-items'
WHERE config.id = $1`, one.config).Scan(&texts.options, &texts.config, &texts.workItems); err != nil {
		v.t.Fatal(err)
	}
	return texts
}

// verbWrite is the write of `dho admin sync-configs artifact-excludes set`: artifactexclude.Parse with the stored
// provider, then artifactexclude.WriteTx, in one transaction. nil prefixes clear.
func (v *cascadeVenue) verbWrite(one cascadeSeeded, provider string, prefixes []string) {
	v.t.Helper()
	ctx := context.Background()
	var value pyjson.Value
	if prefixes != nil {
		items := make([]pyjson.Value, len(prefixes))
		for index, prefix := range prefixes {
			items[index] = prefix
		}
		value = items
	}
	change, err := artifactexclude.Parse(provider, value)
	if err != nil {
		v.t.Fatal(err)
	}
	tx, err := v.pool.Begin(ctx)
	if err != nil {
		v.t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := artifactexclude.WriteTx(ctx, tx, v.org, one.config, change, time.Now().UTC()); err != nil {
		v.t.Fatal(err)
	}
	if err := tx.Commit(ctx); err != nil {
		v.t.Fatal(err)
	}
}

// shownList is the ci_artifact_exclude_prefixes a rendered configuration shows in sync_options ("<none>" when absent).
func shownList(t *testing.T, body string) string {
	t.Helper()
	var shown struct {
		SyncOptions map[string]json.RawMessage `json:"sync_options"`
	}
	if err := json.Unmarshal([]byte(body), &shown); err != nil {
		t.Fatalf("decode %s: %v", body, err)
	}
	raw, present := shown.SyncOptions["ci_artifact_exclude_prefixes"]
	if !present {
		return "<none>"
	}
	var list []string
	if err := json.Unmarshal(raw, &list); err != nil {
		t.Fatalf("decode the list %s: %v", raw, err)
	}
	return strings.Join(list, ",")
}

// get is the rendered configuration of GET /sync-configs/{id}.
func (v *cascadeVenue) get(config uuid.UUID) string {
	v.t.Helper()
	status, body := v.call("GET", "/api/v1/admin/sync-configs/"+config.String(), "")
	if status != 200 {
		v.t.Fatalf("GET %s: %d %s", config, status, body)
	}
	return body
}

// listed is every configuration of the list endpoint, rendered, by id.
func (v *cascadeVenue) listed() map[string]string {
	v.t.Helper()
	status, body := v.call("GET", "/api/v1/admin/sync-configs?include_migrated=true", "")
	if status != 200 {
		v.t.Fatalf("list: %d %s", status, body)
	}
	var items []json.RawMessage
	if err := json.Unmarshal([]byte(body), &items); err != nil {
		v.t.Fatal(err)
	}
	out := map[string]string{}
	for _, item := range items {
		var head struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal(item, &head); err != nil {
			v.t.Fatal(err)
		}
		out[head.ID] = string(item)
	}
	return out
}

func (v *cascadeVenue) storedOptionsHasKey(config uuid.UUID) bool {
	v.t.Helper()
	var has bool
	if err := v.pool.QueryRow(context.Background(), `SELECT (sync_options::jsonb -> 'ci_artifact_exclude_prefixes') IS NOT NULL
FROM sync_configurations WHERE id = $1`, config).Scan(&has); err != nil {
		v.t.Fatal(err)
	}
	return has
}

// The PATCH and the dho verb write the same bytes: the list goes into the integration's config only (it applies to
// every sync config of this integration), the configuration's sync_options and the GitHub work-item runtime options
// (configuration, integration, work-items dataset) stay byte-equal, and the PATCH answer shows the list. A clear
// removes it and leaves every other key as it was.
func TestArtifactExcludePatchAndVerbWriteByteEqualConfig(t *testing.T) {
	v := startCascadeVenue(t, true)
	for _, name := range []string{"GITHUB_FETCH_COMMENTS", "GITHUB_FETCH_MILESTONES", "GITHUB_COMMENTS_LIMIT"} {
		t.Setenv(name, "")
	}
	byPatch, byVerb := v.seedArtifactExclude("github"), v.seedArtifactExclude("github")
	before := v.artifactExcludeTexts(byPatch)
	if verbBefore := v.artifactExcludeTexts(byVerb); verbBefore != before {
		t.Fatalf("the two seeded configurations differ: %+v vs %+v", before, verbBefore)
	}
	status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+byPatch.config.String(),
		`{"sync_options":{"ci_artifact_exclude_prefixes":["ci-images-","ci-part-"]}}`)
	if status != 200 {
		t.Fatalf("PATCH: %d %s", status, body)
	}
	if got := shownList(t, body); got != "ci-images-,ci-part-" {
		t.Fatalf("the PATCH answer shows %s, want the list", got)
	}
	v.verbWrite(byVerb, "github", []string{"ci-images-", "ci-part-"})
	patched, verbed := v.artifactExcludeTexts(byPatch), v.artifactExcludeTexts(byVerb)
	if patched != verbed {
		t.Fatalf("the PATCH and the verb wrote different documents:\nPATCH %+v\nverb  %+v", patched, verbed)
	}
	wantConfig := `{` + artifactExcludeRuntime + `, "ci_artifact_exclude_prefixes": ["ci-images-", "ci-part-"]}`
	if patched.options != before.options || patched.config != wantConfig || patched.workItems != before.workItems {
		t.Fatalf("written %+v, want sync_options unchanged %s, config %s, work-items options unchanged %s",
			patched, before.options, wantConfig, before.workItems)
	}

	status, body = v.call("PATCH", "/api/v1/admin/sync-configs/"+byPatch.config.String(), `{"sync_options":{"ci_artifact_exclude_prefixes":null}}`)
	if status != 200 || shownList(t, body) != "<none>" {
		t.Fatalf("PATCH clear: %d %s", status, body)
	}
	v.verbWrite(byVerb, "github", nil)
	patched, verbed = v.artifactExcludeTexts(byPatch), v.artifactExcludeTexts(byVerb)
	if patched != verbed || patched != before {
		t.Fatalf("after the clear: PATCH %+v verb %+v, want both equal to the seeded documents %+v", patched, verbed, before)
	}
}

// The verb does not snapshot the work-item runtime options from its environment: with GITHUB_COMMENTS_LIMIT set to
// another value, a write of the list changes nothing but the integration's list.
func TestArtifactExcludeVerbLeavesWorkItemRuntimeKeys(t *testing.T) {
	v := startCascadeVenue(t, true)
	t.Setenv("GITHUB_COMMENTS_LIMIT", "7")
	t.Setenv("GITHUB_FETCH_COMMENTS", "false")
	one := v.seedArtifactExclude("github")
	before := v.artifactExcludeTexts(one)
	v.verbWrite(one, "github", []string{"ci-images-"})
	after := v.artifactExcludeTexts(one)
	if after.options != before.options || after.workItems != before.workItems ||
		after.config != `{`+artifactExcludeRuntime+`, "ci_artifact_exclude_prefixes": ["ci-images-"]}` {
		t.Errorf("written %+v from %+v: want the runtime options as stored and only the integration's list added", after, before)
	}
}

// One list per integration: a write on child A (by the PATCH, then by the verb) shows on GET of child A, child B and
// the parent and on the list endpoint; [x, y] then [z] shows [z] everywhere (last writer wins, no merge); a clear
// (null, [] and the verb's --clear) removes it everywhere. No configuration's own sync_options keeps the key, and a
// configuration of another integration without a list renders its stored options as they are.
func TestArtifactExcludeSiblingsShowOneListLastWriterWins(t *testing.T) {
	v := startCascadeVenue(t, true)
	t.Setenv("HIDE_MIGRATED_CHILD_CONFIGS", "")
	for _, name := range []string{"GITHUB_FETCH_COMMENTS", "GITHUB_FETCH_MILESTONES", "GITHUB_COMMENTS_LIMIT"} {
		t.Setenv(name, "")
	}
	parent := v.seedArtifactExclude("github")
	childA, childB := v.child(parent, "github", `["tests"]`), v.child(parent, "github", `["tests"]`)
	control := v.seedArtifactExclude("github")
	var controlStored string
	if err := v.pool.QueryRow(context.Background(), `SELECT sync_options::text FROM sync_configurations WHERE id = $1`, control.config).Scan(&controlStored); err != nil {
		t.Fatal(err)
	}
	check := func(label, want string) {
		t.Helper()
		list := v.listed()
		for name, config := range map[string]uuid.UUID{"child A": childA.config, "child B": childB.config, "parent": parent.config} {
			if got := shownList(t, v.get(config)); got != want {
				t.Errorf("%s: GET %s shows %s, want %s", label, name, got, want)
			}
			if got := shownList(t, list[config.String()]); got != want {
				t.Errorf("%s: the list shows %s for %s, want %s", label, got, name, want)
			}
			if v.storedOptionsHasKey(config) {
				t.Errorf("%s: %s keeps the key in its own sync_options", label, name)
			}
		}
		var shownControl struct {
			SyncOptions json.RawMessage `json:"sync_options"`
		}
		if err := json.Unmarshal([]byte(v.get(control.config)), &shownControl); err != nil {
			t.Fatal(err)
		}
		var stored, shown any
		if err := json.Unmarshal([]byte(controlStored), &stored); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(shownControl.SyncOptions, &shown); err != nil {
			t.Fatal(err)
		}
		storedJSON, _ := json.Marshal(stored)
		shownJSON, _ := json.Marshal(shown)
		if string(storedJSON) != string(shownJSON) {
			t.Errorf("%s: another integration's config shows %s, want its stored options %s", label, shownJSON, storedJSON)
		}
	}
	patch := func(value string) {
		t.Helper()
		status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+childA.config.String(), `{"sync_options":{"ci_artifact_exclude_prefixes":`+value+`}}`)
		if status != 200 {
			t.Fatalf("PATCH child A %s: %d %s", value, status, body)
		}
	}
	patch(`["ci-x-","ci-y-"]`)
	check("PATCH [x, y] on child A", "ci-x-,ci-y-")
	patch(`["ci-z-"]`)
	check("PATCH [z] on child A", "ci-z-")
	patch(`null`)
	check("PATCH null on child A", "<none>")
	patch(`["ci-x-"]`)
	patch(`[]`)
	check("PATCH [] on child A", "<none>")

	v.verbWrite(childA, "github", []string{"ci-x-", "ci-y-"})
	check("verb [x, y] on child A", "ci-x-,ci-y-")
	v.verbWrite(childA, "github", []string{"ci-z-"})
	check("verb [z] on child A", "ci-z-")
	v.verbWrite(childA, "github", nil)
	check("verb --clear on child A", "<none>")
}

// The integrations PATCH replaces the integration's config whole: it keeps the list when the new config does not carry
// the key, a carried list replaces it (null removes it), and a refused value is a 422 that names no value and writes
// nothing. Every sync configuration of the integration shows the result.
func TestIntegrationPatchKeepsTheArtifactExcludeList(t *testing.T) {
	v := startCascadeVenue(t, true)
	one := v.seedArtifactExclude("github")
	v.verbWrite(one, "github", []string{"ci-x-"})
	path := "/api/v1/admin/integrations/" + one.integration.String()
	status, body := v.call("PATCH", path, `{"config":{"fetch_comments": false}}`)
	if status != 200 {
		t.Fatalf("PATCH without the key: %d %s", status, body)
	}
	if got := shownList(t, v.get(one.config)); got != "ci-x-" {
		t.Errorf("after a config without the key the configuration shows %s, want the kept list", got)
	}
	status, body = v.call("PATCH", path, `{"config":{"fetch_comments": false, "ci_artifact_exclude_prefixes": ["ci-z-"]}}`)
	if status != 200 || shownList(t, v.get(one.config)) != "ci-z-" {
		t.Errorf("PATCH with [z]: %d %s, shown %s, want [z]", status, body, shownList(t, v.get(one.config)))
	}
	var before string
	if err := v.pool.QueryRow(context.Background(), `SELECT config::text FROM integrations WHERE id = $1`, one.integration).Scan(&before); err != nil {
		t.Fatal(err)
	}
	status, body = v.call("PATCH", path, `{"config":{"ci_artifact_exclude_prefixes": ["zz9 x"]}}`)
	var after string
	if err := v.pool.QueryRow(context.Background(), `SELECT config::text FROM integrations WHERE id = $1`, one.integration).Scan(&after); err != nil {
		t.Fatal(err)
	}
	if status != 422 || strings.Contains(body, "zz9") || after != before {
		t.Errorf("an invalid list: %d %s, config %s -> %s, want a 422 without the value and nothing written", status, body, before, after)
	}
	status, body = v.call("PATCH", path, `{"config":{"fetch_comments": false, "ci_artifact_exclude_prefixes": null}}`)
	if status != 200 || shownList(t, v.get(one.config)) != "<none>" {
		t.Errorf("PATCH with null: %d %s, want the list gone", status, body)
	}
}

// A configuration of a provider the field does nothing for refuses it with a 422 that names the field, never the value,
// and nothing is written; a value out of bounds is refused the same way on GitHub.
func TestArtifactExcludePatchRefusesWhatTheFieldCannotServe(t *testing.T) {
	v := startCascadeVenue(t, true)
	for _, cell := range []struct {
		provider, body string
	}{
		{"gitlab", `{"sync_options":{"ci_artifact_exclude_prefixes":["ci-zz9-"]}}`},
		{"jira", `{"sync_options":{"ci_artifact_exclude_prefixes":["ci-zz9-"]}}`},
		{"linear", `{"sync_options":{"ci_artifact_exclude_prefixes":["ci-zz9-"]}}`},
		{"github", `{"sync_options":{"ci_artifact_exclude_prefixes":["ci zz9"]}}`},
		{"github", `{"sync_options":{"ci_artifact_exclude_prefixes":[""]}}`},
	} {
		label := cell.provider + " " + cell.body
		one := v.seed(cell.provider, `["tests"]`, nil)
		var before string
		if err := v.pool.QueryRow(context.Background(), `SELECT sync_options::text || coalesce((SELECT config::text FROM integrations WHERE id = $2), '')
FROM sync_configurations WHERE id = $1`, one.config, one.integration).Scan(&before); err != nil {
			t.Fatal(err)
		}
		status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+one.config.String(), cell.body)
		if status != 422 || !strings.Contains(body, `"field"`) || !strings.Contains(body, "ci_artifact_exclude_prefixes") || strings.Contains(body, "zz9") {
			t.Errorf("%s: %d %s, want a 422 naming the field and not the value", label, status, body)
		}
		var after string
		if err := v.pool.QueryRow(context.Background(), `SELECT sync_options::text || coalesce((SELECT config::text FROM integrations WHERE id = $2), '')
FROM sync_configurations WHERE id = $1`, one.config, one.integration).Scan(&after); err != nil {
			t.Fatal(err)
		}
		if after != before {
			t.Errorf("%s: the refused save wrote %s -> %s", label, before, after)
		}
	}
}

// A create and a batch create of a GitHub configuration store a valid list in the new integration only (never in the
// configuration's sync_options) and show it in the answer and on GET; a gitlab create is refused.
func TestArtifactExcludeCreateWritesTheListToTheIntegration(t *testing.T) {
	v := startCascadeVenue(t, true)
	for index, cell := range []struct {
		path, body, key string
	}{
		{"/api/v1/admin/sync-configs", `{"name":"ax create","provider":"github","sync_targets":["tests"],"sync_options":{"all_repos":true,"ci_artifact_exclude_prefixes":["ci-images-"]}}`, ""},
		{"/api/v1/admin/sync-configs/batch", `{"name":"ax batch","provider":"github","sync_targets":["tests"],"sync_options":{"owner":"acme","ci_artifact_exclude_prefixes":["ci-images-"]},"repos":["acme/api"]}`, "parent"},
	} {
		status, body := v.call("POST", cell.path, cell.body)
		if status != 201 {
			t.Fatalf("cell %d: %d %s", index, status, body)
		}
		rendered := body
		if cell.key != "" {
			var wrapped map[string]json.RawMessage
			if err := json.Unmarshal([]byte(body), &wrapped); err != nil {
				t.Fatal(err)
			}
			rendered = string(wrapped[cell.key])
		}
		var created struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(rendered), &created); err != nil {
			t.Fatal(err)
		}
		if got := shownList(t, rendered); got != "ci-images-" {
			t.Errorf("cell %d: the answer shows %s, want the list", index, got)
		}
		var options, config string
		if err := v.pool.QueryRow(context.Background(), `SELECT config.sync_options::text, integration.config::text
FROM sync_configurations AS config JOIN integrations AS integration ON integration.id = config.integration_id
WHERE config.id = $1::uuid`, created.ID).Scan(&options, &config); err != nil {
			t.Fatal(err)
		}
		if strings.Contains(options, "ci_artifact_exclude_prefixes") || !strings.Contains(config, `"ci_artifact_exclude_prefixes": ["ci-images-"]`) {
			t.Errorf("cell %d: sync_options %s, integration config %s: want the list in the integration only", index, options, config)
		}
		if got := shownList(t, v.get(uuid.MustParse(created.ID))); got != "ci-images-" {
			t.Errorf("cell %d: GET shows %s, want the list", index, got)
		}
	}
	status, body := v.call("POST", "/api/v1/admin/sync-configs",
		`{"name":"ax gitlab","provider":"gitlab","sync_targets":["tests"],"sync_options":{"all_repos":true,"ci_artifact_exclude_prefixes":["ci-zz9-"]}}`)
	if status != 422 || strings.Contains(body, "zz9") {
		t.Errorf("gitlab create: %d %s, want a 422 without the value", status, body)
	}
}

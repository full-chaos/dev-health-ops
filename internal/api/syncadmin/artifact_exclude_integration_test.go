//go:build integration

package syncadmin

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

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

// The PATCH and the dho verb write the same bytes: the list goes into the configuration's sync_options and its
// integration's config, and the GitHub work-item runtime options (configuration, integration, work-items dataset) stay
// byte-equal. A clear removes the key from both documents and leaves every other key as it was.
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
	v.verbWrite(byVerb, "github", []string{"ci-images-", "ci-part-"})
	patched, verbed := v.artifactExcludeTexts(byPatch), v.artifactExcludeTexts(byVerb)
	if patched != verbed {
		t.Fatalf("the PATCH and the verb wrote different documents:\nPATCH %+v\nverb  %+v", patched, verbed)
	}
	wantOptions := `{"schedule_cron": "0 * * * *", ` + artifactExcludeRuntime + `, "ci_artifact_exclude_prefixes": ["ci-images-", "ci-part-"]}`
	wantConfig := `{` + artifactExcludeRuntime + `, "ci_artifact_exclude_prefixes": ["ci-images-", "ci-part-"]}`
	if patched.options != wantOptions || patched.config != wantConfig || patched.workItems != before.workItems {
		t.Fatalf("written %+v, want options %s, config %s and the work-items options unchanged %s", patched, wantOptions, wantConfig, before.workItems)
	}
	var shown struct {
		SyncOptions struct {
			Prefixes []string `json:"ci_artifact_exclude_prefixes"`
		} `json:"sync_options"`
	}
	if err := json.Unmarshal([]byte(body), &shown); err != nil || strings.Join(shown.SyncOptions.Prefixes, ",") != "ci-images-,ci-part-" {
		t.Fatalf("the PATCH answer shows %v (%v), want the stored list", shown.SyncOptions.Prefixes, err)
	}

	status, body = v.call("PATCH", "/api/v1/admin/sync-configs/"+byPatch.config.String(), `{"sync_options":{"ci_artifact_exclude_prefixes":null}}`)
	if status != 200 {
		t.Fatalf("PATCH clear: %d %s", status, body)
	}
	v.verbWrite(byVerb, "github", nil)
	patched, verbed = v.artifactExcludeTexts(byPatch), v.artifactExcludeTexts(byVerb)
	if patched != verbed || patched != before {
		t.Fatalf("after the clear: PATCH %+v verb %+v, want both equal to the seeded documents %+v", patched, verbed, before)
	}
}

// The verb does not snapshot the work-item runtime options from its environment: with GITHUB_COMMENTS_LIMIT set to
// another value, a write of the list changes nothing but the list.
func TestArtifactExcludeVerbLeavesWorkItemRuntimeKeys(t *testing.T) {
	v := startCascadeVenue(t, true)
	t.Setenv("GITHUB_COMMENTS_LIMIT", "7")
	t.Setenv("GITHUB_FETCH_COMMENTS", "false")
	one := v.seedArtifactExclude("github")
	before := v.artifactExcludeTexts(one)
	v.verbWrite(one, "github", []string{"ci-images-"})
	after := v.artifactExcludeTexts(one)
	for _, text := range []string{after.options, after.config} {
		if !strings.Contains(text, artifactExcludeRuntime) || !strings.HasSuffix(text, `, "ci_artifact_exclude_prefixes": ["ci-images-"]}`) {
			t.Errorf("written %s: want the runtime options as stored and only the list added", text)
		}
	}
	if after.workItems != before.workItems {
		t.Errorf("the work-items options moved: %s -> %s", before.workItems, after.workItems)
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

// A create and a batch create of a GitHub configuration store a valid list in the configuration and in its new
// integration, as given; a gitlab create is refused.
func TestArtifactExcludeCreateWritesTheListToBothDocuments(t *testing.T) {
	v := startCascadeVenue(t, true)
	for index, cell := range []struct {
		path, body string
	}{
		{"/api/v1/admin/sync-configs", `{"name":"ax create","provider":"github","sync_targets":["tests"],"sync_options":{"all_repos":true,"ci_artifact_exclude_prefixes":["ci-images-"]}}`},
		{"/api/v1/admin/sync-configs/batch", `{"name":"ax batch","provider":"github","sync_targets":["tests"],"sync_options":{"owner":"acme","ci_artifact_exclude_prefixes":["ci-images-"]},"repos":["acme/api"]}`},
	} {
		status, body := v.call("POST", cell.path, cell.body)
		if status != 201 {
			t.Fatalf("cell %d: %d %s", index, status, body)
		}
		var created struct {
			ID string `json:"id"`
		}
		if err := json.Unmarshal([]byte(body), &created); err != nil {
			t.Fatal(err)
		}
		var options, config string
		if err := v.pool.QueryRow(context.Background(), `SELECT config.sync_options::text, integration.config::text
FROM sync_configurations AS config JOIN integrations AS integration ON integration.id = config.integration_id
WHERE config.id = $1::uuid`, created.ID).Scan(&options, &config); err != nil {
			t.Fatal(err)
		}
		for _, text := range []string{options, config} {
			if !strings.Contains(text, `"ci_artifact_exclude_prefixes": ["ci-images-"]`) {
				t.Errorf("cell %d: stored %s, want the list", index, text)
			}
		}
	}
	status, body := v.call("POST", "/api/v1/admin/sync-configs",
		`{"name":"ax gitlab","provider":"gitlab","sync_targets":["tests"],"sync_options":{"all_repos":true,"ci_artifact_exclude_prefixes":["ci-zz9-"]}}`)
	if status != 422 || strings.Contains(body, "zz9") {
		t.Errorf("gitlab create: %d %s, want a 422 without the value", status, body)
	}
}

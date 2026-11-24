//go:build integration

package syncadmin

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
)

// seedCrossOrgIntegration is the data anomaly no API path makes: a GitHub configuration of the venue's org whose
// integration_id points at an integration of another org (seeded directly), that integration carrying a list.
func (v *cascadeVenue) seedCrossOrgIntegration() (one cascadeSeeded, other uuid.UUID) {
	v.t.Helper()
	one = v.seedArtifactExclude("github")
	otherOrg := uuid.NewString()
	pgseed.Org(context.Background(), v.t, v.pool, otherOrg, "enterprise")
	other = uuid.New()
	v.exec(`INSERT INTO integrations (id, org_id, provider, name, config, is_active, created_at, updated_at)
VALUES ($1, $2, 'github', 'other org integration', $3::json, true, now(), now())`, other, otherOrg,
		`{"ci_artifact_exclude_prefixes": ["ci-other-"], `+artifactExcludeRuntime+`}`)
	v.exec(`UPDATE sync_configurations SET integration_id = $2 WHERE id = $1`, one.config, other)
	return one, other
}

func (v *cascadeVenue) integrationConfigText(id uuid.UUID) string {
	v.t.Helper()
	var text string
	if err := v.pool.QueryRow(context.Background(), `SELECT config::text FROM integrations WHERE id = $1`, id).Scan(&text); err != nil {
		v.t.Fatal(err)
	}
	return text
}

// A PATCH of one org's GitHub configuration takes the runtime-options row lock on its integration only when that
// integration is the org's own: with the configuration pointed at another org's integration, the PATCH must not wait
// on that org's integration row. The test holds the other org's row (FOR NO KEY UPDATE, as a writer of that org
// does); the PATCH must answer while the row is held. RED at main: the lock is taken by id alone (update_config.go,
// writeGitHubRuntimeOptions), so the PATCH waits on the other org's row until the test releases it.
func TestGitHubRuntimeOptionsLockSkipsAnotherOrgsIntegration(t *testing.T) {
	v := startCascadeVenue(t, true)
	one, other := v.seedCrossOrgIntegration()
	ctx := context.Background()
	holder, err := v.pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = holder.Rollback(ctx) }()
	if _, err := holder.Exec(ctx, `SELECT 1 FROM integrations WHERE id = $1 FOR NO KEY UPDATE`, other); err != nil {
		t.Fatal(err)
	}
	type answer struct {
		status int
		body   string
		err    error
	}
	requestCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
	defer cancel()
	done := make(chan answer, 1)
	go func() {
		status, body, err := v.callCtx(requestCtx, "PATCH", "/api/v1/admin/sync-configs/"+one.config.String(), `{"sync_options":{"fetch_comments":false}}`)
		done <- answer{status, body, err}
	}()
	var got answer
	answered := false
	select {
	case got = <-done:
		answered = true
	case <-time.After(10 * time.Second):
	}
	if !answered {
		var waiting int
		if err := v.pool.QueryRow(ctx, `SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock' AND query ILIKE '%FROM integrations WHERE id%'`).Scan(&waiting); err != nil {
			t.Fatal(err)
		}
		t.Errorf("the PATCH of an org's configuration did not answer in 10 s while another org's integration row was held "+
			"(%d backends wait on a lock reading integrations by id): it locks the other org's integration", waiting)
	}
	if err := holder.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if !answered {
		got = <-done
	}
	if got.err != nil {
		t.Fatalf("PATCH: %v", got.err)
	}
	if got.status != 422 || !strings.Contains(got.body, "the sync configuration has no integration") || strings.Contains(got.body, `"field"`) {
		t.Errorf("the PATCH without the list answered %d %s, want the 422 \"the sync configuration has no integration\" with no field "+
			"(the update's integration org check, CHAOS-14582)", got.status, got.body)
	}
	if text := v.integrationConfigText(other); !strings.Contains(text, `"ci-other-"`) {
		t.Errorf("the other org's integration config changed: %s", text)
	}
}

// A configuration pointed at another org's integration never shows or writes that integration's list: GET shows no
// ci_artifact_exclude_prefixes; a PATCH with the key is the named 422 refusal of a configuration with no integration
// (artifactexclude.WriteTx reads the integration by org and id and finds none of this org), never a 500, and writes
// nothing; the other org's integration config is byte-equal before and after.
func TestArtifactExcludeNeverCrossesIntoAnotherOrgsIntegration(t *testing.T) {
	v := startCascadeVenue(t, true)
	one, other := v.seedCrossOrgIntegration()
	before := v.integrationConfigText(other)
	if shown := shownList(t, v.get(one.config)); shown != "<none>" {
		t.Errorf("GET shows %q of another org's integration, want no ci_artifact_exclude_prefixes", shown)
	}
	listed, ok := v.listed()[one.config.String()]
	if !ok {
		t.Fatalf("the list endpoint does not hold the configuration")
	}
	if shown := shownList(t, listed); shown != "<none>" {
		t.Errorf("the list endpoint shows %q of another org's integration, want no ci_artifact_exclude_prefixes", shown)
	}
	var optionsBefore string
	if err := v.pool.QueryRow(context.Background(), `SELECT sync_options::text FROM sync_configurations WHERE id = $1`, one.config).Scan(&optionsBefore); err != nil {
		t.Fatal(err)
	}
	status, body := v.call("PATCH", "/api/v1/admin/sync-configs/"+one.config.String(), `{"sync_options":{"ci_artifact_exclude_prefixes":["ci-mine-"]}}`)
	if status != 422 || !strings.Contains(body, `"field"`) || !strings.Contains(body, "ci_artifact_exclude_prefixes") ||
		!strings.Contains(body, "the sync configuration has no integration") || strings.Contains(body, "ci-other-") || strings.Contains(body, "ci-mine-") {
		t.Errorf("PATCH with the key: %d %s, want the 422 refusal naming the field and \"the sync configuration has no integration\", no list", status, body)
	}
	if after := v.integrationConfigText(other); after != before {
		t.Errorf("the other org's integration config changed: %s -> %s", before, after)
	}
	var optionsAfter string
	if err := v.pool.QueryRow(context.Background(), `SELECT sync_options::text FROM sync_configurations WHERE id = $1`, one.config).Scan(&optionsAfter); err != nil {
		t.Fatal(err)
	}
	if optionsAfter != optionsBefore {
		t.Errorf("the refused PATCH wrote the configuration's sync_options: %s -> %s", optionsBefore, optionsAfter)
	}
}

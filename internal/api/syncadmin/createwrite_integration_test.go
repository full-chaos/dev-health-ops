//go:build integration

package syncadmin

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/providersync"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestCreateWriteVenueOracleSequenceUnderTheAPIRole runs the create
// path's write sequence on the real migrated schema as the api role (so
// its grants are exercised too) and pins the rows each provider shape
// leaves. It is not the parity proof: the raw-row differential against
// the Python create route is the route's venue oracle.
func TestCreateWriteVenueOracleSequenceUnderTheAPIRole(t *testing.T) {
	ctx := context.Background()
	_, file, _, _ := runtime.Caller(0)
	root := filepath.Clean(filepath.Join(filepath.Dir(file), "..", "..", ".."))
	org := uuid.New().String()
	goodCredential, badCredential, otherOrgCredential := uuid.New(), uuid.New(), uuid.New()
	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root: root,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, _ *venueoracle.Venue) map[string]map[string]any {
			at := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
			for _, statement := range []struct {
				sql  string
				args []any
			}{
				{`INSERT INTO organizations (id, slug, name, settings, tier, is_active, created_at, updated_at) VALUES ($1, 'o', 'o', '{}', 'enterprise', true, $2, $2)`, []any{org, at}},
				{`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, config, created_at, updated_at) VALUES ($1, $2, 'pagerduty', 'good', true, '{"account_id": " acct ", "subdomain": "sub"}', $3, $3)`, []any{goodCredential, org, at}},
				{`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, config, created_at, updated_at) VALUES ($1, $2, 'pagerduty', 'bad', true, '{"subdomain": "sub"}', $3, $3)`, []any{badCredential, org, at}},
				{`INSERT INTO integration_credentials (id, org_id, provider, name, is_active, config, created_at, updated_at) VALUES ($1, $2, 'pagerduty', 'other', true, '{"account_id": "a", "subdomain": "s"}', $3, $3)`, []any{otherOrgCredential, uuid.New().String(), at}},
			} {
				if _, err := admin.Exec(ctx, statement.sql, statement.args...); err != nil {
					t.Fatalf("seed: %v", err)
				}
			}
			return nil
		},
	})
	pool, err := pgxpool.New(ctx, venue.GoAPIDatabaseURI(t))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	now := time.Date(2026, 9, 24, 12, 0, 0, 0, time.UTC)
	env := map[string]string{"GITHUB_COMMENTS_LIMIT": "7"}
	lookup := func(name string) (string, bool) { value, ok := env[name]; return value, ok }

	create := func(name, provider string, targets []string, options string, credential *string) (*plannerCreated, error) {
		t.Helper()
		decoded, err := pyjson.DecodeString(options)
		if err != nil {
			t.Fatal(err)
		}
		parent := decoded.(*pyjson.Object)
		tx, err := pool.Begin(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback(ctx) }()
		created, err := createPlannerManagedConfig(ctx, tx, plannerCreate{
			orgID: org, name: name, provider: provider, credentialID: credential, syncTargets: targets, parentOptions: parent,
			buildSourceRows: func(_, configID uuid.UUID) []newSourceRow {
				lower := provider
				if lower == "github" || lower == "gitlab" || lower == "pagerduty" {
					return nil
				}
				return nonGitSourceRows(provider, parent, name, configID.String())
			},
		}, now, lookup)
		if err != nil {
			return nil, err
		}
		if err := tx.Commit(ctx); err != nil {
			t.Fatal(err)
		}
		return created, nil
	}
	query := func(sql string, args ...any) string {
		t.Helper()
		rows, err := pool.Query(ctx, sql, args...)
		if err != nil {
			t.Fatal(err)
		}
		values, err := pgx.CollectRows(rows, func(row pgx.CollectableRow) (string, error) {
			var text string
			err := row.Scan(&text)
			return text, err
		})
		if err != nil {
			t.Fatal(err)
		}
		return fmt.Sprint(values)
	}
	expect := func(what, got, want string) {
		t.Helper()
		if got != want {
			t.Errorf("%s:\n got  %s\n want %s", what, got, want)
		}
	}

	// GitHub, token-wide, scheduled: the runtime snapshot joins the options
	// (the env limit), the datasets follow the targets with blame, the job is
	// ACTIVE on the config's schedule.
	github, err := create("gh", "github", []string{"git", "work-items"}, `{"all_repos": true, "schedule_cron": "0 */6 * * *", "timezone": "Europe/Paris"}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	expect("github options", query(`SELECT sync_options::text FROM sync_configurations WHERE id = $1`, github.config.ID),
		`[{"all_repos": true, "schedule_cron": "0 */6 * * *", "timezone": "Europe/Paris", "fetch_comments": true, "fetch_milestones": true, "comments_limit": 7}]`)
	keys, _ := providersync.PlannerDatasetKeys("github", []string{"git", "work-items"})
	expect("github datasets", query(`SELECT dataset_key FROM integration_datasets WHERE integration_id = $1 ORDER BY dataset_key COLLATE "C"`, github.integrationID),
		fmt.Sprint(sortedCopy(keys)))
	expect("github work-items options", query(`SELECT options::text FROM integration_datasets WHERE integration_id = $1 AND dataset_key = 'work-items'`, github.integrationID),
		`[{"legacy_targets": ["git", "work-items"], "fetch_comments": true, "fetch_milestones": true, "comments_limit": 7}]`)
	expect("github job", query(`SELECT concat_ws('|', name, provider, schedule_cron, timezone, status, job_config::text) FROM scheduled_jobs WHERE sync_config_id = $1`, github.config.ID),
		fmt.Sprintf(`[sync-config-%s|github|0 */6 * * *|Europe/Paris|0|{"provider": "github", "sync_config_id": "%s"}]`, github.config.ID, github.config.ID))
	expect("github sources", query(`SELECT external_id FROM integration_sources WHERE integration_id = $1`, github.integrationID), `[]`)

	// Jira with an explicit project: one project source, marked explicit;
	// manual-only, so the job is PAUSED on the default schedule.
	jira, err := create("jira cfg", "jira", []string{"work-items"}, `{"project_key": "ENG"}`, nil)
	if err != nil {
		t.Fatal(err)
	}
	expect("jira source", query(`SELECT concat_ws('|', provider, source_type, external_id, name, full_name, metadata::text, is_enabled) FROM integration_sources WHERE integration_id = $1`, jira.integrationID),
		fmt.Sprintf(`[jira|project|ENG|jira cfg|ENG|{"planner_managed_sync_config_id": "%s", "explicit_project_scope": true}|t]`, jira.config.ID))
	expect("jira job", query(`SELECT concat_ws('|', schedule_cron, timezone, status) FROM scheduled_jobs WHERE sync_config_id = $1`, jira.config.ID), `[0 * * * *|UTC|1]`)

	// PagerDuty with a credential whose identity is usable: the account
	// source is added and the operational datasets stay enabled.
	good := goodCredential.String()
	pd, err := create("pd", "pagerduty", []string{"operational"}, `{}`, &good)
	if err != nil {
		t.Fatal(err)
	}
	expect("pagerduty account source", query(`SELECT concat_ws('|', provider, source_type, external_id, is_enabled) FROM integration_sources WHERE integration_id = $1`, pd.integrationID),
		`[pagerduty|account|acct|t]`)
	expect("pagerduty config still active", query(`SELECT is_active::text FROM sync_configurations WHERE id = $1`, pd.config.ID), `[true]`)

	// A credential without a usable identity disables the config with the
	// reason; another org's credential is not the org's, so nothing changes.
	bad := badCredential.String()
	pdBad, err := create("pd bad", "pagerduty", []string{"operational"}, `{}`, &bad)
	if err != nil {
		t.Fatal(err)
	}
	expect("pagerduty stamped", query(`SELECT concat_ws('|', is_active, last_sync_success, last_sync_error, last_sync_stats::text) FROM sync_configurations WHERE id = $1`, pdBad.config.ID),
		`[f|f|PagerDuty credential account identity is invalid|{"phase": "pagerduty_repair", "error": "PagerDuty credential account identity is invalid"}]`)
	// The repair disabled that config, so its explicitly scheduled job is
	// PAUSED, as Python's _upsert_scheduled_job reads the disabled config.
	pdBadScheduled, err := create("pd bad scheduled", "pagerduty", []string{"operational"}, `{"schedule_cron": "0 * * * *"}`, &bad)
	if err != nil {
		t.Fatal(err)
	}
	expect("pagerduty disabled config's job", query(`SELECT status::text FROM scheduled_jobs WHERE sync_config_id = $1`, pdBadScheduled.config.ID), `[1]`)
	other := otherOrgCredential.String()
	pdOther, err := create("pd other", "pagerduty", []string{"operational"}, `{}`, &other)
	if err != nil {
		t.Fatal(err)
	}
	expect("pagerduty other org's credential", query(`SELECT count(*)::text FROM integration_sources WHERE integration_id = $1`, pdOther.integrationID), `[0]`)

	// The refusals write nothing: a PagerDuty selection other than
	// operational, a malformed GitHub work-item option, a credential id that
	// is not a UUID.
	before := query(`SELECT count(*)::text FROM integrations WHERE org_id = $1`, org)
	if _, err := create("pd wrong", "pagerduty", []string{"incidents"}, `{}`, nil); !errors.Is(err, providersync.ErrPagerDutyTargetNotOperational) {
		t.Errorf("pagerduty non-operational: %v", err)
	}
	if _, err := create("gh bad", "github", []string{"work-items"}, `{"all_repos": true, "fetch_comments": "no"}`, nil); err == nil {
		t.Error("github malformed option: no error")
	}
	notUUID := "not-a-uuid"
	if _, err := create("bad cred", "jira", []string{"work-items"}, `{}`, &notUUID); !errors.Is(err, errCredentialIDNotUUID) {
		t.Errorf("credential id: %v", err)
	}
	expect("refusals wrote nothing", query(`SELECT count(*)::text FROM integrations WHERE org_id = $1`, org), before)
	if os.Getenv("DEV_HEALTH_LIVE_PYTHON_ORACLES") == "1" {
		venueoracle.WriteGoOnlyProof(t, "the create write sequence under the api role; the Python differential is the create route's venue")
	}
}

func sortedCopy(values []string) []string {
	out := append([]string(nil), values...)
	for i := range out {
		for j := i + 1; j < len(out); j++ {
			if out[j] < out[i] {
				out[i], out[j] = out[j], out[i]
			}
		}
	}
	return out
}

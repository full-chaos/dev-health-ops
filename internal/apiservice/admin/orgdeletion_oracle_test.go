//go:build integration

package admin_test

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/venueoracle"
)

// TestOrgDeletionMatchesThePythonAPI is CHAOS-6306 approval condition 4: the
// Superuser-guarded DELETE /orgs/{org_id} route, both dry_run=true and a
// real delete, proven byte-for-byte against the real Python route and with
// post-delete row counts compared on both planes -- not just the response
// body. ClickHouse is deliberately left unconfigured on both planes here
// (h.clickHouseDSN=="", CLICKHOUSE_URI="" on the Python plane, the venue's
// own default): the live system.columns vs. migration-regex table-set
// divergence is a separate, already-flagged, pending disposition
// (orgdeletion_clickhouse_oracle_test.go) -- this test's job is the
// Postgres purge, the Superuser guard, dry_run's no-op guarantee, and the
// scheduled_jobs disable-then-delete step, not ClickHouse.
//
// It seeds a representative cross-section of orgDeletionTargets, not all
// 46: a direct string org_id column with an indirect child of its own
// (scheduled_jobs -> job_runs), a direct uuid org_id column with an
// indirect uuid-typed child of its own (invoices -> invoice_line_items),
// and a plain direct uuid org_id column (org_ip_allowlist, settings). A
// second, untouched control org proves the scope predicate never leaks
// past the target org.
func TestOrgDeletionMatchesThePythonAPI(t *testing.T) {
	ctx := context.Background()
	root := repoRoot(t)
	const jwtKey = "venue-oracle-test-secret-key-for-org-deletion-flow-32-bytes!!"

	targetOrgID := uuid.New()
	controlOrgID := uuid.New()
	superID := uuid.New()
	memberID := uuid.New()
	scheduledJobID := uuid.New()
	controlJobID := uuid.New()
	invoiceID := uuid.New()
	controlInvoiceID := uuid.New()

	venue := venueoracle.Start(t, ctx, venueoracle.Options{
		Root:   root,
		JWTKey: jwtKey,
		Seed: func(t *testing.T, ctx context.Context, admin *pgxpool.Pool, v *venueoracle.Venue) map[string]map[string]any {
			t.Helper()
			exec := func(sql string, args ...any) {
				t.Helper()
				if _, err := admin.Exec(ctx, sql, args...); err != nil {
					t.Fatalf("seed: %v\n%s", err, sql)
				}
			}
			for _, org := range []uuid.UUID{targetOrgID, controlOrgID} {
				exec(`INSERT INTO organizations (id, slug, name, tier, managed_by, is_active, created_at, updated_at)
VALUES ($1, $2, $2, 'community', 'stripe', true, now(), now())`, org, "venue-orgdel-"+org.String()[:8])
			}
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-orgdel-super@example.com', true, true, true, 0, now(), now())`, superID)
			exec(`INSERT INTO users (id, email, is_active, is_verified, is_superuser, token_version, created_at, updated_at)
VALUES ($1, 'venue-orgdel-member@example.com', true, true, false, 0, now(), now())`, memberID)
			exec(`INSERT INTO memberships (id, org_id, user_id, role, joined_at, created_at, updated_at)
VALUES ($1, $2, $3, 'owner', now(), now(), now())`, uuid.New(), targetOrgID, memberID)

			// settings: direct, string org_id.
			exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES ($1, $2, 'general', 'theme', 'dark', false, now(), now())`, uuid.New(), targetOrgID.String())
			exec(`INSERT INTO settings (id, org_id, category, key, value, is_encrypted, created_at, updated_at)
VALUES ($1, $2, 'general', 'theme', 'light', false, now(), now())`, uuid.New(), controlOrgID.String())

			// scheduled_jobs (direct, string org_id) + job_runs (indirect,
			// via scheduled_jobs.org_id, string).
			exec(`INSERT INTO scheduled_jobs (id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, status, is_running, run_count, failure_count, created_at, updated_at)
VALUES ($1, $2, 'venue job', 'sync', '', '0 * * * *', 'UTC', '{}', 1, true, 0, 0, now(), now())`, scheduledJobID, targetOrgID.String())
			exec(`INSERT INTO job_runs (id, job_id, status, started_at, created_at) VALUES ($1, $2, 0, now(), now())`, uuid.New(), scheduledJobID)
			exec(`INSERT INTO scheduled_jobs (id, org_id, name, job_type, provider, schedule_cron, timezone, job_config, status, is_running, run_count, failure_count, created_at, updated_at)
VALUES ($1, $2, 'control job', 'sync', '', '0 * * * *', 'UTC', '{}', 1, false, 0, 0, now(), now())`, controlJobID, controlOrgID.String())
			exec(`INSERT INTO job_runs (id, job_id, status, started_at, created_at) VALUES ($1, $2, 0, now(), now())`, uuid.New(), controlJobID)

			// invoices (direct, uuid org_id) + invoice_line_items (indirect,
			// via invoices.org_id, uuid).
			exec(`INSERT INTO invoices (id, org_id, stripe_invoice_id, stripe_customer_id, status, amount_due, created_at)
VALUES ($1, $2, $3, 'cus_venue', 'paid', 1000, now())`, invoiceID, targetOrgID, "in_venue_target_"+invoiceID.String()[:8])
			exec(`INSERT INTO invoice_line_items (id, invoice_id, description, amount, quantity) VALUES ($1, $2, 'venue line', 1000, 1)`, uuid.New(), invoiceID)
			exec(`INSERT INTO invoices (id, org_id, stripe_invoice_id, stripe_customer_id, status, amount_due, created_at)
VALUES ($1, $2, $3, 'cus_venue', 'paid', 500, now())`, controlInvoiceID, controlOrgID, "in_venue_control_"+controlInvoiceID.String()[:8])
			exec(`INSERT INTO invoice_line_items (id, invoice_id, description, amount, quantity) VALUES ($1, $2, 'control line', 500, 1)`, uuid.New(), controlInvoiceID)

			// org_ip_allowlist: direct, uuid org_id, no indirect children.
			exec(`INSERT INTO org_ip_allowlist (id, org_id, ip_range, is_active, created_at, updated_at)
VALUES ($1, $2, '10.0.0.0/8', true, now(), now())`, uuid.New(), targetOrgID)
			exec(`INSERT INTO org_ip_allowlist (id, org_id, ip_range, is_active, created_at, updated_at)
VALUES ($1, $2, '10.0.0.0/8', true, now(), now())`, uuid.New(), controlOrgID)

			return map[string]map[string]any{
				"super":  {"user_id": superID.String(), "email": "venue-orgdel-super@example.com", "is_superuser": true},
				"member": {"user_id": memberID.String(), "email": "venue-orgdel-member@example.com", "org_id": targetOrgID.String(), "role": "owner"},
			}
		},
	})

	bearer := func(name string) string { return "Bearer " + venue.Tokens[name] }
	authHeaders := func(name string) map[string]string { return map[string]string{"Authorization": bearer(name)} }

	goBase, _ := startGoServer(t, ctx, venue, jwtKey)
	normalize := venueoracle.DiffOptions{
		Normalize: func(request venueoracle.Request, body string) string {
			return redactField(t, body, "timestamp")
		},
	}

	// First batch: every dry_run=true (and rejection) case. Diffed and row
	// counts checked BEFORE the real delete below ever runs -- Diff sends a
	// batch to each plane in order, so proving dry_run is a true no-op
	// requires the real delete to not yet be in the batch, not just to be
	// the response body's own claim.
	dryRunRequests := []venueoracle.Request{
		{Name: "delete org non-superuser refused", Method: "DELETE",
			Path: "/api/v1/admin/orgs/" + targetOrgID.String(), Headers: authHeaders("member")},
		{Name: "delete org malformed id", Method: "DELETE",
			Path: "/api/v1/admin/orgs/not-a-uuid?dry_run=true", Headers: authHeaders("super")},
		{Name: "delete org dry run nonexistent org", Method: "DELETE",
			Path: "/api/v1/admin/orgs/" + uuid.New().String() + "?dry_run=true", Headers: authHeaders("super")},
		{Name: "delete org dry run", Method: "DELETE",
			Path: "/api/v1/admin/orgs/" + targetOrgID.String() + "?dry_run=true", Headers: authHeaders("super")},
	}
	dryRunPython := venue.ServePython(t, dryRunRequests)
	t.Log(venueoracle.Diff(t, goBase, dryRunRequests, dryRunPython, normalize))

	// Finding-shaped proof: dry_run=true must be a true no-op. Row counts
	// for every seeded target-org table stay exactly as seeded on BOTH
	// planes after the dry-run request above ran against both.
	for _, query := range []string{
		"SELECT count(*) FROM settings WHERE org_id = '" + targetOrgID.String() + "'",
		"SELECT count(*) FROM scheduled_jobs WHERE org_id = '" + targetOrgID.String() + "'",
		"SELECT count(*) FROM invoices WHERE org_id = '" + targetOrgID.String() + "'",
		"SELECT count(*) FROM org_ip_allowlist WHERE org_id = '" + targetOrgID.String() + "'",
	} {
		source := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), query)
		got := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), query)
		if source != got {
			t.Errorf("post-dry-run row count differs (query %q):\n python: %s\n go:     %s", query, source, got)
		}
		if source != "1" {
			t.Errorf("post-dry-run row count for target org (query %q) = %s, want 1 (dry_run must not delete)", query, source)
		}
	}

	// Second batch: the real delete, sent only now that dry_run's no-op
	// proof above is already checked.
	realRequests := []venueoracle.Request{
		{Name: "delete org real", Method: "DELETE",
			Path: "/api/v1/admin/orgs/" + targetOrgID.String(), Headers: authHeaders("super")},
	}
	realPython := venue.ServePython(t, realRequests)
	t.Log(venueoracle.Diff(t, goBase, realRequests, realPython, normalize))

	// Finding-shaped proof: the REAL delete above purges every seeded
	// target-org row -- direct and indirect predicates alike -- on BOTH
	// planes, while the control org's own rows survive untouched on both
	// planes too.
	type rowCheck struct {
		label, query string
		wantTarget   string
	}
	checks := []rowCheck{
		{"settings", "SELECT count(*) FROM settings WHERE org_id = '" + targetOrgID.String() + "'", "0"},
		{"settings control", "SELECT count(*) FROM settings WHERE org_id = '" + controlOrgID.String() + "'", "1"},
		{"scheduled_jobs", "SELECT count(*) FROM scheduled_jobs WHERE org_id = '" + targetOrgID.String() + "'", "0"},
		{"scheduled_jobs control", "SELECT count(*) FROM scheduled_jobs WHERE org_id = '" + controlOrgID.String() + "'", "1"},
		{"job_runs (indirect)", "SELECT count(*) FROM job_runs WHERE job_id = '" + scheduledJobID.String() + "'", "0"},
		{"job_runs control (indirect)", "SELECT count(*) FROM job_runs WHERE job_id = '" + controlJobID.String() + "'", "1"},
		{"invoices", "SELECT count(*) FROM invoices WHERE org_id = '" + targetOrgID.String() + "'", "0"},
		{"invoices control", "SELECT count(*) FROM invoices WHERE org_id = '" + controlOrgID.String() + "'", "1"},
		{"invoice_line_items (indirect)", "SELECT count(*) FROM invoice_line_items WHERE invoice_id = '" + invoiceID.String() + "'", "0"},
		{"invoice_line_items control (indirect)", "SELECT count(*) FROM invoice_line_items WHERE invoice_id = '" + controlInvoiceID.String() + "'", "1"},
		{"org_ip_allowlist", "SELECT count(*) FROM org_ip_allowlist WHERE org_id = '" + targetOrgID.String() + "'", "0"},
		{"org_ip_allowlist control", "SELECT count(*) FROM org_ip_allowlist WHERE org_id = '" + controlOrgID.String() + "'", "1"},
		{"organizations", "SELECT count(*) FROM organizations WHERE id = '" + targetOrgID.String() + "'", "0"},
		{"organizations control", "SELECT count(*) FROM organizations WHERE id = '" + controlOrgID.String() + "'", "1"},
	}
	for _, c := range checks {
		source := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.SourceDB), c.query)
		got := venueoracle.TableRows(t, ctx, venue.AdminURI(t, venue.GoDB), c.query)
		if source != got {
			t.Errorf("post-delete row count differs (%s):\n python: %s\n go:     %s", c.label, source, got)
		}
		if got != c.wantTarget {
			t.Errorf("post-delete row count (%s) = %s, want %s", c.label, got, c.wantTarget)
		}
	}
}

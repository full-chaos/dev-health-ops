//go:build integration

package syncdispatchruntime

import (
	"context"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgschema"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/pgseed"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	credTestIntegrationCred = "00000000-0000-4000-8000-0000000000c1"
	credTestRunCred         = "00000000-0000-4000-8000-0000000000c2"
	credTestUnitA           = "00000000-0000-4000-8000-0000000000c3"
	credTestUnitB           = "00000000-0000-4000-8000-0000000000c4"
)

type credState struct {
	success *bool
	at      *time.Time
	errText *string
}

func readCred(t *testing.T, ctx context.Context, pool *pgxpool.Pool, id string) credState {
	t.Helper()
	var s credState
	if err := pool.QueryRow(ctx, `SELECT last_test_success, last_test_at, last_test_error FROM integration_credentials WHERE id = $1::uuid`, id).
		Scan(&s.success, &s.at, &s.errText); err != nil {
		t.Fatal(err)
	}
	return s
}

func boolp(b bool) *bool { return &b }

// TestFinalizeStampsCredentialConnected pins CHAOS-11881: a run with at least
// one successful unit marks its credential connected, a run with none never
// writes, and the stamp is refreshed at most every 15 minutes.
func TestFinalizeStampsCredentialConnected(t *testing.T) {
	// ago is how old the credential's last_test_at is before the run; zero value means "never tested".
	type pre struct {
		success  *bool
		ago      time.Duration
		errText  string
		inactive bool
	}
	cases := []struct {
		name       string
		pre        pre
		units      []string
		runCred    bool // run carries its own credential_id (otherwise the integration's is used)
		noCred     bool // neither run nor integration names a credential
		wantStamp  bool
		wantSucces *bool // expected state of the stamped credential when unchanged
	}{
		{name: "success run on never-tested credential", units: []string{"success"}, wantStamp: true},
		{name: "partial_failed run with one ok unit", units: []string{"success", "failed"}, wantStamp: true},
		{name: "failed run keeps a connected credential", pre: pre{success: boolp(true), ago: time.Hour}, units: []string{"failed"}},
		{name: "failed run leaves a never-tested credential", units: []string{"failed"}},
		{name: "fresh connected credential is not rewritten", pre: pre{success: boolp(true), ago: 5 * time.Minute}, units: []string{"success"}},
		{name: "stale connected credential is refreshed", pre: pre{success: boolp(true), ago: 20 * time.Minute}, units: []string{"success"}, wantStamp: true},
		{name: "success heals an earlier failed test", pre: pre{success: boolp(false), ago: 2 * time.Hour, errText: "boom"}, units: []string{"success"}, wantStamp: true},
		{name: "failed run keeps a failed test and its error", pre: pre{success: boolp(false), ago: time.Hour, errText: "bad token"}, units: []string{"failed"}},
		{name: "late run does not overwrite a newer failed test", pre: pre{success: boolp(false), ago: -time.Hour, errText: "bad token"}, units: []string{"success"}},
		{name: "late run does not overwrite a newer connected test", pre: pre{success: boolp(true), ago: -time.Hour}, units: []string{"success"}},
		{name: "run credential wins over integration credential", units: []string{"success"}, runCred: true, wantStamp: true},
		{name: "run with no credential is a no-op", units: []string{"success"}, noCred: true},
		{name: "inactive credential is not touched", pre: pre{inactive: true}, units: []string{"success"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
			defer cancel()
			instance := pgschema.Start(ctx, t)
			defer instance.Close(context.Background())
			pool, err := pgxpool.New(ctx, instance.URI)
			if err != nil {
				t.Fatal(err)
			}
			defer pool.Close()
			seedFinalizeRoute(t, ctx, pool)
			pgseed.Credential(ctx, t, pool, credTestIntegrationCred, finalizeTestOrg, "github")
			pgseed.Credential(ctx, t, pool, credTestRunCred, finalizeTestOrg, "github")
			stamped := credTestIntegrationCred
			if tc.runCred {
				stamped = credTestRunCred
			}
			if !tc.noCred {
				if _, err := pool.Exec(ctx, `UPDATE integrations SET credential_id = $1::uuid WHERE id = $2::uuid`, credTestIntegrationCred, finalizeTestIntegration); err != nil {
					t.Fatal(err)
				}
			}
			insertFinalizeRun(t, ctx, pool, len(tc.units), "", "")
			if tc.runCred {
				if _, err := pool.Exec(ctx, `UPDATE sync_runs SET credential_id = $1::uuid WHERE id = $2::uuid`, credTestRunCred, finalizeTestRun); err != nil {
					t.Fatal(err)
				}
			}
			unitIDs := []string{credTestUnitA, credTestUnitB}
			for i, status := range tc.units {
				pgseed.InsertSyncRunUnit(ctx, t, pool, pgseed.SyncRunUnit{
					ID: unitIDs[i], RunID: finalizeTestRun, OrgID: finalizeTestOrg, IntegrationID: finalizeTestIntegration,
					SourceID: finalizeTestSource, Status: status,
					ResultJSON: map[string]string{"success": `{}`, "failed": `{"error_category":"provider_error"}`}[status],
				})
			}
			var preAt *time.Time
			if tc.pre.success != nil {
				at := time.Now().UTC().Add(-tc.pre.ago)
				preAt = &at
				var errText *string
				if tc.pre.errText != "" {
					errText = &tc.pre.errText
				}
				if _, err := pool.Exec(ctx, `UPDATE integration_credentials SET last_test_success=$2, last_test_at=$3, last_test_error=$4 WHERE id=$1::uuid`, stamped, tc.pre.success, at, errText); err != nil {
					t.Fatal(err)
				}
			}
			if tc.pre.inactive {
				if _, err := pool.Exec(ctx, `UPDATE integration_credentials SET is_active=false WHERE id=$1::uuid`, stamped); err != nil {
					t.Fatal(err)
				}
			}
			before := readCred(t, ctx, pool, stamped)
			other := credTestRunCred
			if tc.runCred {
				other = credTestIntegrationCred
			}
			otherBefore := readCred(t, ctx, pool, other)

			service, err := NewNativeFinalizeSyncRunService(pool, nil)
			if err != nil {
				t.Fatal(err)
			}
			if err := service.Finalize(ctx, newFinalizeArgs()); err != nil {
				t.Fatalf("Finalize: %v", err)
			}

			got := readCred(t, ctx, pool, stamped)
			if tc.wantStamp {
				var completedAt time.Time
				if err := pool.QueryRow(ctx, `SELECT completed_at FROM sync_runs WHERE id=$1::uuid`, finalizeTestRun).Scan(&completedAt); err != nil {
					t.Fatal(err)
				}
				if got.success == nil || !*got.success {
					t.Fatalf("last_test_success = %v, want true", got.success)
				}
				if got.at == nil || !got.at.Equal(completedAt) {
					t.Fatalf("last_test_at = %v, want run completed_at %v", got.at, completedAt)
				}
				if got.errText != nil {
					t.Fatalf("last_test_error = %q, want NULL", *got.errText)
				}
			} else {
				if (got.success == nil) != (before.success == nil) || (got.success != nil && *got.success != *before.success) {
					t.Fatalf("last_test_success changed: %v -> %v", before.success, got.success)
				}
				if (got.at == nil) != (before.at == nil) || (got.at != nil && !got.at.Equal(*before.at)) {
					t.Fatalf("last_test_at changed: %v -> %v", before.at, got.at)
				}
				if (got.errText == nil) != (before.errText == nil) || (got.errText != nil && *got.errText != *before.errText) {
					t.Fatalf("last_test_error changed: %v -> %v", before.errText, got.errText)
				}
			}
			otherAfter := readCred(t, ctx, pool, other)
			if (otherAfter.success == nil) != (otherBefore.success == nil) || otherAfter.at != nil && otherBefore.at != nil && !otherAfter.at.Equal(*otherBefore.at) {
				t.Fatalf("the other credential was touched: %v -> %v", otherBefore, otherAfter)
			}
			_ = preAt
		})
	}
}

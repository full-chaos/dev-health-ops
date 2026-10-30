//go:build integration

package restorefence

import (
	"context"
	"encoding/json"
	"errors"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
)

// revocationSentinel is the stored ciphertext of the fixture, built at run time; it must never reach a report, an error
// or the stored operator actions.
var revocationSentinel = strings.Join([]string{"fence", "revocation", "sentinel", "e41a"}, "-")

func revocationID(name string) string {
	return uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-requeue-"+name)).String()
}

func (d *fenceDB) revocation(t *testing.T, ctx context.Context, name, credential, purpose string, attempts int) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO public.provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, last_error, created_at, updated_at)
VALUES ($1, 'org-x', 'pagerduty', $2, $3, $4, 'v1', 'pending', $5, CASE WHEN $5 > 0 THEN 'remote_revoke_failed' END, '2026-10-20T00:00:00Z', now())`,
		revocationID(name), credential, purpose, revocationSentinel+"-"+name, attempts)
}

func (d *fenceDB) pagerDutyCredential(t *testing.T, ctx context.Context, name string) {
	t.Helper()
	d.must(t, ctx, `INSERT INTO public.integration_credentials (id, org_id, provider, name, is_active, created_at, updated_at)
VALUES ($1, 'org-x', 'pagerduty', $2, true, now(), now())`, uuid.NewSHA1(uuid.NameSpaceURL, []byte("fence-requeue-cred-"+name)), name)
}

type revocationState struct {
	present             bool
	ciphertext          string
	keyVersion          string
	status              string
	attempts            int
	lastErrorNull       bool
	createdAt           time.Time
	credential, purpose string
}

func (d *fenceDB) revocationState(t *testing.T, ctx context.Context, name string) revocationState {
	t.Helper()
	var s revocationState
	var lastError *string
	err := d.admin.QueryRow(ctx, `SELECT token_encrypted, token_key_version, status, attempts, last_error, created_at, credential_name, purpose
FROM public.provider_oauth_revocations WHERE id = $1`, revocationID(name)).Scan(&s.ciphertext, &s.keyVersion, &s.status, &s.attempts, &lastError, &s.createdAt, &s.credential, &s.purpose)
	if err == nil {
		s.present, s.lastErrorNull = true, lastError == nil
	}
	return s
}

func assertNoRevocationCiphertext(t *testing.T, ctx context.Context, d *fenceDB, report Report, err error) {
	t.Helper()
	raw, mErr := json.Marshal(report)
	if mErr != nil {
		t.Fatal(mErr)
	}
	if strings.Contains(string(raw), revocationSentinel) {
		t.Error("the apply report carries a revocation ciphertext")
	}
	if err != nil && strings.Contains(err.Error(), revocationSentinel) {
		t.Error("the apply error carries a revocation ciphertext")
	}
	if n := d.count(t, ctx, `SELECT count(*) FROM auth.restore_fence_state WHERE operator_actions::text LIKE '%`+revocationSentinel+`%'`); n != 0 {
		t.Error("the stored operator actions carry a revocation ciphertext")
	}
}

// requeuePair builds T in both databases (one revocation both hold, a credential both hold) and the end state after
// T: a disconnect of the T-era credential (kept), a rotation of a credential connected after T (late), and a refused
// setup revoke (setup). The restored database lacks all three revocation rows.
func requeuePair(t *testing.T, ctx context.Context) (restored, end *fenceDB) {
	t.Helper()
	restored, end = newFenceDB(t, ctx), newFenceDB(t, ctx)
	for _, d := range []*fenceDB{restored, end} {
		d.stateT(t, ctx)
		d.pagerDutyCredential(t, ctx, "kept")
		d.revocation(t, ctx, "both", "kept", "replacement", 2)
	}
	end.must(t, ctx, `UPDATE public.provider_oauth_revocations SET attempts = 5 WHERE id = $1`, revocationID("both"))
	end.revocation(t, ctx, "kept", "kept", "disconnect", 3)
	end.pagerDutyCredential(t, ctx, "late")
	end.revocation(t, ctx, "late", "late", "replacement", 0)
	end.revocation(t, ctx, "setup", "pending-setup", "setup", 1)
	return restored, end
}

// Q1/Q3: with the source set, each manifest revocation the restored database lacks is put back with the source
// ciphertext as fresh pending work (attempts 0, no error, created_at kept), and named: deliverable when its credential
// row exists (or it is a setup revoke), blocked when the credential was connected after T (disconnect cannot reach it).
func TestTheFenceRequeuesRevocationsQueuedAfterTFromTheSource(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := requeuePair(t, ctx)
	manifest := exportEnd(t, ctx, end)
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true, Source: PGRevocationSource{Pool: end.admin}})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	for _, name := range []string{"kept", "late", "setup"} {
		got, want := restored.revocationState(t, ctx, name), end.revocationState(t, ctx, name)
		if !got.present {
			t.Errorf("revocation %s was not put back", name)
			continue
		}
		if got.ciphertext != want.ciphertext {
			t.Errorf("revocation %s: the ciphertext differs from the source", name)
		}
		if got.keyVersion != want.keyVersion || !got.createdAt.Equal(want.createdAt) || got.credential != want.credential || got.purpose != want.purpose {
			t.Errorf("revocation %s: key version, created_at, credential or purpose differ from the source", name)
		}
		if got.status != "pending" || got.attempts != 0 || !got.lastErrorNull {
			t.Errorf("revocation %s: status %q attempts %d last_error null %v, want pending, 0, null", name, got.status, got.attempts, got.lastErrorNull)
		}
	}
	if s := restored.revocationState(t, ctx, "both"); s.attempts != 2 {
		t.Errorf("a revocation the restored database holds was written: attempts %d, want 2", s.attempts)
	}
	sameSet(t, "provider_revocation_requeued", reviewIDs(report, "provider_revocation_requeued", "public.provider_oauth_revocations"), sortedIDs(revocationID("kept"), revocationID("setup")))
	sameSet(t, "provider_revocation_requeued_blocked", reviewIDs(report, "provider_revocation_requeued_blocked", "public.provider_oauth_revocations"), sortedIDs(revocationID("late")))
	if ids := reviewIDs(report, "requeue_provider_revocation", "public.provider_oauth_revocations"); len(ids) != 0 {
		t.Errorf("requeued revocations are still named as missing: %v", ids)
	}
	var requeued int64 = -1
	for _, s := range report.Sections {
		if s.Name == "public.provider_oauth_revocations.requeued" {
			requeued = s.Changed
		}
	}
	if requeued != 3 {
		t.Errorf("the requeue section changed %d rows, want 3", requeued)
	}
	// Delivery (a): the disconnect retry of the credential (pagerduty_disconnect.go:204) selects the requeued row.
	// Delivery (b): the disconnect finds no credential row (pagerduty_disconnect.go:105) and returns before any retry.
	var picked string
	if err := restored.admin.QueryRow(ctx, `SELECT id::text FROM provider_oauth_revocations WHERE org_id = $1 AND provider = 'pagerduty' AND credential_name = $2 AND status = 'pending' AND id = $3`,
		"org-x", "kept", revocationID("kept")).Scan(&picked); err != nil {
		t.Errorf("the disconnect retry of credential kept does not select the requeued row: %v", err)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM integration_credentials WHERE org_id = 'org-x' AND provider = 'pagerduty' AND name = 'late'`); n != 0 {
		t.Errorf("the blocked fixture is wrong: credential late has %d restored row(s)", n)
	}
	assertNoRevocationCiphertext(t, ctx, restored, report, nil)

	again, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true, Source: PGRevocationSource{Pool: end.admin}})
	if err != nil || !again.AlreadyApplied {
		t.Fatalf("second apply = %+v, %v; want already applied", again, err)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.provider_oauth_revocations`); n != 4 {
		t.Errorf("after a second apply the restored database holds %d revocations, want 4", n)
	}
}

func TestADryRunWithTheSourceNamesTheRequeueAndWritesNothing(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := requeuePair(t, ctx)
	report, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{Source: PGRevocationSource{Pool: end.admin}})
	if err != nil {
		t.Fatalf("dry run: %v", err)
	}
	if ids := reviewIDs(report, "provider_revocation_requeued", "public.provider_oauth_revocations"); len(ids) != 2 {
		t.Errorf("the dry run names %v, want the two deliverable revocations", ids)
	}
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.provider_oauth_revocations`); n != 1 {
		t.Errorf("a dry run wrote revocations: %d rows, want 1", n)
	}
}

func TestWithoutTheSourceTheRevocationsAreOnlyNamed(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := requeuePair(t, ctx)
	report, err := Apply(ctx, restored.admin, restored.schema, exportEnd(t, ctx, end), Options{Apply: true})
	if err != nil {
		t.Fatalf("apply: %v", err)
	}
	sameSet(t, "requeue_provider_revocation", reviewIDs(report, "requeue_provider_revocation", "public.provider_oauth_revocations"),
		sortedIDs(revocationID("kept"), revocationID("late"), revocationID("setup")))
	if n := restored.count(t, ctx, `SELECT count(*) FROM public.provider_oauth_revocations`); n != 1 {
		t.Errorf("without a source the fence wrote revocations: %d rows, want 1", n)
	}
}

func TestASourceOnAnotherTimelineIsRefusedAndNothingIsWritten(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := requeuePair(t, ctx)
	manifest := exportEnd(t, ctx, end)
	manifest.SourceTimeline++
	if err := manifest.Seal(); err != nil {
		t.Fatal(err)
	}
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true, Source: PGRevocationSource{Pool: end.admin}})
	if !errors.Is(err, ErrApply) {
		t.Fatalf("a source on another timeline = %v, want ErrApply", err)
	}
	assertNothingWritten(t, ctx, restored)
	assertNoRevocationCiphertext(t, ctx, restored, report, err)
}

func TestAManifestRevocationMissingAtTheSourceIsRefused(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	defer cancel()
	restored, end := requeuePair(t, ctx)
	manifest := exportEnd(t, ctx, end)
	end.must(t, ctx, `DELETE FROM public.provider_oauth_revocations WHERE id = $1`, revocationID("late"))
	report, err := Apply(ctx, restored.admin, restored.schema, manifest, Options{Apply: true, Source: PGRevocationSource{Pool: end.admin}})
	if !errors.Is(err, ErrApply) || !strings.Contains(err.Error(), "missing at the source") {
		t.Fatalf("a manifest revocation missing at the source = %v, want ErrApply naming it", err)
	}
	assertNothingWritten(t, ctx, restored)
	assertNoRevocationCiphertext(t, ctx, restored, report, err)
}

func assertNothingWritten(t *testing.T, ctx context.Context, d *fenceDB) {
	t.Helper()
	if n := d.count(t, ctx, `SELECT count(*) FROM public.provider_oauth_revocations`); n != 1 {
		t.Errorf("a refused apply wrote revocations: %d rows, want 1", n)
	}
	if n := d.count(t, ctx, `SELECT count(*) FROM auth.restore_fence_state WHERE source = 'manifest'`); n != 0 {
		t.Errorf("a refused apply wrote %d fence row(s)", n)
	}
}

func sortedIDs(ids ...string) []string {
	out := append([]string(nil), ids...)
	sort.Strings(out)
	return out
}

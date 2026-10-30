package restorefence

import (
	"context"
	"errors"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

// requeueSentinel stands in for a DSN or a stored ciphertext; it must never reach an error or a report.
var requeueSentinel = strings.Join([]string{"requeue", "sentinel", "4b1d"}, "-")

type fakeRevocationSource struct {
	calls    int
	ids      []string
	timeline int64
	rows     string
	err      error
}

func (f *fakeRevocationSource) Revocations(_ context.Context, _ authschema.ValidatedIdentifier, ids []string) (SourceRead, error) {
	f.calls++
	f.ids = append([]string(nil), ids...)
	return SourceRead{Timeline: f.timeline, Revocations: f.rows, AuthLocks: "[]", LegacyLocks: "[]"}, f.err
}

func manifestWithRevocations(t *testing.T, ids ...string) Manifest {
	t.Helper()
	m := sampleManifest(t)
	for i := range m.Sections {
		if m.Sections[i].Name != "public.provider_oauth_revocations" {
			continue
		}
		for _, id := range ids {
			m.Sections[i].Rows = append(m.Sections[i].Rows, Row{"id": id, "org_id": "org-x", "provider": "pagerduty", "purpose": "disconnect"})
		}
	}
	if err := m.Seal(); err != nil {
		t.Fatal(err)
	}
	return m
}

func TestTheRevocationSourceReadsExactlyTheManifestIDsAndRefusesAnotherTimeline(t *testing.T) {
	a, b := "00000000-0000-4000-8000-0000000000b2", "00000000-0000-4000-8000-0000000000a1"
	m := manifestWithRevocations(t, a, b)
	src := &fakeRevocationSource{timeline: m.SourceTimeline + 1, rows: `[{"token_encrypted":"` + requeueSentinel + `"}]`}
	_, err := Apply(context.Background(), nil, testSchema(t), &m, Options{Source: src})
	if !errors.Is(err, ErrApply) || !strings.Contains(err.Error(), "timeline") {
		t.Fatalf("a source on another timeline = %v, want ErrApply naming the timeline", err)
	}
	if strings.Contains(err.Error(), requeueSentinel) {
		t.Errorf("the refusal quotes the source rows: %v", err)
	}
	want := []string{a, b}
	sort.Strings(want)
	if src.calls != 1 || strings.Join(src.ids, ",") != strings.Join(want, ",") {
		t.Errorf("the source was asked for %v (%d calls), want exactly the manifest revocation ids %v", src.ids, src.calls, want)
	}
}

func TestTheRevocationSourceIsReadEvenWithNoManifestRevocation(t *testing.T) {
	m := manifestWithRevocations(t)
	src := &fakeRevocationSource{timeline: m.SourceTimeline + 1, rows: "[]"}
	if _, err := Apply(context.Background(), nil, testSchema(t), &m, Options{Source: src}); !errors.Is(err, ErrApply) || src.calls != 1 {
		t.Fatalf("a source on another timeline with no revocation ids = %v (%d calls), want ErrApply after one read", err, src.calls)
	}
}

func TestTheRevocationSourceNeedsAManifest(t *testing.T) {
	src := &fakeRevocationSource{}
	if _, err := Apply(context.Background(), nil, testSchema(t), nil, Options{BlindMargin: 5, Source: src}); !errors.Is(err, ErrApply) {
		t.Fatalf("a revocation source with a blind margin = %v, want ErrApply", err)
	}
	if src.calls != 0 {
		t.Errorf("the source was read %d times for a blind margin", src.calls)
	}
}

func TestARevocationSourceErrorIsReportedWithoutItsText(t *testing.T) {
	m := manifestWithRevocations(t, "00000000-0000-4000-8000-0000000000c3")
	src := &fakeRevocationSource{err: errors.New("dial host=shadow password=" + requeueSentinel)}
	_, err := Apply(context.Background(), nil, testSchema(t), &m, Options{Source: src})
	if !errors.Is(err, ErrApply) {
		t.Fatalf("a failed source read = %v, want ErrApply", err)
	}
	if strings.Contains(err.Error(), requeueSentinel) || strings.Contains(err.Error(), "host=") {
		t.Errorf("the source error text reached the caller: %v", err)
	}
}

func TestTheSourceReadSelectsOnlyTheIDsAndNeverTheQueueState(t *testing.T) {
	q := revocationSourceSQL
	if !strings.Contains(q, "WHERE id = ANY($1::uuid[])") {
		t.Errorf("the source read is not limited to the manifest ids: %s", q)
	}
	for _, col := range []string{"'id'", "'org_id'", "'provider'", "'credential_name'", "'purpose'", "'token_encrypted'", "'token_key_version'", "'created_at'"} {
		if !strings.Contains(q, col) {
			t.Errorf("the source read lacks %s: %s", col, q)
		}
	}
	for _, col := range []string{"status", "attempts", "last_error", "updated_at"} {
		if strings.Contains(q, col) {
			t.Errorf("the source read carries the queue state %s: %s", col, q)
		}
	}
}

func TestTheRequeueInsertsOnlyMissingManifestRowsAsFreshPendingWork(t *testing.T) {
	q := requeueInsertSQL
	for _, want := range []string{
		"INSERT INTO public.provider_oauth_revocations (id, org_id, provider, credential_name, purpose, token_encrypted, token_key_version, status, attempts, last_error, created_at, updated_at)",
		"'pending', 0, NULL, s.created_at, clock_timestamp()",
		"JOIN m ON m.id = s.id",
		"ON CONFLICT (id) DO NOTHING",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("the requeue insert lacks %q:\n%s", want, q)
		}
	}
	for _, bad := range []string{"s.status", "s.attempts", "s.last_error", "s.updated_at"} {
		if strings.Contains(q, bad) {
			t.Errorf("the requeue copies the source queue state %s:\n%s", bad, q)
		}
	}
	if !strings.Contains(requeueMissingSQL, "NOT EXISTS (SELECT 1 FROM s WHERE s.id = l.id)") || !strings.Contains(requeueMissingSQL, "NOT EXISTS (SELECT 1 FROM public.provider_oauth_revocations t WHERE t.id = x.id)") {
		t.Errorf("the missing-at-source count does not count the manifest ids the restored database lacks and the source lacks:\n%s", requeueMissingSQL)
	}
}

func TestTheRequeueSplitsDeliverableFromBlockedByTheCredentialRow(t *testing.T) {
	q := requeueInsertSQL
	for _, want := range []string{
		"i.purpose = 'setup'",
		"FROM public.integration_credentials c WHERE c.org_id = i.org_id AND c.provider = i.provider AND c.name = i.credential_name",
	} {
		if !strings.Contains(q, want) {
			t.Errorf("the deliverable split lacks %q:\n%s", want, q)
		}
	}
}

func TestTheRequeueActionsNameTheirFollowUps(t *testing.T) {
	if ActionProviderRevocationRequeued != "provider_revocation_requeued" || ActionProviderRevocationRequeuedBlocked != "provider_revocation_requeued_blocked" {
		t.Fatalf("action names: %q %q", ActionProviderRevocationRequeued, ActionProviderRevocationRequeuedBlocked)
	}
	if followUp[ActionProviderRevocationRequeued] != "none" {
		t.Errorf("the deliverable requeue follow-up = %q, want none (the operator disconnects or reconnects)", followUp[ActionProviderRevocationRequeued])
	}
	if f := followUp[ActionProviderRevocationRequeuedBlocked]; !regexp.MustCompile(`^CHAOS-[0-9]+$`).MatchString(f) || f == "CHAOS-11835" {
		t.Errorf("the blocked requeue follow-up = %q, want the revocation worker ticket", f)
	}
	if f := followUpOf(OperatorAction{Kind: ActionRequeueProviderRevocation, Source: revocationSection}); f != requeueRunbookStep {
		t.Errorf("requeue_provider_revocation of the revocation table follow-up = %q, want the runbook step %q", f, requeueRunbookStep)
	}
	if !strings.Contains(requeueRunbookStep, "backup-and-restore.md") || !strings.Contains(requeueRunbookStep, "RESTORE_FENCE_SOURCE_URI") {
		t.Errorf("the runbook follow-up does not name the runbook and the variable: %q", requeueRunbookStep)
	}
	if f := followUpOf(OperatorAction{Kind: ActionRequeueProviderRevocation, Source: "public.provider_oauth_credentials"}); f != credentialRevokeStep {
		t.Errorf("requeue_provider_revocation of the deleted credentials follow-up = %q, want the manual revoke step %q (the fence delete writes no revocation row)", f, credentialRevokeStep)
	}
	for _, want := range []string{"backup-and-restore.md", "revoke-the-old-token-at-the-provider-by-hand", "then-reconnect-the-credential"} {
		if !strings.Contains(credentialRevokeStep, want) {
			t.Errorf("the deleted-credential follow-up %q does not say %q", credentialRevokeStep, want)
		}
	}
	if strings.Contains(credentialRevokeStep, "CHAOS-") || strings.Contains(credentialRevokeStep, " ") {
		t.Errorf("the deleted-credential follow-up must name no worker ticket and be one token: %q", credentialRevokeStep)
	}
	for _, k := range []ActionKind{ActionForcePasswordReset, ActionUserTombstoned, ActionProviderRevocationRequeuedBlocked} {
		if followUpOf(OperatorAction{Kind: k, Source: "x"}) != followUp[k] {
			t.Errorf("%s: the follow-up does not come from the kind", k)
		}
	}
}

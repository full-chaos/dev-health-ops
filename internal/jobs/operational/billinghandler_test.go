package operational

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

const billingOrgID = "00000000-0000-4000-8000-000000000010"

// hasCategory reports whether err carries the given jobruntime category. The
// category lives on an unexported marker type, so its Error() text is the
// supported way to read it -- the same idiom synccoverage and workgraph use.
func hasCategory(err error, category jobruntime.ErrorCategory) bool {
	return err != nil && strings.Contains(err.Error(), string(category))
}

type fakeFence struct {
	claim       ClaimResult
	claimErr    error
	completeErr error
	// releaseErr fails EVERY release attempt. releaseFailures instead fails
	// only the first N, so a transient blip that the inline retry recovers
	// from can be distinguished from one that never clears.
	releaseErr      error
	releaseFailures int
	claims          int
	completions     int
	releases        int
	completedNow    time.Time
	// released is the durable effect: whether the claim is actually clear.
	released bool
}

func (fence *fakeFence) Claim(_ context.Context, _ string, now time.Time) (ClaimResult, error) {
	fence.claims++
	if fence.claimErr != nil {
		return ClaimResult{}, fence.claimErr
	}
	if fence.claim.Claimed && fence.claim.ClaimedAt == nil {
		claimedAt := now
		fence.claim.ClaimedAt = &claimedAt
	}
	return fence.claim, nil
}

func (fence *fakeFence) MarkCompleted(_ context.Context, _ string, now time.Time) error {
	fence.completions++
	fence.completedNow = now
	return fence.completeErr
}

func (fence *fakeFence) ReleaseClaim(_ context.Context, _ string) error {
	fence.releases++
	if fence.releaseErr != nil {
		return fence.releaseErr
	}
	if fence.releases <= fence.releaseFailures {
		return errors.New("billing claim release is unavailable")
	}
	fence.released = true
	return nil
}

type fakeOwners struct {
	owner OwnerContact
	err   error
	calls int
}

func (owners *fakeOwners) LoadOrgOwner(context.Context, string) (OwnerContact, error) {
	owners.calls++
	return owners.owner, owners.err
}

type fakeSender struct {
	sent []EmailMessage
	err  error
	// recordAsSentDespiteError marks the message as sent even though err is
	// still returned -- the shape of "the provider accepted the message,
	// then the response timed out": the provider actually received it, the
	// caller just never learned that. Left false, err alone (the ordinary
	// case) never records a send.
	recordAsSentDespiteError bool
}

func (sender *fakeSender) Name() string { return "fake" }

func (sender *fakeSender) Send(_ context.Context, message EmailMessage) error {
	if sender.err != nil {
		if sender.recordAsSentDespiteError {
			sender.sent = append(sender.sent, message)
		}
		return sender.err
	}
	sender.sent = append(sender.sent, message)
	return nil
}

func billingExecution() *jobruntime.Execution[jobruntime.BillingNotificationArgs] {
	org := billingOrgID
	return &jobruntime.Execution[jobruntime.BillingNotificationArgs]{
		Args: jobruntime.BillingNotificationArgs{
			EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.BillingNotificationPayload]{
				Payload: jobcontract.BillingNotificationPayload{NotificationID: billingID},
			},
		},
		OrganizationID: &org,
		Envelope: jobcontract.Envelope{
			Domain: jobcontract.DomainLink{Type: "billing_notification", ID: billingID},
		},
	}
}

func billingRow(attributes string) BillingNotification {
	return BillingNotification{
		ID:               billingID,
		OrganizationID:   billingOrgID,
		NotificationType: EmailTypeInvoiceReceipt,
		IdempotencyKey:   "billing:key",
		Attributes:       []byte(attributes),
	}
}

func newTestBillingHandler(
	t *testing.T, store DeliveryStore, fence BillingFence, owners OwnerLookup, sender EmailSender,
) *BillingHandler {
	t.Helper()
	handler, err := NewBillingHandler(store, fence, owners, sender, "https://app.example.test")
	if err != nil {
		t.Fatal(err)
	}
	handler.now = func() time.Time { return time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC) }
	return handler
}

func TestBillingHandlerClaimsRendersSendsAndCompletes(t *testing.T) {
	store := &fakeStore{billing: billingRow(
		`{"amount_cents":4999,"currency":"usd","invoice_url":"https://invoice.example.test/in_1"}`)}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	owners := &fakeOwners{owner: OwnerContact{
		Email: "owner@example.test", FullName: "Dana Reed", OrgName: "Northwind Analytics"}}
	sender := &fakeSender{}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	if err := handler.Work(context.Background(), billingExecution()); err != nil {
		t.Fatal(err)
	}
	if fence.claims != 1 || fence.completions != 1 || fence.releases != 0 {
		t.Fatalf("fence calls: claims=%d completions=%d releases=%d",
			fence.claims, fence.completions, fence.releases)
	}
	if len(sender.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(sender.sent))
	}
	message := sender.sent[0]
	if message.To != "owner@example.test" || message.Subject != "Invoice receipt" {
		t.Fatalf("message = %+v", message)
	}
}

// TestBillingHandlerSendsNothingWhenTheClaimIsLost: losing the claim must
// never reach the sender, whatever the reason.
//
// NOTE (CHAOS-5353 r1): this test used to assert that BOTH a completed claim
// and an in-flight one returned SUCCESS. The in-flight half encoded the P1
// defect -- it is what let a retry whose own release had failed report a
// delivery that never happened. Only the completed case is a success now; the
// in-flight case is pinned as Retryable by
// TestRetryAfterASendFailureIsNeverReportedAsADuplicate below.
func TestBillingHandlerSendsNothingWhenTheClaimIsLost(t *testing.T) {
	completedAt := time.Date(2026, 9, 7, 11, 0, 0, 0, time.UTC)
	claimedAt := completedAt
	freshClaimedAt := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC).
		Add(-StaleClaimThreshold / 2)
	for _, test := range []struct {
		name        string
		claim       ClaimResult
		wantSuccess bool
	}{
		{
			name:        "already completed",
			claim:       ClaimResult{ClaimedAt: &claimedAt, CompletedAt: &completedAt},
			wantSuccess: true,
		},
		{
			name:        "in flight and uncompleted",
			claim:       ClaimResult{ClaimedAt: &freshClaimedAt},
			wantSuccess: false,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeStore{billing: billingRow(`{}`)}
			fence := &fakeFence{claim: test.claim}
			owners := &fakeOwners{}
			sender := &fakeSender{}
			handler := newTestBillingHandler(t, store, fence, owners, sender)

			err := handler.Work(context.Background(), billingExecution())
			if test.wantSuccess && err != nil {
				t.Fatalf("a genuinely completed notification must succeed, got %v", err)
			}
			if !test.wantSuccess {
				if err == nil {
					t.Fatal("an UNCOMPLETED claim was reported as a successful delivery")
				}
				if !hasCategory(err, jobruntime.CategoryRetryable) {
					t.Fatalf("classified as %v, want retryable", err)
				}
			}
			if len(sender.sent) != 0 {
				t.Fatal("an email was sent despite losing the claim")
			}
			if owners.calls != 0 {
				t.Fatal("the owner was looked up despite losing the claim")
			}
			if fence.completions != 0 || fence.releases != 0 {
				t.Fatalf("a lost claim wrote to the fence: completions=%d releases=%d",
					fence.completions, fence.releases)
			}
		})
	}
}

// TestBillingHandlerReportsAStaleClaimAsPermanent: a claim held past the
// threshold with no completion means we do NOT know whether the email went
// out, so it must surface as a distinct failure, not a silent success.
func TestBillingHandlerReportsAStaleClaimAsPermanent(t *testing.T) {
	claimedAt := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC).
		Add(-StaleClaimThreshold - time.Minute)
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{ClaimedAt: &claimedAt}}
	sender := &fakeSender{}
	handler := newTestBillingHandler(t, store, fence, &fakeOwners{}, sender)

	err := handler.Work(context.Background(), billingExecution())
	if err == nil {
		t.Fatal("a stale claim was reported as success")
	}
	if !hasCategory(err, jobruntime.CategoryPermanent) {
		t.Fatalf("stale claim classified as %v, want permanent", err)
	}
	if len(sender.sent) != 0 {
		t.Fatal("a stale claim still sent an email")
	}
}

// TestBillingHandlerReleasesTheClaimExactlyOnceOnAPermanentDrop covers the
// class of bug codex round 3 found in the Python original: an unguarded exit
// between the claim and the send left the claim held, so a retry reported a
// duplicate for an email that was never attempted.
func TestBillingHandlerReleasesTheClaimExactlyOnceOnAPermanentDrop(t *testing.T) {
	for _, test := range []struct {
		name         string
		notification BillingNotification
	}{
		{"unknown email type", func() BillingNotification {
			row := billingRow(`{}`)
			row.NotificationType = "not_a_billing_email"
			return row
		}()},
		{"malformed attributes", billingRow(`{"amount_cents":"not-a-number"}`)},
		{"invalid organization id", func() BillingNotification {
			row := billingRow(`{}`)
			row.OrganizationID = "org-abc"
			return row
		}()},
	} {
		t.Run(test.name, func(t *testing.T) {
			execution := billingExecution()
			// The tenant check compares the row against the envelope, so an
			// invalid-org case must disagree with the row, not the envelope.
			execution.OrganizationID = &test.notification.OrganizationID
			store := &fakeStore{billing: test.notification}
			fence := &fakeFence{claim: ClaimResult{Claimed: true}}
			sender := &fakeSender{}
			handler := newTestBillingHandler(t, store, fence, &fakeOwners{}, sender)

			err := handler.Work(context.Background(), execution)
			if err == nil {
				t.Fatal("a permanent drop was reported as success")
			}
			if !hasCategory(err, jobruntime.CategoryPermanent) {
				t.Fatalf("classified as %v, want permanent", err)
			}
			if fence.releases != 1 {
				t.Fatalf("claim released %d times, want exactly 1 -- a held claim "+
					"makes the next retry report a duplicate for an email never sent",
					fence.releases)
			}
			if fence.completions != 0 {
				t.Fatal("a dropped notification was marked completed")
			}
			if len(sender.sent) != 0 {
				t.Fatal("a dropped notification still sent an email")
			}
		})
	}
}

// TestBillingHandlerReleasesTheClaimAndRetriesWhenTheSendFails: the send
// failed, so nothing went out; the claim must be released or the fence would
// permanently skip a delivery that never happened.
func TestBillingHandlerReleasesTheClaimAndRetriesWhenTheSendFails(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{err: errors.New("smtp server unreachable")}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	err := handler.Work(context.Background(), billingExecution())
	if err == nil {
		t.Fatal("a failed send was reported as success")
	}
	if !hasCategory(err, jobruntime.CategoryRetryable) {
		t.Fatalf("classified as %v, want retryable", err)
	}
	if fence.releases != 1 {
		t.Fatalf("claim released %d times, want exactly 1", fence.releases)
	}
	if fence.completions != 0 {
		t.Fatal("a failed send was marked completed")
	}
}

// TestBillingHandlerNeverReleasesOrRetriesAfterASuccessfulSend is the
// duplicate-prevention invariant codex round 2 found: a transient failure
// writing completed_at must NOT release the claim and must NOT retry, because
// the email is already out.
func TestBillingHandlerNeverReleasesOrRetriesAfterASuccessfulSend(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{
		claim:       ClaimResult{Claimed: true},
		completeErr: errors.New("completion write is unavailable"),
	}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	if err := handler.Work(context.Background(), billingExecution()); err != nil {
		t.Fatalf("a sent email with a failed fence write must not retry, got %v", err)
	}
	if len(sender.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(sender.sent))
	}
	if fence.releases != 0 {
		t.Fatal("the claim was released after the email had already gone out; " +
			"a retry would duplicate it")
	}
}

// TestBillingHandlerCompletesWithoutSendingWhenTheOrgHasNoOwner pins the
// Python behavior: no owner is a data condition, not a retryable failure, and
// the notification is completed so it is not attempted forever.
func TestBillingHandlerCompletesWithoutSendingWhenTheOrgHasNoOwner(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	owners := &fakeOwners{err: ErrOrgOwnerNotFound}
	sender := &fakeSender{}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	if err := handler.Work(context.Background(), billingExecution()); err != nil {
		t.Fatalf("an ownerless organization must not fail the job, got %v", err)
	}
	if len(sender.sent) != 0 {
		t.Fatal("an email was sent with no owner to address")
	}
	if fence.completions != 1 || fence.releases != 0 {
		t.Fatalf("completions=%d releases=%d; want the row completed, not released",
			fence.completions, fence.releases)
	}
}

// TestBillingHandlerRetriesWhenTheOwnerLookupIsUnavailable separates a
// transient database failure from the permanent "no owner" condition above.
func TestBillingHandlerRetriesWhenTheOwnerLookupIsUnavailable(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	owners := &fakeOwners{err: errors.New("billing owner lookup is unavailable")}
	handler := newTestBillingHandler(t, store, fence, owners, &fakeSender{})

	err := handler.Work(context.Background(), billingExecution())
	if !hasCategory(err, jobruntime.CategoryRetryable) {
		t.Fatalf("classified as %v, want retryable", err)
	}
	if fence.releases != 1 {
		t.Fatalf("claim released %d times, want exactly 1", fence.releases)
	}
}

// TestBillingHandlerRejectsATenantMismatchBeforeClaiming keeps the
// authoritative-tenant guard from the pre-cutover handler: a row whose org
// disagrees with the envelope is never claimed, let alone emailed.
func TestBillingHandlerRejectsATenantMismatchBeforeClaiming(t *testing.T) {
	row := billingRow(`{}`)
	row.OrganizationID = "00000000-0000-4000-8000-00000000ffff"
	store := &fakeStore{billing: row}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	sender := &fakeSender{}
	handler := newTestBillingHandler(t, store, fence, &fakeOwners{}, sender)

	err := handler.Work(context.Background(), billingExecution())
	if !hasCategory(err, jobruntime.CategoryPermanent) {
		t.Fatalf("classified as %v, want permanent", err)
	}
	if fence.claims != 0 {
		t.Fatal("a tenant mismatch reached the fence")
	}
	if len(sender.sent) != 0 {
		t.Fatal("a tenant mismatch sent an email")
	}
}

// TestBillingHandlerRetriesWhenTheClaimItselfIsUnavailable: a database failure
// taking the claim is transient, and nothing is released (nothing was taken).
func TestBillingHandlerRetriesWhenTheClaimItselfIsUnavailable(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claimErr: errors.New("billing claim is unavailable")}
	handler := newTestBillingHandler(t, store, fence, &fakeOwners{}, &fakeSender{})

	err := handler.Work(context.Background(), billingExecution())
	if !hasCategory(err, jobruntime.CategoryRetryable) {
		t.Fatalf("classified as %v, want retryable", err)
	}
	if fence.releases != 0 {
		t.Fatal("a claim that was never taken was released")
	}
}

// TestBillingHandlerSurvivesAFailedRelease: the release is best-effort, and
// its failure must not replace the original error the caller is reporting.
func TestBillingHandlerSurvivesAFailedRelease(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{
		claim:      ClaimResult{Claimed: true},
		releaseErr: errors.New("billing claim release is unavailable"),
	}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{err: errors.New("smtp server unreachable")}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	err := handler.Work(context.Background(), billingExecution())
	if err == nil {
		t.Fatal("want the original send failure")
	}
	if !errors.Is(err, sender.err) {
		t.Fatalf("the release failure replaced the original error: %v", err)
	}
	if !hasCategory(err, jobruntime.CategoryRetryable) {
		t.Fatalf("classified as %v, want retryable", err)
	}
}

func TestNewBillingHandlerRequiresEveryDependency(t *testing.T) {
	store := &fakeStore{}
	fence := &fakeFence{}
	owners := &fakeOwners{}
	sender := &fakeSender{}
	for _, test := range []struct {
		name   string
		store  DeliveryStore
		fence  BillingFence
		owners OwnerLookup
		sender EmailSender
	}{
		{"no store", nil, fence, owners, sender},
		{"no fence", store, nil, owners, sender},
		{"no owners", store, fence, nil, sender},
		{"no sender", store, fence, owners, nil},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := NewBillingHandler(
				test.store, test.fence, test.owners, test.sender, ""); err == nil {
				t.Fatal("construction succeeded with a missing dependency")
			}
		})
	}
}

func TestEmailSenderSelectionFollowsTheExistingEnvironmentNames(t *testing.T) {
	for _, test := range []struct {
		name     string
		env      map[string]string
		wantName string
		wantErr  bool
	}{
		{"defaults to console", map[string]string{}, "console", false},
		{"console is explicit", map[string]string{"EMAIL_PROVIDER": "console"}, "console", false},
		{"case and spacing are tolerated",
			map[string]string{"EMAIL_PROVIDER": "  SMTP  "}, "smtp", false},
		{"smtp", map[string]string{"EMAIL_PROVIDER": "smtp", "SMTP_HOST": "mailpit"}, "smtp", false},
		{"resend via EMAIL_API_KEY",
			map[string]string{"EMAIL_PROVIDER": "resend", "EMAIL_API_KEY": "k"}, "resend", false},
		{"resend via RESEND_API_KEY",
			map[string]string{"EMAIL_PROVIDER": "resend", "RESEND_API_KEY": "k"}, "resend", false},
		{"resend without a key is refused",
			map[string]string{"EMAIL_PROVIDER": "resend"}, "", true},
		{"an unknown provider is refused, never silently defaulted",
			map[string]string{"EMAIL_PROVIDER": "sendgrid"}, "", true},
		{"a bad SMTP_PORT is refused",
			map[string]string{"EMAIL_PROVIDER": "smtp", "SMTP_PORT": "not-a-port"}, "", true},
	} {
		t.Run(test.name, func(t *testing.T) {
			// UNSET every variable first so a value leaking in from the
			// developer's own shell cannot decide the outcome. It must be
			// unset, not blanked: since the CHAOS-5353 r1 fix, blank means
			// "configured to nothing" and is a refusal, which is exactly what
			// TestEmptyEnvironmentValuesAreRefusedNotDefaulted pins. t.Setenv
			// first so the test framework restores the original on cleanup,
			// then Unsetenv to reach the genuinely-absent state.
			for _, name := range []string{
				"EMAIL_PROVIDER", "EMAIL_FROM_ADDRESS", "EMAIL_API_KEY", "RESEND_API_KEY",
				"SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_USE_TLS",
			} {
				t.Setenv(name, "")
				if err := os.Unsetenv(name); err != nil {
					t.Fatal(err)
				}
			}
			for name, value := range test.env {
				t.Setenv(name, value)
			}
			sender, err := NewEmailSenderFromEnv(&http.Client{Timeout: time.Second})
			if test.wantErr {
				if err == nil {
					t.Fatalf("want an error, got provider %q", sender.Name())
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if sender.Name() != test.wantName {
				t.Fatalf("provider = %q, want %q", sender.Name(), test.wantName)
			}
		})
	}
}

func TestSmtpUseTLSAcceptsThePythonTruthyValues(t *testing.T) {
	for value, want := range map[string]bool{
		"true": true, "TRUE": true, "1": true, "yes": true, " Yes ": true,
		"false": false, "0": false, "no": false, "": false, "on": false,
	} {
		t.Run(value, func(t *testing.T) {
			t.Setenv("EMAIL_PROVIDER", "smtp")
			t.Setenv("SMTP_USE_TLS", value)
			sender, err := NewEmailSenderFromEnv(nil)
			if err != nil {
				t.Fatal(err)
			}
			smtpSender, ok := sender.(*smtpEmailSender)
			if !ok {
				t.Fatalf("provider = %T", sender)
			}
			if smtpSender.useTLS != want {
				t.Fatalf("SMTP_USE_TLS=%q gave useTLS=%v, want %v", value, smtpSender.useTLS, want)
			}
		})
	}
}

func TestDecodeBillingAttributesMatchesPythonDefaultsAndCoercion(t *testing.T) {
	decoded, err := DecodeBillingAttributes(nil)
	if err != nil {
		t.Fatal(err)
	}
	// Python's `attributes.get(name, default)` defaults, exactly.
	if decoded.Currency != "usd" || decoded.AttemptCount != 1 ||
		decoded.AmountCents != 0 || decoded.DaysRemaining != 0 {
		t.Fatalf("defaults = %+v", decoded)
	}
	numeric, err := DecodeBillingAttributes([]byte(`{"amount_cents":"4999","days_remaining":2.9}`))
	if err != nil {
		t.Fatal(err)
	}
	if numeric.AmountCents != 4999 {
		t.Errorf("int(\"4999\") = %d, want 4999", numeric.AmountCents)
	}
	if numeric.DaysRemaining != 2 {
		t.Errorf("int(2.9) = %d, want 2 (Python truncates toward zero)", numeric.DaysRemaining)
	}
	for _, malformed := range []string{
		`{"amount_cents":"abc"}`,
		`{"attempt_count":null}`,
		`{"days_remaining":{"a":1}}`,
		`[]`,
	} {
		if _, err := DecodeBillingAttributes([]byte(malformed)); !errors.Is(err, ErrMalformedAttributes) {
			t.Errorf("DecodeBillingAttributes(%s) = %v, want ErrMalformedAttributes", malformed, err)
		}
	}
}

// ---------------------------------------------------------------------------
// CHAOS-5353 r1 regression pins. Each of these FAILS at the fix parent.
// ---------------------------------------------------------------------------

// TestRetryAfterASendFailureIsNeverReportedAsADuplicate is the r1 P1 pin.
//
// At the fix parent: attempt 1's send failed AND its release failed, so the
// claim stayed held; attempt 2 met that fresh uncompleted claim, logged
// duplicate_suppressed and returned nil. River recorded success for an email
// that was never sent, and the "will surface as a stale claim" comment could
// not come true because nothing retried again.
//
// At the tip: an uncompleted claim is Retryable, never success.
func TestRetryAfterASendFailureIsNeverReportedAsADuplicate(t *testing.T) {
	claimedAt := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC).Add(-time.Minute)
	store := &fakeStore{billing: billingRow(`{}`)}
	// The row this retry meets: claimed a minute ago, well inside the stale
	// window, never completed -- exactly the state a failed release leaves.
	fence := &fakeFence{claim: ClaimResult{ClaimedAt: &claimedAt}}
	sender := &fakeSender{}
	handler := newTestBillingHandler(t, store, fence, &fakeOwners{}, sender)

	err := handler.Work(context.Background(), billingExecution())
	if err == nil {
		t.Fatal("a retry meeting an UNCOMPLETED claim reported success; " +
			"the notification was never sent and River will not try again")
	}
	if !hasCategory(err, jobruntime.CategoryRetryable) {
		t.Fatalf("classified as %v, want retryable", err)
	}
	if len(sender.sent) != 0 {
		t.Fatal("the claim was held by another attempt but this one sent anyway")
	}
	if fence.completions != 0 {
		t.Fatal("a notification nobody sent was marked completed")
	}
}

// TestACompletedClaimStillResolvesImmediately guards the fix above from
// over-reaching: a genuine prior SUCCESS must still short-circuit to success,
// or every duplicate would retry until it went stale.
func TestACompletedClaimStillResolvesImmediately(t *testing.T) {
	at := time.Date(2026, 9, 7, 11, 59, 0, 0, time.UTC)
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{ClaimedAt: &at, CompletedAt: &at}}
	sender := &fakeSender{}
	handler := newTestBillingHandler(t, store, fence, &fakeOwners{}, sender)

	if err := handler.Work(context.Background(), billingExecution()); err != nil {
		t.Fatalf("a genuinely completed notification must succeed, got %v", err)
	}
	if len(sender.sent) != 0 || fence.completions != 0 {
		t.Fatal("a completed notification was acted on again")
	}
}

// TestAFailedReleaseIsRetriedSoTheNextAttemptCanSend is the other half of the
// r1 P1: the release itself is retried inline, so a transient database blip
// no longer strands the notification behind its own claim.
//
// At the fix parent the release was attempted exactly once, so releases==1 and
// the claim stayed held.
func TestAFailedReleaseIsRetriedSoTheNextAttemptCanSend(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	// The first release attempt fails, the second succeeds.
	fence := &fakeFence{claim: ClaimResult{Claimed: true}, releaseFailures: 1}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{err: errors.New("smtp server unreachable")}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	err := handler.Work(context.Background(), billingExecution())
	if !hasCategory(err, jobruntime.CategoryRetryable) {
		t.Fatalf("classified as %v, want retryable", err)
	}
	if fence.releases < 2 {
		t.Fatalf("release attempted %d time(s); a transient failure must be retried "+
			"or the notification is stranded behind its own claim", fence.releases)
	}
	if !fence.released {
		t.Fatal("the claim was never actually released, so the next attempt " +
			"cannot send this notification")
	}
}

// TestReleaseThatNeverSucceedsStillDoesNotFakeSuccess: the inline retry is
// bounded, so pin what happens when it is exhausted. The original error must
// survive, and nothing may report a delivery.
func TestReleaseThatNeverSucceedsStillDoesNotFakeSuccess(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{
		claim:      ClaimResult{Claimed: true},
		releaseErr: errors.New("billing claim release is unavailable"),
	}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{err: errors.New("smtp server unreachable")}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	err := handler.Work(context.Background(), billingExecution())
	if err == nil {
		t.Fatal("an undelivered notification reported success")
	}
	if !errors.Is(err, sender.err) {
		t.Fatalf("the release failure replaced the original send error: %v", err)
	}
	if fence.releases != releaseClaimAttempts {
		t.Fatalf("release attempted %d times, want the full bounded %d",
			fence.releases, releaseClaimAttempts)
	}
	if fence.completions != 0 {
		t.Fatal("a notification nobody sent was marked completed")
	}
}

// TestEnvelopeIdempotencyKeyMismatchRefusesBeforeClaiming is the r1 P2 pin for
// the restored identity fence. At the fix parent the handler ignored
// execution.Args.IdempotencyKey entirely and sent the row.
func TestEnvelopeIdempotencyKeyMismatchRefusesBeforeClaiming(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	execution := billingExecution()
	execution.Args.IdempotencyKey = "billing:some-other-row"

	err := handler.Work(context.Background(), execution)
	if err == nil {
		t.Fatal("a job whose envelope names a DIFFERENT row still sent the email")
	}
	if !hasCategory(err, jobruntime.CategoryPermanent) {
		t.Fatalf("classified as %v, want permanent", err)
	}
	if len(sender.sent) != 0 {
		t.Fatal("an email was sent despite the identity mismatch")
	}
	if fence.claims != 0 {
		t.Fatal("the mismatch reached the fence; it must refuse BEFORE claiming")
	}
}

// TestAnAbsentEnvelopeIdempotencyKeyStillSends keeps the restored fence from
// becoming a rolling-deploy hazard: a producer that never sets the key must
// degrade to "no cross-check", not to "billing mail stops".
func TestAnAbsentEnvelopeIdempotencyKeyStillSends(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	execution := billingExecution()
	execution.Args.IdempotencyKey = ""

	if err := handler.Work(context.Background(), execution); err != nil {
		t.Fatalf("an absent envelope key must not stop delivery, got %v", err)
	}
	if len(sender.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(sender.sent))
	}
}

// TestEmptyEnvironmentValuesAreRefusedNotDefaulted is the r1 P1 pin for
// empty-vs-absent. At the fix parent every one of these silently took a
// default -- EMAIL_PROVIDER="" became the console sender, which logs instead
// of sending while the handler marks the notification delivered.
func TestEmptyEnvironmentValuesAreRefusedNotDefaulted(t *testing.T) {
	for _, test := range []struct {
		name string
		env  map[string]string
	}{
		{"empty EMAIL_PROVIDER", map[string]string{"EMAIL_PROVIDER": ""}},
		{"empty EMAIL_FROM_ADDRESS", map[string]string{
			"EMAIL_PROVIDER": "console", "EMAIL_FROM_ADDRESS": ""}},
		{"empty SMTP_HOST", map[string]string{
			"EMAIL_PROVIDER": "smtp", "SMTP_HOST": ""}},
		{"empty SMTP_PORT", map[string]string{
			"EMAIL_PROVIDER": "smtp", "SMTP_PORT": ""}},
	} {
		t.Run(test.name, func(t *testing.T) {
			for name, value := range test.env {
				t.Setenv(name, value)
			}
			sender, err := NewEmailSenderFromEnv(nil)
			if err == nil {
				t.Fatalf("a configured-but-empty value was silently defaulted to %q; "+
					"billing mail would go nowhere while rows are marked delivered",
					sender.Name())
			}
		})
	}
}

// TestAbsentEnvironmentValuesStillTakeTheirDefaults is the counterpart: the
// fix must reject EMPTY without breaking ABSENT, which is the ordinary case.
func TestAbsentEnvironmentValuesStillTakeTheirDefaults(t *testing.T) {
	// t.Setenv FIRST so the framework registers a cleanup that restores the
	// caller's original value, THEN Unsetenv to reach the genuinely-absent
	// state. Unsetenv alone would leak: it discards whatever the caller had
	// and never puts it back, so a later test in the same binary would see an
	// environment this one silently emptied.
	for _, name := range []string{
		"EMAIL_PROVIDER", "EMAIL_FROM_ADDRESS", "SMTP_HOST", "SMTP_PORT",
	} {
		t.Setenv(name, "")
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
	}
	sender, err := NewEmailSenderFromEnv(nil)
	if err != nil {
		t.Fatalf("all variables absent must still yield the console default: %v", err)
	}
	if sender.Name() != "console" {
		t.Fatalf("provider = %q, want console", sender.Name())
	}
}

// ---------------------------------------------------------------------------
// CHAOS-5353 r2 regression pins. Each FAILS at this commit's parent.
// ---------------------------------------------------------------------------

// TestNoOwnerWithAFailedCompletionWriteIsNotReportedAsSuccess is the r2 P1
// pin, and the sibling of the r1 one.
//
// At the fix parent, the completion-write failure branch returned nil
// unconditionally. That is right when an email IS out -- releasing or
// retrying would duplicate it -- but wrong when nothing was sent, which is
// the no-owner case: there was nothing to duplicate, so returning nil merely
// stranded the row behind an uncompleted claim while River recorded success.
func TestNoOwnerWithAFailedCompletionWriteIsNotReportedAsSuccess(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{
		claim:       ClaimResult{Claimed: true},
		completeErr: errors.New("completion write is unavailable"),
	}
	owners := &fakeOwners{err: ErrOrgOwnerNotFound}
	sender := &fakeSender{}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	err := handler.Work(context.Background(), billingExecution())
	if err == nil {
		t.Fatal("nothing was sent and the completion write failed, yet Work " +
			"reported success; the row is stranded behind its own claim")
	}
	if !hasCategory(err, jobruntime.CategoryRetryable) {
		t.Fatalf("classified as %v, want retryable", err)
	}
	if fence.releases == 0 || !fence.released {
		t.Fatalf("the claim was not released (releases=%d released=%v), so the "+
			"retry cannot resolve this notification", fence.releases, fence.released)
	}
	if len(sender.sent) != 0 {
		t.Fatal("an email was sent for an organization with no owner")
	}
}

// TestASentEmailWithAFailedCompletionWriteStillNeverRetries guards the fix
// above from over-reaching. This is the opposite case and the reason the
// branch has to exist at all: the email IS out, so releasing or retrying
// would deliver it twice.
func TestASentEmailWithAFailedCompletionWriteStillNeverRetries(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{
		claim:       ClaimResult{Claimed: true},
		completeErr: errors.New("completion write is unavailable"),
	}
	owners := &fakeOwners{owner: OwnerContact{
		Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	if err := handler.Work(context.Background(), billingExecution()); err != nil {
		t.Fatalf("a delivered email must not retry on a bookkeeping failure, got %v", err)
	}
	if len(sender.sent) != 1 {
		t.Fatalf("sent %d messages, want 1", len(sender.sent))
	}
	if fence.releases != 0 {
		t.Fatal("the claim was released after the email had gone out; " +
			"a retry would duplicate it")
	}
}

// TestAppBaseURLDistinguishesAbsentFromEmpty is the r2 P2 pin. At the fix
// parent an explicitly empty APP_BASE_URL silently became the default, so
// every link in a trial email pointed at example.com.
func TestAppBaseURLDistinguishesAbsentFromEmpty(t *testing.T) {
	t.Run("absent takes the default", func(t *testing.T) {
		t.Setenv("APP_BASE_URL", "")
		if err := os.Unsetenv("APP_BASE_URL"); err != nil {
			t.Fatal(err)
		}
		base, err := AppBaseURLFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if base != "https://example.com" {
			t.Fatalf("base = %q, want the default", base)
		}
	})
	t.Run("empty is refused", func(t *testing.T) {
		t.Setenv("APP_BASE_URL", "")
		if _, err := AppBaseURLFromEnv(); !errors.Is(err, ErrAppBaseURLEmpty) {
			t.Fatalf("an explicitly empty APP_BASE_URL returned %v; it must be "+
				"refused, not silently defaulted", err)
		}
	})
	t.Run("whitespace only is refused", func(t *testing.T) {
		t.Setenv("APP_BASE_URL", "   ")
		if _, err := AppBaseURLFromEnv(); !errors.Is(err, ErrAppBaseURLEmpty) {
			t.Fatalf("a whitespace-only APP_BASE_URL returned %v", err)
		}
	})
	t.Run("a real value keeps its trailing-slash trim", func(t *testing.T) {
		t.Setenv("APP_BASE_URL", "https://app.example.test//")
		base, err := AppBaseURLFromEnv()
		if err != nil {
			t.Fatal(err)
		}
		if base != "https://app.example.test" {
			t.Fatalf("base = %q", base)
		}
	})
}

// ---------------------------------------------------------------------------
// CHAOS-5399 regression pin. FAILS at this commit's parent.
// ---------------------------------------------------------------------------

// TestAmbiguousProviderResultProducesExactlyOneSendAcrossTwoAttempts is the
// CHAOS-5399 red/green pin. At the fix parent, deliver() treated every Send
// error identically as "nothing was sent" and released the claim; a retry
// then claimed and sent again, so a provider that accepted a message and
// then merely failed to confirm it (a timed-out HTTP response, a lost SMTP
// connection after DATA) produced two emails for one notification.
func TestAmbiguousProviderResultProducesExactlyOneSendAcrossTwoAttempts(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{
		// "Accepted, then the response timed out": the provider genuinely
		// received the message (recorded in sent), but Send still reports
		// failure, and the failure is ambiguous rather than a clean
		// rejection.
		err: &AmbiguousSendError{
			Err: errors.New("resend API response uncertain: context deadline exceeded"),
		},
		recordAsSentDespiteError: true,
	}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	// Attempt 1: ambiguous result. The claim must NOT be released -- doing
	// so is exactly what lets a retry duplicate a message that may already
	// be out.
	err := handler.Work(context.Background(), billingExecution())
	if err == nil {
		t.Fatal("an ambiguous send was reported as success")
	}
	if !hasCategory(err, jobruntime.CategoryRetryable) {
		t.Fatalf("classified as %v, want retryable", err)
	}
	if fence.releases != 0 {
		t.Fatalf("the claim was released after an ambiguous result (releases=%d); "+
			"a retry can now send a duplicate", fence.releases)
	}

	// Attempt 2: this job's own retry meets the claim attempt 1 deliberately
	// left held -- exactly what a real `UPDATE ... WHERE claimed_at IS
	// NULL` reports the second time (claimed, not completed, not released).
	claimedAt := handler.now()
	fence.claim = ClaimResult{ClaimedAt: &claimedAt}
	err = handler.Work(context.Background(), billingExecution())
	if err == nil {
		t.Fatal("a retry meeting the still-held ambiguous claim reported success")
	}
	if !hasCategory(err, jobruntime.CategoryRetryable) {
		t.Fatalf("classified as %v, want retryable", err)
	}

	if len(sender.sent) != 1 {
		t.Fatalf("sent %d messages across two handler attempts, want exactly 1 -- "+
			"the ambiguous first attempt may already have delivered it", len(sender.sent))
	}
}

// TestAmbiguousProviderResultIsDistinctFromAnOrdinaryRejection guards the fix
// above from over-reaching: a genuinely clean rejection (no AmbiguousSendError)
// must still release and retry exactly as before -- otherwise every ordinary
// transient failure would stall behind an unreleased claim until it went
// stale, which is a much worse regression than the bug being fixed.
func TestAmbiguousProviderResultIsDistinctFromAnOrdinaryRejection(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{err: errors.New("resend API rejected the message: status 422")}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	err := handler.Work(context.Background(), billingExecution())
	if !hasCategory(err, jobruntime.CategoryRetryable) {
		t.Fatalf("classified as %v, want retryable", err)
	}
	if fence.releases != 1 {
		t.Fatalf("claim released %d times, want exactly 1 -- a clean rejection must "+
			"still free the row for a real retry", fence.releases)
	}
}

// ---------------------------------------------------------------------------
// CHAOS-5399 r1 (codex round 1, P1) regression pin.
// ---------------------------------------------------------------------------

// TestAmbiguousProviderResultSnoozesPastStaleness is the r1 P1 pin: this job
// kind's max_attempts is 4 (contracts/jobs/v1/registry.json) with
// bounded_exponential_jitter backoff -- comfortably under StaleClaimThreshold
// (15m). At the r1 fix parent, the ambiguous branch returned a plain
// jobruntime.Retryable, which consumes the job's bounded attempt budget the
// same as any ordinary failure. Every follow-up attempt within that budget
// meets the still-fresh, held claim and is suppressed as a duplicate
// (reportLostClaim's non-stale branch) -- so all 4 attempts are consumed
// WITHOUT the claim ever reaching staleness, River discards the job
// permanently, and nothing ever runs Work() for this notification again: the
// claim stays held forever with no automated path to an operator alert.
func TestAmbiguousProviderResultSnoozesPastStaleness(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{
		err:                      &AmbiguousSendError{Err: errors.New("resend API response uncertain: timeout")},
		recordAsSentDespiteError: true,
	}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	err := handler.Work(context.Background(), billingExecution())
	if !hasCategory(err, jobruntime.CategoryRetryable) {
		t.Fatalf("classified as %v, want retryable", err)
	}
	delay, snoozed := jobruntime.SnoozeDelay(err)
	if !snoozed {
		t.Fatal("an ambiguous result was a plain Retryable, not a snoozed retry -- " +
			"it will consume this job kind's bounded attempt budget (4, all well " +
			"under the 15m staleness threshold) and can never reach the staleness alert")
	}
	if delay <= StaleClaimThreshold {
		t.Fatalf("snooze delay = %v, want strictly more than StaleClaimThreshold (%v) -- "+
			"the follow-up attempt must land AFTER the claim is genuinely stale",
			delay, StaleClaimThreshold)
	}
}

// TestAmbiguousTimeoutCauseDoesNotLeakPastTheSnoozeMarker is the r2 P1 pin.
// At the r2 fix parent, the ambiguous branch passed deliverErr straight into
// jobruntime.RetryableAfter with its cause chain intact. jobruntime's own
// classify() checks errors.Is(err, context.DeadlineExceeded) BEFORE it ever
// inspects the RetryableAfter snooze marker -- and the single most common
// ambiguous shape (an http.Client response timeout) wraps exactly that
// cause, via resendEmailSender.Send's `fmt.Errorf("resend API response
// uncertain: %w", err)`. TestAmbiguousProviderResultSnoozesPastStaleness
// above used a plain errors.New("...timeout") string for its
// AmbiguousSendError, which never reproduced this: only a REAL
// context.DeadlineExceeded in the chain triggers classify's early branch.
// This test uses that real shape.
func TestAmbiguousTimeoutCauseDoesNotLeakPastTheSnoozeMarker(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{
		err: &AmbiguousSendError{
			// The exact shape resendEmailSender.Send produces on a real
			// client-side response timeout.
			Err: fmt.Errorf("resend API response uncertain: %w",
				fmt.Errorf("Post %q: %w", "https://api.resend.com/emails", context.DeadlineExceeded)),
		},
		recordAsSentDespiteError: true,
	}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	err := handler.Work(context.Background(), billingExecution())
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("Work()'s returned error still satisfies errors.Is(..., context.DeadlineExceeded) -- " +
			"jobruntime.classify checks exactly this BEFORE it looks for the RetryableAfter " +
			"snooze marker, so this would silently downgrade to an ordinary attempt-consuming " +
			"CategoryTimeout retry instead of snoozing past staleness")
	}
	delay, snoozed := jobruntime.SnoozeDelay(err)
	if !snoozed || delay <= StaleClaimThreshold {
		t.Fatalf("SnoozeDelay() = (%v, %v), want a delay > %v", delay, snoozed, StaleClaimThreshold)
	}
}

// TestAmbiguousOutcomeDuringADrainStillSnoozesAndHoldsTheClaim replaces the
// r3 residual's WARN pin. That WARN existed only to make an accepted
// jobruntime defect observable: classify() consulted the LIVE context for
// context.Canceled before it ever looked for the RetryableAfter snooze
// marker, so a worker draining at the exact moment an ambiguous outcome was
// classified bypassed this branch's snooze no matter what it returned.
// CHAOS-5455 fixed that ordering in jobruntime for every job kind
// (internal/jobruntime/classify_snooze_order_test.go pins the classification
// itself, including this handler's exact error shape), so a log line
// announcing a bypass that can no longer happen would now be false.
//
// What stays this handler's own to prove is that a drain changes NOTHING
// about the two properties this branch is responsible for: the ambiguous
// result is still returned as a snooze past StaleClaimThreshold, and the
// claim is still never released.
func TestAmbiguousOutcomeDuringADrainStillSnoozesAndHoldsTheClaim(t *testing.T) {
	store := &fakeStore{billing: billingRow(`{}`)}
	fence := &fakeFence{claim: ClaimResult{Claimed: true}}
	owners := &fakeOwners{owner: OwnerContact{Email: "o@example.test", FullName: "D", OrgName: "N"}}
	sender := &fakeSender{
		err:                      &AmbiguousSendError{Err: errors.New("resend API response uncertain: timeout")},
		recordAsSentDespiteError: true,
	}
	handler := newTestBillingHandler(t, store, fence, owners, sender)

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // the worker context is already done, as during a shutdown/drain

	err := handler.Work(ctx, billingExecution())
	if err == nil {
		t.Fatal("Work() = nil, want the ambiguous send's error")
	}
	delay, snoozed := jobruntime.SnoozeDelay(err)
	if !snoozed || delay <= StaleClaimThreshold {
		t.Fatalf("SnoozeDelay() = (%v, %v) under a drained context, want a delay > %v",
			delay, snoozed, StaleClaimThreshold)
	}
	if fence.releases != 0 {
		t.Fatalf("claim released %d times during a drain, want 0 -- an ambiguous "+
			"result is exactly the case where releasing lets a retry duplicate a "+
			"message that may already have gone out", fence.releases)
	}
}

// TestSMTPExplicitRejectionAfterDataIsNotAmbiguous is the r1 P1 pin for
// emailsender.go: an explicit SMTP rejection reply (e.g. "550 rejected")
// received after DATA's terminator is a DEFINITE, stated non-send -- no
// different from Mail/Rcpt/Data being rejected earlier -- and must release
// the claim for a real retry, not stall it behind FenceOutcomeAmbiguous.
func TestSMTPExplicitRejectionAfterDataIsNotAmbiguous(t *testing.T) {
	server := newFakeSMTPServer(t, "reject")
	defer server.listener.Close()
	host, port := splitHostPort(t, server.addr())

	sender := &smtpEmailSender{from: "billing@example.test", host: host, port: port}
	err := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if err == nil {
		t.Fatal("Send() = nil, want the explicit rejection error")
	}
	if _, ambiguous := isAmbiguous(err); ambiguous {
		t.Fatalf("Send() = %v classified ambiguous; an explicit SMTP rejection reply "+
			"IS the server's definite answer, not a lost acknowledgement", err)
	}
}

// TestResendKnownRejectionSurvivesABodyReadFailure is the r1 P1 pin: the
// status code alone is the whole diagnosis for a non-2xx response. A body
// read failure on TOP of an already-known 4xx must not promote it to
// ambiguous -- the body was never needed to classify a definite rejection in
// the first place.
func TestResendKnownRejectionSurvivesABodyReadFailure(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Content-Length", "1000")
			w.WriteHeader(http.StatusUnprocessableEntity)
			_, _ = w.Write([]byte("{")) // truncated: body read will fail
		}))
	defer server.Close()
	t.Setenv("RESEND_API_BASE_URL", server.URL)

	sender := &resendEmailSender{
		from: "billing@example.test", apiKey: "k",
		client: &http.Client{Timeout: 5 * time.Second},
	}
	err := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if err == nil {
		t.Fatal("Send() = nil, want the 422 rejection")
	}
	if _, ambiguous := isAmbiguous(err); ambiguous {
		t.Fatalf("Send() = %v classified ambiguous; a KNOWN 4xx needs nothing from "+
			"the body to classify, so a body read failure on top of it changes nothing", err)
	}
}

// TestResendTLSHandshakeFailureIsNotAmbiguous is the r1 P1 pin: a TLS
// handshake failure happens before the HTTP request is ever written, so no
// bytes reached the server -- a clean non-send, same as a refused dial. The
// old net.OpError/net.DNSError pattern-matching missed this shape entirely
// (a certificate failure surfaces as neither).
func TestResendTLSHandshakeFailureIsNotAmbiguous(t *testing.T) {
	// httptest.NewTLSServer's certificate is not trusted by a default
	// http.Client, so the handshake fails deterministically before any HTTP
	// request can be written.
	server := httptest.NewTLSServer(http.HandlerFunc(
		func(w http.ResponseWriter, r *http.Request) {
			t.Fatal("the handler must never run -- the handshake should fail first")
		}))
	defer server.Close()
	t.Setenv("RESEND_API_BASE_URL", server.URL)

	sender := &resendEmailSender{
		from: "billing@example.test", apiKey: "k",
		client: &http.Client{Timeout: 5 * time.Second},
	}
	err := sender.Send(context.Background(), EmailMessage{
		To: "owner@example.test", Subject: "s", HTML: "<p>h</p>",
	})
	if err == nil {
		t.Fatal("Send() = nil, want a TLS handshake failure")
	}
	if _, ambiguous := isAmbiguous(err); ambiguous {
		t.Fatalf("Send() = %v classified ambiguous; a TLS handshake failure happens "+
			"before any HTTP request byte is written, so this is a clean non-send", err)
	}
}

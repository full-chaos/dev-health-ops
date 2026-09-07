package operational

import (
	"context"
	"errors"
	"net/http"
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
	claim        ClaimResult
	claimErr     error
	completeErr  error
	releaseErr   error
	claims       int
	completions  int
	releases     int
	completedNow time.Time
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
	return fence.releaseErr
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
}

func (sender *fakeSender) Name() string { return "fake" }

func (sender *fakeSender) Send(_ context.Context, message EmailMessage) error {
	if sender.err != nil {
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

// TestBillingHandlerSendsNothingWhenTheClaimIsLost is the duplicate-suppression
// path: losing the claim must never reach the sender.
func TestBillingHandlerSendsNothingWhenTheClaimIsLost(t *testing.T) {
	// A completed claim is suppressed regardless of age; an in-flight one is
	// only an ordinary duplicate while it is INSIDE the stale window, so this
	// fixture sits well within it (the stale case is its own test below).
	completedAt := time.Date(2026, 9, 7, 11, 0, 0, 0, time.UTC)
	oldClaimedAt := completedAt
	freshClaimedAt := time.Date(2026, 9, 7, 12, 0, 0, 0, time.UTC).
		Add(-StaleClaimThreshold / 2)
	for _, test := range []struct {
		name  string
		claim ClaimResult
	}{
		{"already completed", ClaimResult{ClaimedAt: &oldClaimedAt, CompletedAt: &completedAt}},
		{"in flight", ClaimResult{ClaimedAt: &freshClaimedAt}},
	} {
		t.Run(test.name, func(t *testing.T) {
			store := &fakeStore{billing: billingRow(`{}`)}
			fence := &fakeFence{claim: test.claim}
			owners := &fakeOwners{}
			sender := &fakeSender{}
			handler := newTestBillingHandler(t, store, fence, owners, sender)

			if err := handler.Work(context.Background(), billingExecution()); err != nil {
				t.Fatalf("a suppressed duplicate must succeed, got %v", err)
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
			// Blank every variable first so a value leaking in from the
			// developer's own shell cannot decide the outcome. Blank reads as
			// absent everywhere in NewEmailSenderFromEnv.
			for _, name := range []string{
				"EMAIL_PROVIDER", "EMAIL_FROM_ADDRESS", "EMAIL_API_KEY", "RESEND_API_KEY",
				"SMTP_HOST", "SMTP_PORT", "SMTP_USERNAME", "SMTP_PASSWORD", "SMTP_USE_TLS",
			} {
				t.Setenv(name, "")
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

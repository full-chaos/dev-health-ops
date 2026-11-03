package pushexpiry

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushevent"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

const due = "2026-11-03T12:00:00Z"

type fakeStore struct {
	tokens    []Token
	sent      []Token
	pageCalls int
	failIDs   map[uuid.UUID]bool
}

func (store *fakeStore) Page(_ context.Context, after Cursor, limit int) ([]Token, error) {
	store.pageCalls++
	rows := append([]Token(nil), store.tokens...)
	sort.Slice(rows, func(i, j int) bool {
		if !rows[i].ExpiresAt.Equal(rows[j].ExpiresAt) {
			return rows[i].ExpiresAt.Before(rows[j].ExpiresAt)
		}
		return rows[i].ID.String() < rows[j].ID.String()
	})
	var out []Token
	for _, t := range rows {
		after_ := t.ExpiresAt.After(after.ExpiresAt) || (t.ExpiresAt.Equal(after.ExpiresAt) && t.ID.String() > after.ID.String())
		if after_ && len(out) < limit {
			out = append(out, t)
		}
	}
	return out, nil
}

func (store *fakeStore) Send(_ context.Context, token Token) error {
	if store.failIDs[token.ID] {
		return errors.New("boom")
	}
	store.sent = append(store.sent, token)
	return nil
}

func execution(scheduledFor string) *jobruntime.Execution[jobruntime.ExternalPushExpirySweepArgs] {
	return &jobruntime.Execution[jobruntime.ExternalPushExpirySweepArgs]{
		Args: jobruntime.ExternalPushExpirySweepArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.ExternalPushExpirySweepPayload]{
			Payload: jobcontract.ExternalPushExpirySweepPayload{ScheduledFor: scheduledFor},
		}},
	}
}

func token(expires time.Time) Token {
	source := uuid.New()
	return Token{ID: uuid.New(), OrgID: uuid.NewString(), SourceID: &source,
		Scopes: []byte(`["ingest:write","ingest:read"]`), ExpiresAt: expires}
}

func newHandler(t *testing.T, store Store, on bool) (*Handler, *bytes.Buffer) {
	t.Helper()
	var logs bytes.Buffer
	handler, err := NewHandler(store, on, slog.New(slog.NewTextHandler(&logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	return handler, &logs
}

func TestSweepOffTouchesNothing(t *testing.T) {
	store := &fakeStore{tokens: []Token{token(time.Now().Add(-time.Hour))}}
	handler, logs := newHandler(t, store, false)
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if store.pageCalls != 0 || len(store.sent) != 0 {
		t.Fatalf("flag off touched the store: pages=%d sent=%d", store.pageCalls, len(store.sent))
	}
	if !strings.Contains(logs.String(), "outcome=off") {
		t.Fatalf("no off line: %s", logs.String())
	}
}

func TestSweepRefusesAnInvalidScheduledFor(t *testing.T) {
	handler, _ := newHandler(t, &fakeStore{}, true)
	for _, bad := range []string{"", "not a time", "2026-11-03T12:00:00+02:00"} {
		err := handler.Work(context.Background(), execution(bad))
		if err == nil || !isPermanent(err) {
			t.Fatalf("%q: want a permanent error, got %v", bad, err)
		}
	}
}

func TestSweepSendsEveryExpiredTokenOnceAcrossPages(t *testing.T) {
	store := &fakeStore{}
	shared := time.Date(2026, 11, 1, 0, 0, 0, 0, time.UTC)
	for i := 0; i < BatchLimit*2+7; i++ {
		store.tokens = append(store.tokens, token(shared))
	}
	handler, _ := newHandler(t, store, true)
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	seen := map[uuid.UUID]int{}
	for _, sent := range store.sent {
		seen[sent.ID]++
	}
	if len(seen) != len(store.tokens) {
		t.Fatalf("sent %d distinct of %d: a page skipped tokens that share one expires_at", len(seen), len(store.tokens))
	}
	for id, n := range seen {
		if n != 1 {
			t.Fatalf("%s sent %d times", id, n)
		}
	}
}

func TestSweepStopsAtTheRunCap(t *testing.T) {
	store := &fakeStore{}
	for i := 0; i < RunCap+50; i++ {
		store.tokens = append(store.tokens, token(time.Date(2026, 11, 1, 0, 0, i, 0, time.UTC)))
	}
	handler, _ := newHandler(t, store, true)
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if len(store.sent) != RunCap {
		t.Fatalf("sent %d, want the cap %d", len(store.sent), RunCap)
	}
}

func TestSweepCountsAFailedTokenAndGoesOn(t *testing.T) {
	store := &fakeStore{failIDs: map[uuid.UUID]bool{}}
	for i := 0; i < 3; i++ {
		store.tokens = append(store.tokens, token(time.Date(2026, 11, 1, 0, 0, i, 0, time.UTC)))
	}
	store.failIDs[store.tokens[0].ID] = true
	handler, logs := newHandler(t, store, true)
	err := handler.Work(context.Background(), execution(due))
	if err == nil || isPermanent(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
	if len(store.sent) != 2 {
		t.Fatalf("a failed token stopped the run: sent %d", len(store.sent))
	}
	if strings.Contains(logs.String(), "boom") {
		t.Fatalf("the log holds the error text: %s", logs.String())
	}
}

func TestExpiredEventShape(t *testing.T) {
	expires := time.Date(2026, 11, 1, 3, 4, 5, 0, time.FixedZone("x", 3600))
	tok := token(expires)
	event, err := ExpiredEvent(tok)
	if err != nil {
		t.Fatal(err)
	}
	if event.EventType != externalpush.Expired || event.AggregateID != tok.ID.String() ||
		event.Binding != externalpush.BindingSource || event.SourceID != tok.SourceID.String() {
		t.Fatalf("event = %+v", event)
	}
	if event.Change.Status != "expired" || event.Change.ExpiresAt == nil || !event.Change.ExpiresAt.Equal(expires) || event.Change.ExpiresAt.Location() != time.UTC {
		t.Fatalf("change = %+v", event.Change)
	}
	if event.Snapshot == nil || event.Snapshot.Fingerprint != pushevent.Fingerprint(tok.ID) ||
		strings.Join(event.Snapshot.Scopes, ",") != "ingest:read,ingest:write" || event.Snapshot.Status != "active" {
		t.Fatalf("snapshot = %+v", event.Snapshot)
	}
}

func TestExpiredEventOfAnOrganizationWideTokenHasNoSource(t *testing.T) {
	tok := token(time.Now().Add(-time.Hour))
	tok.SourceID = nil
	event, err := ExpiredEvent(tok)
	if err != nil {
		t.Fatal(err)
	}
	if event.Binding != externalpush.BindingOrganization || event.SourceID != "" {
		t.Fatalf("event = %+v", event)
	}
}

func TestEveryExpiredEventPassesTheContractCheck(t *testing.T) {
	for _, orgWide := range []bool{false, true} {
		tok := token(time.Now().Add(-time.Hour))
		if orgWide {
			tok.SourceID = nil
		}
		event, err := ExpiredEvent(tok)
		if err != nil {
			t.Fatal(err)
		}
		if err := externalpush.Check(event.EventType, event.AggregateID, event.OrganizationID, event.Binding, event.SourceID, event.Change); err != nil {
			t.Fatalf("orgWide=%v: event: %v", orgWide, err)
		}
		if event.Snapshot == nil {
			t.Fatal("no snapshot")
		}
		if err := externalpush.Check(externalpush.Created, event.AggregateID, event.OrganizationID, event.Binding, event.SourceID, *event.Snapshot); err != nil {
			t.Fatalf("orgWide=%v: snapshot: %v", orgWide, err)
		}
	}
}

func TestSweepLogsHoldNoScopeOrNameText(t *testing.T) {
	store := &fakeStore{tokens: []Token{token(time.Now().Add(-time.Hour))}}
	handler, logs := newHandler(t, store, true)
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "ingest:") || strings.Contains(logs.String(), store.tokens[0].OrgID) {
		t.Fatalf("log holds scope or organization text: %s", logs.String())
	}
	if !strings.Contains(logs.String(), "sent=1") {
		t.Fatalf("no run line with a count: %s", logs.String())
	}
}

func TestScanSQLTakesNoLockAndWritesNothing(t *testing.T) {
	upper := strings.ToUpper(pageSQL)
	for _, word := range []string{"FOR UPDATE", "FOR SHARE", "FOR NO KEY", "FOR KEY SHARE", "UPDATE ", "SKIP LOCKED"} {
		if strings.Contains(upper, word) {
			t.Fatalf("scan SQL holds %q", word)
		}
	}
	if !strings.Contains(pageSQL, "revoked_at IS NULL") || !strings.Contains(pageSQL, "expires_at <= now()") {
		t.Fatal("scan SQL lost the unrevoked or the expiry predicate")
	}
	for _, column := range []string{"NAME", "TOKEN_HASH", "TOKEN_PREFIX", "LAST_USED"} {
		if strings.Contains(upper, column) {
			t.Fatalf("scan SQL reads %q", column)
		}
	}
}

func isPermanent(err error) bool {
	return strings.Contains(err.Error(), string(jobruntime.CategoryPermanent))
}

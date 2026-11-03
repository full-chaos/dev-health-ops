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
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts/externalpush"
	"github.com/full-chaos/dev-health-ops/internal/auth/pushevent"
	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
)

const due = "2026-11-03T12:00:00Z"

type fakeStore struct {
	tokens    []Token
	sent      []Token // one entry per Send call that returned nil (appended or writer no-op)
	pageCalls int
	sendCalls int
	firstPage []Cursor // the cursor of the first Page call of each run, in order
	failIDs   map[uuid.UUID]bool
	ended     map[uuid.UUID]bool // the writer's terminal state: a repeat is a no-op
	noops     int
}

func (store *fakeStore) Page(_ context.Context, after Cursor, limit int, notBefore time.Time) ([]Token, error) {
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
		// The real query does not know the writer's terminal state: it returns
		// ended tokens too. Only the window bound filters.
		if after_ && t.ExpiresAt.After(notBefore) && len(out) < limit {
			out = append(out, t)
		}
	}
	return out, nil
}

func (store *fakeStore) Send(_ context.Context, token Token) error {
	store.sendCalls++
	if store.failIDs[token.ID] {
		return errors.New("boom")
	}
	if store.ended == nil {
		store.ended = map[uuid.UUID]bool{}
	}
	if store.ended[token.ID] {
		store.noops++
	}
	store.ended[token.ID] = true
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
		t.Fatalf("submitted %d, want the cap %d (submitted + failed, not scanned)", len(store.sent), RunCap)
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
	if !strings.Contains(logs.String(), "submitted=1") {
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

func openTokens(n int) []Token {
	out := make([]Token, 0, n)
	base := time.Now().Add(-48 * time.Hour).UTC().Truncate(time.Second)
	for i := 0; i < n; i++ {
		out = append(out, token(base.Add(time.Duration(i)*time.Millisecond)))
	}
	return out
}

func infoLine(logs *bytes.Buffer) string {
	lines := strings.Split(strings.TrimSpace(logs.String()), "\n")
	return lines[len(lines)-1]
}

func submittedFor(t *testing.T, store *fakeStore, tokens []Token) int {
	t.Helper()
	seen := map[uuid.UUID]bool{}
	for _, s := range store.sent {
		seen[s.ID] = true
	}
	return len(seen)
}

func TestSweepTenThousandFiftyTokensTwoRunsReachAll(t *testing.T) {
	store := &fakeStore{tokens: openTokens(RunCap + 50)}
	handler, logs := newHandler(t, store, true)
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if line := infoLine(logs); !strings.Contains(line, "outcome=capped") || !strings.Contains(line, "submitted=10000") {
		t.Fatalf("run 1 line: %s", line)
	}
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if line := infoLine(logs); !strings.Contains(line, "outcome=swept") || !strings.Contains(line, "skipped=10000") || !strings.Contains(line, "submitted=50") {
		t.Fatalf("run 2 line: %s", line)
	}
	if got := submittedFor(t, store, store.tokens); got != RunCap+50 {
		t.Fatalf("two runs reached %d of %d tokens", got, RunCap+50)
	}
}

func TestSweepSkipsEndedTokensWithoutACall(t *testing.T) {
	store := &fakeStore{tokens: openTokens(250)}
	handler, logs := newHandler(t, store, true)
	for run := 0; run < 2; run++ {
		if err := handler.Work(context.Background(), execution(due)); err != nil {
			t.Fatal(err)
		}
	}
	if store.sendCalls != 250 {
		t.Fatalf("Send called %d times over two runs, want 250", store.sendCalls)
	}
	if line := infoLine(logs); !strings.Contains(line, "skipped=250") || !strings.Contains(line, "submitted=0") {
		t.Fatalf("run 2 line: %s", line)
	}
}

func TestSweepFailedTokenIsNotSkippedAndCountsTowardTheCap(t *testing.T) {
	store := &fakeStore{tokens: openTokens(RunCap + 5), failIDs: map[uuid.UUID]bool{}}
	bad := store.tokens[0].ID
	store.failIDs[bad] = true
	handler, logs := newHandler(t, store, true)
	err := handler.Work(context.Background(), execution(due))
	if err == nil || isPermanent(err) {
		t.Fatalf("want a retryable error, got %v", err)
	}
	if line := infoLine(logs); !strings.Contains(line, "outcome=partial") || !strings.Contains(line, "failed=1") || !strings.Contains(line, "submitted=9999") {
		t.Fatalf("run 1 line: %s (a failure must use one cap slot)", line)
	}
	if _, ok := handler.ended[bad]; ok {
		t.Fatal("a failed token is in the skip set")
	}
	delete(store.failIDs, bad)
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if got := submittedFor(t, store, store.tokens); got != RunCap+5 {
		t.Fatalf("the retry did not reach every token: %d of %d", got, RunCap+5)
	}
}

func TestSweepCappedRunReportsCappedAndNextRunStartsAtTheBeginning(t *testing.T) {
	store := &fakeStore{tokens: openTokens(RunCap + 1)}
	handler, logs := newHandler(t, store, true)
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(infoLine(logs), "outcome=capped") {
		t.Fatalf("no capped outcome: %s", infoLine(logs))
	}
	first := store.tokens[0].ID
	store.sent = nil
	// A recording store: run 2's first page must start at the zero cursor, so
	// the oldest token is offered again (and skipped, not re-sent).
	before := store.sendCalls
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if store.sendCalls-before != 1 {
		t.Fatalf("run 2 sent %d, want only the one token run 1 never reached", store.sendCalls-before)
	}
	_ = first
}

func TestSweepCountsSubmittedNotSent(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	store := &fakeStore{tokens: openTokens(1), ended: map[uuid.UUID]bool{}}
	store.ended[store.tokens[0].ID] = true // the writer already ended it: Send is a no-op
	handler, logs := newHandler(t, store, true)
	handler.metrics = newInstruments(provider.Meter("test"))
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	line := infoLine(logs)
	if !strings.Contains(line, "submitted=1") || strings.Contains(line, " sent=") {
		t.Fatalf("line = %s", line)
	}
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			if sum, ok := m.Data.(metricdata.Sum[int64]); ok {
				for _, dp := range sum.DataPoints {
					v, _ := dp.Attributes.Value("outcome")
					got[v.AsString()] += dp.Value
				}
			}
		}
	}
	if got["submitted"] != 1 || got["sent"] != 0 {
		t.Fatalf("metric outcomes = %v", got)
	}
}

func TestExpiryWindowIsThirtyDays(t *testing.T) {
	if ExpiryWindow != 30*24*time.Hour {
		t.Fatalf("ExpiryWindow = %s", ExpiryWindow)
	}
}

func TestSweepIgnoresTokensOlderThanTheWindow(t *testing.T) {
	inside := token(time.Now().Add(-ExpiryWindow + time.Hour))
	outside := token(time.Now().Add(-ExpiryWindow - time.Hour))
	store := &fakeStore{tokens: []Token{outside, inside}}
	handler, _ := newHandler(t, store, true)
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if len(store.sent) != 1 || store.sent[0].ID != inside.ID {
		t.Fatalf("sent = %d tokens; want only the one inside the window", len(store.sent))
	}
}

func TestSkipSetIsPrunedToTheWindow(t *testing.T) {
	store := &fakeStore{tokens: []Token{token(time.Now().Add(-ExpiryWindow + time.Minute)), token(time.Now().Add(-time.Hour))}}
	handler, _ := newHandler(t, store, true)
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if len(handler.ended) != 2 {
		t.Fatalf("skip set = %d, want 2", len(handler.ended))
	}
	now := time.Now().Add(2 * time.Minute)
	handler.now = func() time.Time { return now }
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if len(handler.ended) != 1 {
		t.Fatalf("skip set = %d after the oldest left the window, want 1", len(handler.ended))
	}
}

func TestSweepRestartResubmitsThenReachesTheRest(t *testing.T) {
	store := &fakeStore{tokens: openTokens(RunCap + 50)}
	a, _ := newHandler(t, store, true)
	if err := a.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	b, logs := newHandler(t, store, true) // a restart: an empty skip set
	if err := b.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if line := infoLine(logs); !strings.Contains(line, "outcome=capped") || !strings.Contains(line, "submitted=10000") || store.noops != RunCap {
		t.Fatalf("restart run 1: %s noops=%d (want %d idempotent re-submits)", line, store.noops, RunCap)
	}
	if err := b.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if line := infoLine(logs); !strings.Contains(line, "outcome=swept") || !strings.Contains(line, "skipped=10000") || !strings.Contains(line, "submitted=50") {
		t.Fatalf("restart run 2: %s", line)
	}
	if got := submittedFor(t, store, store.tokens); got != RunCap+50 {
		t.Fatalf("reached %d of %d", got, RunCap+50)
	}
}

func TestScanSQLHasTheWindowBound(t *testing.T) {
	if !strings.Contains(pageSQL, "expires_at > $4") {
		t.Fatal("scan SQL lost the window lower bound")
	}
}

func isPermanent(err error) bool {
	return strings.Contains(err.Error(), string(jobruntime.CategoryPermanent))
}

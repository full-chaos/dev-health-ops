package providerrevocation

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/full-chaos/dev-health-ops/internal/jobcontract"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	sentinelToken  = "SENTINEL-PD-TOKEN-7f3a"
	sentinelCipher = "SENTINEL-PD-CIPHER-91c2"
	sentinelBody   = "SENTINEL-PD-PROVIDER-BODY-4d0e"
)

type fakeRow struct {
	row      Row
	attempts int
	sent     bool
	failures []string
}

type fakeStore struct {
	due     []*fakeRow
	next    int
	nextErr error
	sentErr error
	// lostDeletes makes that many Sent calls fail after the provider call, as
	// a DELETE whose commit is lost; the released row is then due again.
	lostDeletes int
	failedErr   error
	totalsErr   error
	nextCalls   int
	released    int
	pending     int64
	exhausted   int64
	totalCalls  int
	lost        bool
}

func (store *fakeStore) Next(context.Context) (Claim, bool, error) {
	store.nextCalls++
	if store.nextErr != nil {
		return nil, false, store.nextErr
	}
	if store.next >= len(store.due) {
		return nil, false, nil
	}
	row := store.due[store.next]
	store.next++
	return &fakeClaim{store: store, row: row}, true, nil
}

func (store *fakeStore) Totals(context.Context) (int64, int64, error) {
	store.totalCalls++
	return store.pending, store.exhausted, store.totalsErr
}

type fakeClaim struct {
	store *fakeStore
	row   *fakeRow
}

func (claim *fakeClaim) Row() Row { return claim.row.row }

func (claim *fakeClaim) Sent(context.Context) error {
	if claim.store.sentErr != nil {
		return claim.store.sentErr
	}
	if claim.store.lostDeletes > 0 {
		claim.store.lostDeletes--
		claim.store.lost = true
		return errors.New("commit lost")
	}
	claim.row.sent = true
	return nil
}

func (claim *fakeClaim) Failed(_ context.Context, reason string) (int, error) {
	if claim.store.failedErr != nil {
		return 0, claim.store.failedErr
	}
	claim.row.attempts++
	claim.row.failures = append(claim.row.failures, reason)
	return claim.row.attempts, nil
}

func (claim *fakeClaim) Release(context.Context) {
	claim.store.released++
	if claim.store.lost {
		claim.store.lost = false
		claim.store.next--
	}
}

// fakeDecryptor opens "sealed:<token>" and refuses anything else.
type fakeDecryptor struct{}

func (fakeDecryptor) Decrypt(ciphertext secrets.Value) ([]byte, error) {
	text := ciphertext.Reveal()
	if token, ok := strings.CutPrefix(text, "sealed:"); ok {
		return []byte(token), nil
	}
	return nil, errors.New("bad cipher " + text)
}

type fakeRevoker struct {
	calls []string
	fail  map[string]bool
}

func (revoker *fakeRevoker) revoke(_ context.Context, token string) error {
	revoker.calls = append(revoker.calls, token)
	if revoker.fail[token] {
		return fmt.Errorf("pagerduty answered 503: %s for %s", sentinelBody, token)
	}
	return nil
}

func dueRow(ciphertext string, attempts int) *fakeRow {
	return &fakeRow{row: Row{ID: uuid.New(), Ciphertext: ciphertext}, attempts: attempts}
}

func execution(scheduledFor string) *jobruntime.Execution[jobruntime.ProviderOAuthRevocationsArgs] {
	return &jobruntime.Execution[jobruntime.ProviderOAuthRevocationsArgs]{
		Args: jobruntime.ProviderOAuthRevocationsArgs{EnvelopeArgs: jobruntime.EnvelopeArgs[jobcontract.ProviderOAuthRevocationsPayload]{
			Payload: jobcontract.ProviderOAuthRevocationsPayload{ScheduledFor: scheduledFor},
		}},
	}
}

const due = "2026-10-30T12:00:00Z"

type harness struct {
	handler *Handler
	store   *fakeStore
	revoker *fakeRevoker
	logs    *bytes.Buffer
	reader  *sdkmetric.ManualReader
}

func newHarness(t *testing.T, store *fakeStore, configured bool, fail ...string) harness {
	t.Helper()
	revoker := &fakeRevoker{fail: map[string]bool{}}
	for _, token := range fail {
		revoker.fail[token] = true
	}
	logs := &bytes.Buffer{}
	handler, err := NewHandler(store, fakeDecryptor{}, revoker.revoke, configured, slog.New(slog.NewJSONHandler(logs, nil)))
	if err != nil {
		t.Fatal(err)
	}
	reader := sdkmetric.NewManualReader()
	handler.metrics = newInstruments(sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader)).Meter("test"))
	return harness{handler: handler, store: store, revoker: revoker, logs: logs, reader: reader}
}

func (h harness) collect(t *testing.T) metricdata.ResourceMetrics {
	t.Helper()
	var data metricdata.ResourceMetrics
	if err := h.reader.Collect(context.Background(), &data); err != nil {
		t.Fatal(err)
	}
	return data
}

func (h harness) counter(t *testing.T, outcome string) int64 {
	t.Helper()
	for _, scope := range h.collect(t).ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name != rowsInstrument {
				continue
			}
			for _, point := range m.Data.(metricdata.Sum[int64]).DataPoints {
				if value, _ := point.Attributes.Value("outcome"); value.AsString() == outcome {
					return point.Value
				}
			}
		}
	}
	return 0
}

func (h harness) gauge(t *testing.T) (int64, bool) {
	t.Helper()
	for _, scope := range h.collect(t).ScopeMetrics {
		for _, m := range scope.Metrics {
			if m.Name == exhaustedInstrument {
				points := m.Data.(metricdata.Gauge[int64]).DataPoints
				if len(points) == 1 {
					return points[0].Value, true
				}
			}
		}
	}
	return 0, false
}

func TestWorkSendsADueRowAndRetriesARefusedOne(t *testing.T) {
	ok, refused := dueRow("sealed:tok-ok", 0), dueRow("sealed:tok-503", 0)
	h := newHarness(t, &fakeStore{due: []*fakeRow{ok, refused}}, true, "tok-503")
	if err := h.handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if !ok.sent || ok.attempts != 0 {
		t.Fatalf("ok row sent=%v attempts=%d, want sent at 0 attempts", ok.sent, ok.attempts)
	}
	if refused.sent || refused.attempts != 1 || !reflect.DeepEqual(refused.failures, []string{ReasonRemoteRevokeFailed}) {
		t.Fatalf("refused row sent=%v attempts=%d failures=%v", refused.sent, refused.attempts, refused.failures)
	}
	if !reflect.DeepEqual(h.revoker.calls, []string{"tok-ok", "tok-503"}) {
		t.Fatalf("revoke calls %v", h.revoker.calls)
	}
	if h.counter(t, "revoked") != 1 || h.counter(t, "retried") != 1 || h.counter(t, "exhausted") != 0 {
		t.Fatalf("counters revoked=%d retried=%d exhausted=%d", h.counter(t, "revoked"), h.counter(t, "retried"), h.counter(t, "exhausted"))
	}
	if !strings.Contains(h.logs.String(), `"claimed":2,"revoked":1,"retried":1,"exhausted_now":0`) {
		t.Fatalf("run line missing counts: %s", h.logs.String())
	}
}

func TestWorkStopsAtMaxAttemptsLoudly(t *testing.T) {
	last := dueRow("sealed:tok-last", MaxAttempts-1)
	store := &fakeStore{due: []*fakeRow{last}, exhausted: 3}
	h := newHarness(t, store, true, "tok-last")
	if err := h.handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if last.attempts != MaxAttempts || last.sent {
		t.Fatalf("row attempts=%d sent=%v", last.attempts, last.sent)
	}
	if h.counter(t, "exhausted") != 1 || h.counter(t, "retried") != 0 {
		t.Fatalf("counters exhausted=%d retried=%d", h.counter(t, "exhausted"), h.counter(t, "retried"))
	}
	if value, ok := h.gauge(t); !ok || value != 3 {
		t.Fatalf("exhausted gauge=%d recorded=%v, want 3", value, ok)
	}
	warn := ""
	for _, line := range strings.Split(strings.TrimSpace(h.logs.String()), "\n") {
		if strings.Contains(line, "provider_oauth_revocations.exhausted") {
			warn = line
		}
	}
	if !strings.Contains(warn, `"level":"WARN"`) || !strings.Contains(warn, `"revocation_id":"`+last.row.ID.String()+`"`) {
		t.Fatalf("no WARN line naming the row: %s", h.logs.String())
	}
	if !strings.Contains(h.logs.String(), `"exhausted_total":3`) {
		t.Fatalf("run line lacks exhausted_total: %s", h.logs.String())
	}
}

func TestWorkRecordsAnUndecryptableRow(t *testing.T) {
	bad := dueRow("not-sealed", 0)
	h := newHarness(t, &fakeStore{due: []*fakeRow{bad}}, true)
	if err := h.handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if len(h.revoker.calls) != 0 || !reflect.DeepEqual(bad.failures, []string{ReasonTokenDecryptFailed}) {
		t.Fatalf("calls=%v failures=%v", h.revoker.calls, bad.failures)
	}
}

func TestWorkWithoutAClientIDTouchesNoRow(t *testing.T) {
	store := &fakeStore{due: []*fakeRow{dueRow("sealed:tok", 0)}, pending: 4}
	h := newHarness(t, store, false)
	if err := h.handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if store.nextCalls != 0 || len(h.revoker.calls) != 0 || store.due[0].attempts != 0 {
		t.Fatalf("unconfigured run claimed %d rows, called %d times", store.nextCalls, len(h.revoker.calls))
	}
	if !strings.Contains(h.logs.String(), `"skipped_unconfigured":4`) {
		t.Fatalf("no unconfigured count: %s", h.logs.String())
	}
}

func TestWorkClaimsAtMostOneBatch(t *testing.T) {
	var rows []*fakeRow
	for index := 0; index < BatchLimit+5; index++ {
		rows = append(rows, dueRow(fmt.Sprintf("sealed:tok-%d", index), 0))
	}
	store := &fakeStore{due: rows}
	h := newHarness(t, store, true)
	if err := h.handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if store.nextCalls != BatchLimit || len(h.revoker.calls) != BatchLimit {
		t.Fatalf("claims=%d calls=%d, want %d", store.nextCalls, len(h.revoker.calls), BatchLimit)
	}
}

func TestWorkRefusesAnInvalidRequest(t *testing.T) {
	h := newHarness(t, &fakeStore{}, true)
	for _, scheduledFor := range []string{"", "2026-10-30T12:00:00+01:00", "yesterday"} {
		err := h.handler.Work(context.Background(), execution(scheduledFor))
		if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryPermanent)) {
			t.Fatalf("Work(%q) = %v, want permanent", scheduledFor, err)
		}
	}
}

func TestWorkRetriesStoreFailures(t *testing.T) {
	for name, store := range map[string]*fakeStore{
		"claim":  {nextErr: errors.New("postgres unavailable")},
		"sent":   {due: []*fakeRow{dueRow("sealed:tok", 0)}, sentErr: errors.New("delete failed")},
		"failed": {due: []*fakeRow{dueRow("sealed:tok-503", 0)}, failedErr: errors.New("update failed")},
		"totals": {totalsErr: errors.New("count failed")},
	} {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, store, true, "tok-503")
			err := h.handler.Work(context.Background(), execution(due))
			if err == nil || !strings.Contains(err.Error(), string(jobruntime.CategoryRetryable)) {
				t.Fatalf("Work() = %v, want retryable", err)
			}
		})
	}
}

// No decrypted token, ciphertext or provider answer reaches a log line, a
// returned error, a metric attribute or the River payload.
func TestNoTokenCiphertextOrProviderBodyLeaves(t *testing.T) {
	sealed := "sealed:" + sentinelToken
	cases := map[string]*fakeStore{
		"refused":       {due: []*fakeRow{dueRow(sealed, 0)}},
		"exhausted":     {due: []*fakeRow{dueRow(sealed, MaxAttempts-1)}},
		"undecryptable": {due: []*fakeRow{dueRow(sentinelCipher, 0)}},
		"write fails":   {due: []*fakeRow{dueRow(sealed, 0)}, failedErr: errors.New("update failed")},
	}
	for name, store := range cases {
		t.Run(name, func(t *testing.T) {
			h := newHarness(t, store, true, sentinelToken)
			err := h.handler.Work(context.Background(), execution(due))
			surfaces := h.logs.String() + fmt.Sprint(h.collect(t))
			if err != nil {
				surfaces += err.Error()
			}
			for _, sentinel := range []string{sentinelToken, sentinelCipher, sentinelBody} {
				if strings.Contains(surfaces, sentinel) {
					t.Fatalf("%s leaked: %s", sentinel, surfaces)
				}
			}
		})
	}
	payload := reflect.TypeOf(jobcontract.ProviderOAuthRevocationsPayload{})
	if payload.NumField() != 1 || payload.Field(0).Name != "ScheduledFor" {
		t.Fatalf("the River payload carries more than the due time: %v", payload)
	}
}

func TestClaimSQLIsTheSharedClaimRule(t *testing.T) {
	for _, want := range []string{
		"FOR UPDATE SKIP LOCKED",
		"LIMIT 1",
		fmt.Sprintf("attempts < %d", MaxAttempts),
		"(attempts = 0 OR updated_at <= now() - LEAST(interval '1 minute' * power(2, attempts - 1), interval '6 hours'))",
		"NOT (purpose = 'setup' AND attempts = 0 AND created_at > now() - interval '900 seconds')",
		"provider = 'pagerduty' AND status = 'pending'",
	} {
		if !strings.Contains(claimSQL, want) {
			t.Fatalf("claim SQL lacks %q:\n%s", want, claimSQL)
		}
	}
	if !strings.Contains(totalsSQL, fmt.Sprintf("attempts >= %d", MaxAttempts)) {
		t.Fatalf("totals SQL does not count exhausted rows:\n%s", totalsSQL)
	}
}

func TestNewHandlerRefusesMissingDependencies(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	revoke := func(context.Context, string) error { return nil }
	if _, err := NewHandler(nil, fakeDecryptor{}, revoke, true, logger); err == nil {
		t.Fatal("nil store accepted")
	}
	if _, err := NewHandler(&fakeStore{}, nil, revoke, true, logger); err == nil {
		t.Fatal("nil decryptor accepted")
	}
	if _, err := NewHandler(&fakeStore{}, fakeDecryptor{}, nil, true, logger); err == nil {
		t.Fatal("nil revoker accepted")
	}
}

// RFC 7009 section 2.2: the revocation endpoint answers 200 for a token that is
// invalid or already revoked ("invalid tokens do not cause an error response").
// The repository holds no PagerDuty document or recorded answer that says
// otherwise, so the cell pins the RFC answer on the real client: a 200 for an
// already-revoked token counts as sent; a 4xx never does.
func TestAnAlreadyRevokedAnswerCountsAsSentAndA4xxDoesNot(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		values, _ := url.ParseQuery(string(body))
		switch values.Get("token") {
		case "tok-already-revoked":
			w.WriteHeader(http.StatusOK)
		default:
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"error":"invalid_request"}`))
		}
	}))
	defer server.Close()
	config := providerfoundation.PagerDutyRevokeConfig{ClientID: "client", RevokeURL: server.URL}
	revoked, refused := dueRow("sealed:tok-already-revoked", 0), dueRow("sealed:tok-400", 0)
	store := &fakeStore{due: []*fakeRow{revoked, refused}}
	handler, err := NewHandler(store, fakeDecryptor{}, func(ctx context.Context, token string) error {
		return providerfoundation.RevokePagerDutyOAuthToken(ctx, server.Client(), config, token)
	}, true, slog.New(slog.NewTextHandler(io.Discard, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatal(err)
	}
	if !revoked.sent || revoked.attempts != 0 {
		t.Fatalf("already-revoked answer: sent=%v attempts=%d, want sent", revoked.sent, revoked.attempts)
	}
	if refused.sent || !reflect.DeepEqual(refused.failures, []string{ReasonRemoteRevokeFailed}) {
		t.Fatalf("4xx answer: sent=%v failures=%v, want refused", refused.sent, refused.failures)
	}
}

// A lost delete (PagerDuty accepted, the DELETE did not commit) sends the same
// revoke once more on the next run, which PagerDuty answers as for a revoked
// token; the row then ends deleted and nothing is named as exhausted.
func TestALostDeleteSendsTheRevokeOnceMoreAndEndsDeleted(t *testing.T) {
	row := dueRow("sealed:tok-lost-delete", 0)
	store := &fakeStore{due: []*fakeRow{row}, lostDeletes: 1}
	h := newHarness(t, store, true)
	first := h.handler.Work(context.Background(), execution(due))
	if first == nil || !strings.Contains(first.Error(), string(jobruntime.CategoryRetryable)) {
		t.Fatalf("first run = %v, want retryable", first)
	}
	if row.sent || row.attempts != 0 {
		t.Fatalf("after the lost delete: sent=%v attempts=%d, want pending at 0 attempts", row.sent, row.attempts)
	}
	if err := h.handler.Work(context.Background(), execution(due)); err != nil {
		t.Fatalf("second run: %v", err)
	}
	if !row.sent || len(h.revoker.calls) != 2 {
		t.Fatalf("sent=%v provider calls=%d, want the row deleted after exactly 2 calls", row.sent, len(h.revoker.calls))
	}
	if strings.Contains(h.logs.String(), `"level":"WARN"`) {
		t.Fatalf("a lost delete produced a WARN line: %s", h.logs.String())
	}
	if h.counter(t, "exhausted") != 0 || h.counter(t, "revoked") != 1 {
		t.Fatalf("counters exhausted=%d revoked=%d, want 0 and 1", h.counter(t, "exhausted"), h.counter(t, "revoked"))
	}
}

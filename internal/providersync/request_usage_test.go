package providersync

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"encoding/json"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

var errScriptedTransport = errors.New("scripted transport failure")

type scriptedReply struct {
	status  int
	headers http.Header
	body    string
	// err fails the call after the request was written: no response.
	err error
	// writeErr fails the write itself; the transport reports it on
	// WroteRequest and the call returns it.
	writeErr error
	// unwritten models a failure before any write (a refused dial): the
	// transport reports no write at all.
	unwritten bool
}

// scriptedDoer answers each call with the next scripted reply and, like the
// real transport, reports each write through the request's httptrace.
type scriptedDoer struct {
	mu      sync.Mutex
	replies []scriptedReply
	calls   int
}

func (doer *scriptedDoer) Do(request *http.Request) (*http.Response, error) {
	doer.mu.Lock()
	defer doer.mu.Unlock()
	reply := scriptedReply{status: http.StatusOK}
	if doer.calls < len(doer.replies) {
		reply = doer.replies[doer.calls]
	}
	doer.calls++
	if request != nil && !reply.unwritten {
		if trace := httptrace.ContextClientTrace(request.Context()); trace != nil && trace.WroteRequest != nil {
			trace.WroteRequest(httptrace.WroteRequestInfo{Err: reply.writeErr})
		}
	}
	if reply.writeErr != nil {
		return nil, reply.writeErr
	}
	if reply.err != nil {
		return nil, reply.err
	}
	headers := reply.headers
	if headers == nil {
		headers = http.Header{}
	}
	body := reply.body
	if body == "" {
		body = `{}`
	}
	return &http.Response{
		StatusCode: reply.status, Header: headers, Request: request,
		Body: io.NopCloser(strings.NewReader(body)),
	}, nil
}

// spendingCompleteRouteHandler sends calls requests through the route client,
// ignoring their outcome, then returns its batch and err.
type spendingCompleteRouteHandler struct {
	calls int
	batch CompleteRouteBatch
	err   error
}

func (handler *spendingCompleteRouteHandler) Collect(
	ctx context.Context, _ Claim, _ providerfoundation.Credential,
	client *providerfoundation.HTTPClient, _ time.Time,
) (CompleteRouteBatch, error) {
	for index := 0; index < handler.calls; index++ {
		if response, err := client.Do(ctx, http.MethodGet, "/probe", nil); err == nil {
			_ = response.Body.Close()
		}
	}
	if handler.err != nil {
		return CompleteRouteBatch{}, handler.err
	}
	return handler.batch, nil
}

type spendingChunkRouteHandler struct {
	calls int
	err   error
}

func (handler *spendingChunkRouteHandler) Collect(
	context.Context, Claim, providerfoundation.Credential, *providerfoundation.HTTPClient, time.Time,
) (CompleteRouteBatch, error) {
	return CompleteRouteBatch{}, ErrInvalidConfiguration
}

func (handler *spendingChunkRouteHandler) CollectChunks(
	ctx context.Context, _ Claim, _ providerfoundation.Credential,
	client *providerfoundation.HTTPClient, _ time.Time, _ string,
	_ func(ChunkRouteEmission) error,
) error {
	for index := 0; index < handler.calls; index++ {
		if response, err := client.Do(ctx, http.MethodGet, "/probe", nil); err == nil {
			_ = response.Body.Close()
		}
	}
	return handler.err
}

type mismatchingCompleteRouteComparator struct{}

func (mismatchingCompleteRouteComparator) CompareCompleteRoute(
	context.Context, Claim, CompleteRouteBatch,
) (ShadowComparison, error) {
	return ShadowComparison{Match: false}, nil
}

// memoryRequestUsageSink stores rows; failures > 0 rejects that many writes,
// failures < 0 rejects every write.
type memoryRequestUsageSink struct {
	mu       sync.Mutex
	rows     []RequestUsageRow
	writes   int
	failures int
}

func (sink *memoryRequestUsageSink) WriteRequestUsage(_ context.Context, rows []RequestUsageRow) error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	sink.writes++
	if sink.failures != 0 {
		if sink.failures > 0 {
			sink.failures--
		}
		return errScriptedTransport
	}
	sink.rows = append(sink.rows, rows...)
	return nil
}

func (sink *memoryRequestUsageSink) stored() []RequestUsageRow {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return append([]RequestUsageRow(nil), sink.rows...)
}

func (sink *memoryRequestUsageSink) writeCount() int {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	return sink.writes
}

func sumRequestUsage(rows []RequestUsageRow) RequestUsageTotals {
	var totals RequestUsageTotals
	for _, row := range rows {
		totals.Requests += int(row.Requests)
		totals.Responses += int(row.Responses)
	}
	return totals
}

// syncBuffer is a log output safe for the writer goroutine and the test.
type syncBuffer struct {
	mu     sync.Mutex
	buffer bytes.Buffer
}

func (output *syncBuffer) Write(p []byte) (int, error) {
	output.mu.Lock()
	defer output.mu.Unlock()
	return output.buffer.Write(p)
}

func (output *syncBuffer) String() string {
	output.mu.Lock()
	defer output.mu.Unlock()
	return output.buffer.String()
}

// newTestRequestUsageWriter starts a writer over sink with a fast cadence and
// its own text log output.
func newTestRequestUsageWriter(t *testing.T, sink RequestUsageSink) (*RequestUsageWriter, *syncBuffer) {
	t.Helper()
	logs := &syncBuffer{}
	writer := NewRequestUsageWriter(sink, slog.New(slog.NewTextHandler(logs, nil)))
	writer.cadence = 5 * time.Millisecond
	writer.writeTimeout = 200 * time.Millisecond
	writer.Start()
	t.Cleanup(func() { writer.Close(time.Second) })
	return writer, logs
}

var usageLogLine = regexp.MustCompile(`msg=provider_sync\.request_usage_(flush_failed|unsent|buffer_dropped) .*?requests=(\d+) responses=(\d+) rest_requests=(\d+) rest_responses=\d+ graphql_requests=(\d+)`)

// namedByLastLine reads the requests named by the last writer line of kind.
func namedByLastLine(logs, kind string) (requests, responses int, found bool) {
	for _, match := range usageLogLine.FindAllStringSubmatch(logs, -1) {
		if match[1] != kind {
			continue
		}
		requests, _ = strconv.Atoi(match[2])
		responses, _ = strconv.Atoi(match[3])
		found = true
	}
	return requests, responses, found
}

func TestExecuteCountsEveryWireAttemptOnEveryExit(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	errCollect := errors.New("route decode failed")
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
	cases := []struct {
		name    string
		replies []scriptedReply
		calls   int
		adjust  func(*CompleteRouteExecutor)
		wantErr func(error) bool
		want    RequestUsageTotals
	}{
		{"success", []scriptedReply{{status: 200}, {status: 200}}, 2, nil,
			func(err error) bool { return err == nil }, RequestUsageTotals{Requests: 2, Responses: 2}},
		{"route error after answered, failed-read and failed-write attempts",
			[]scriptedReply{{status: 200}, {status: 503}, {err: errScriptedTransport}, {writeErr: errScriptedTransport}, {unwritten: true, err: errScriptedTransport}}, 5,
			func(executor *CompleteRouteExecutor) {
				executor.Handler = &spendingCompleteRouteHandler{calls: 5, err: errCollect}
			},
			func(err error) bool { return errors.Is(err, errCollect) }, RequestUsageTotals{Requests: 3, Responses: 2}},
		{"shadow mismatch", []scriptedReply{{status: 200}}, 1,
			func(executor *CompleteRouteExecutor) { executor.Comparator = mismatchingCompleteRouteComparator{} },
			func(err error) bool { return errors.Is(err, ErrShadowMismatch) }, RequestUsageTotals{Requests: 1, Responses: 1}},
		{"effect commit failure", []scriptedReply{{status: 200}}, 1,
			func(executor *CompleteRouteExecutor) {
				executor.Committer.Sink = &memoryEffectSink{failAfterWrite: "feature_flag", failure: errCollect}
			},
			func(err error) bool { return errors.Is(err, errCollect) }, RequestUsageTotals{Requests: 1, Responses: 1}},
		{"configuration rejected before any request", nil, 0,
			func(executor *CompleteRouteExecutor) { executor.Handler = nil },
			func(err error) bool { return errors.Is(err, ErrInvalidConfiguration) }, RequestUsageTotals{}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			claim, session := completeRouteSession(t, now, false)
			doer := &scriptedDoer{replies: tc.replies}
			sink := &memoryRequestUsageSink{}
			writer, _ := newTestRequestUsageWriter(t, sink)
			executor := completeRouteExecutor(now, &spendingCompleteRouteHandler{
				calls: tc.calls, batch: completeRouteFixture(t, claim),
			}, &memoryEffectLedger{}, &memoryEffectSink{})
			executor.Doer = doer
			executor.RequestUsage = writer
			if tc.adjust != nil {
				tc.adjust(&executor)
			}
			result, err := executor.Execute(context.Background(), session, descriptor)
			if !tc.wantErr(err) {
				t.Fatalf("err=%v", err)
			}
			if result.RequestUsage != tc.want {
				t.Fatalf("result usage=%+v want %+v", result.RequestUsage, tc.want)
			}
			writer.Close(time.Second)
			rows := sink.stored()
			if got := sumRequestUsage(rows); got != tc.want {
				t.Fatalf("stored usage=%+v want %+v rows=%+v", got, tc.want, rows)
			}
			for _, row := range rows {
				if row.OrgID != claim.OrgID || row.UnitID != claim.ID || row.SyncRunID != claim.SyncRunID ||
					row.Provider != claim.Provider || row.Dataset != claim.Dataset ||
					row.IntegrationID != claim.IntegrationID || row.ExecutionID != rows[0].ExecutionID ||
					row.ExecutionID == "" || row.Attempt != uint32(claim.Attempt) || row.Transport != requestTransportREST {
					t.Fatalf("row identity=%+v claim=%+v", row, claim)
				}
			}
		})
	}
}

func TestExecuteCountsAFailedChunkedStreamInvocation(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	_, session := completeRouteSessionFor(t, now, false, "github", "cicd")
	descriptor, ok := Descriptor("github", "cicd")
	if !ok || !descriptor.Chunked {
		t.Fatalf("descriptor=%+v ok=%t", descriptor, ok)
	}
	errStream := errors.New("job fetch exhausted")
	sink := &memoryRequestUsageSink{}
	writer, _ := newTestRequestUsageWriter(t, sink)
	executor := completeRouteExecutor(now, &spendingChunkRouteHandler{calls: 2, err: errStream},
		newChunkMemoryStore(), &memoryEffectSink{})
	executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: "github"}
	executor.Credentials.Decryptor = chunkedCredentialDecryptor{}
	executor.Doer = &scriptedDoer{replies: []scriptedReply{{status: 200}, {err: errScriptedTransport}}}
	executor.RequestUsage = writer

	result, err := executor.Execute(context.Background(), session, descriptor)
	if !errors.Is(err, errStream) {
		t.Fatalf("err=%v", err)
	}
	writer.Close(time.Second)
	want := RequestUsageTotals{Requests: 2, Responses: 1}
	if result.RequestUsage != want || sumRequestUsage(sink.stored()) != want {
		t.Fatalf("result=%+v stored=%+v", result.RequestUsage, sumRequestUsage(sink.stored()))
	}
}

func TestRequestCountingDoerCountsOnlyInsideAnExecutionAndOnce(t *testing.T) {
	t.Parallel()
	delegate := &scriptedDoer{}
	counted := CountRequests(CountRequests(delegate))
	oauth := CountRequests(&scriptedDoer{})
	request := func(ctx context.Context) *http.Request {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example.test/v1/items", nil)
		if err != nil {
			t.Fatal(err)
		}
		return request
	}
	if _, err := counted.Do(request(context.Background())); err != nil {
		t.Fatal(err)
	}
	ledger := NewRequestLedger()
	ctx := WithRequestLedger(context.Background(), ledger)
	if _, err := counted.Do(request(ctx)); err != nil {
		t.Fatal(err)
	}
	if _, err := oauth.Do(request(ctx)); err != nil {
		t.Fatal(err)
	}
	if got := ledger.Totals(); got != (RequestUsageTotals{Requests: 2, Responses: 2}) || delegate.calls != 2 {
		t.Fatalf("totals=%+v delegate calls=%d", got, delegate.calls)
	}
	if _, err := counted.Do(nil); err != nil || delegate.calls != 3 {
		t.Fatalf("nil request err=%v delegate calls=%d", err, delegate.calls)
	}
	if CountRequests(nil) != nil {
		t.Fatal("nil doer must stay nil")
	}
}

func TestRequestCountingDoerClassifiesTransportStatusAndRateLimit(t *testing.T) {
	t.Parallel()
	ledger := NewRequestLedger()
	ctx := WithRequestLedger(context.Background(), ledger)
	github := http.Header{}
	github.Set("X-RateLimit-Remaining", "4990")
	github.Set("X-RateLimit-Limit", "5000")
	github.Set("X-RateLimit-Used", "10")
	github.Set("X-RateLimit-Reset", "1760000000")
	github.Set("X-RateLimit-Resource", "graphql")
	gitlab := http.Header{}
	gitlab.Set("RateLimit-Remaining", "not-a-number")
	gitlab.Set("RateLimit-Limit", "600")
	gitlab.Set("Retry-After", "30")
	doer := CountRequests(&scriptedDoer{replies: []scriptedReply{
		{status: 200, headers: github}, // graphql
		{status: 429, headers: gitlab}, // graphql, trailing slash
		{status: 200},
		{status: 302},
		{status: 404},
		{status: 500},
		{err: errScriptedTransport},
		{status: 199},
	}})
	paths := []string{"/graphql", "/api/graphql/", "/v3/repos", "/v3/repos", "/v3/repos", "/v3/repos", "/v3/repos", "/graphqlx"}
	for _, path := range paths {
		request := (&http.Request{Method: http.MethodGet, URL: &url.URL{Scheme: "https", Host: "api.example.test", Path: path}, Header: http.Header{}}).WithContext(ctx)
		_, _ = doer.Do(request)
	}
	deltas := ledger.takeDeltas()
	graphql, rest := deltas[requestTransportGraphQL], deltas[requestTransportREST]
	if graphql.Requests != 2 || graphql.Responses != 2 || graphql.Status2xx != 1 || graphql.Status429 != 1 ||
		graphql.LatestStatus == nil || *graphql.LatestStatus != 429 {
		t.Fatalf("graphql=%+v", graphql)
	}
	if graphql.RateLimit.Remaining != nil || graphql.RateLimit.Limit == nil || *graphql.RateLimit.Limit != 600 ||
		graphql.RateLimit.RetryAfter != "30" || graphql.RateLimit.Resource != "" {
		t.Fatalf("graphql rate limit=%+v", graphql.RateLimit)
	}
	if rest.Requests != 6 || rest.Responses != 5 || rest.Status2xx != 1 || rest.Status3xx != 1 ||
		rest.Status4xx != 1 || rest.Status5xx != 1 || rest.Status429 != 0 ||
		rest.LatestStatus == nil || *rest.LatestStatus != 199 {
		t.Fatalf("rest=%+v", rest)
	}
	if again := ledger.takeDeltas(); len(again) != 0 {
		t.Fatalf("second take=%+v", again)
	}
	githubLimit, present := parseRequestRateLimit(github)
	if !present || githubLimit.Remaining == nil || *githubLimit.Remaining != 4990 || githubLimit.Used == nil ||
		*githubLimit.Used != 10 || githubLimit.Reset != "1760000000" || githubLimit.Resource != "graphql" {
		t.Fatalf("github rate limit=%+v present=%t", githubLimit, present)
	}
	if _, present := parseRequestRateLimit(http.Header{}); present {
		t.Fatal("no rate-limit headers must read as absent")
	}
}

func TestRequestCountingDoerCountsOnlyCleanWrites(t *testing.T) {
	t.Parallel()
	ledger := NewRequestLedger()
	ctx := WithRequestLedger(context.Background(), ledger)
	doer := CountRequests(&scriptedDoer{replies: []scriptedReply{
		{writeErr: errScriptedTransport},
		{unwritten: true, err: errScriptedTransport},
		{err: errScriptedTransport},
		{status: 200},
	}})
	for range 4 {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.example.test/v1", nil)
		if err != nil {
			t.Fatal(err)
		}
		_, _ = doer.Do(request)
	}
	if got := ledger.Totals(); got != (RequestUsageTotals{Requests: 2, Responses: 1}) {
		t.Fatalf("totals=%+v (a failed write and an unwritten call are not sends)", got)
	}
}

func TestRequestLedgerStatusClassBoundaries(t *testing.T) {
	t.Parallel()
	ledger := NewRequestLedger()
	limited := http.Header{}
	limited.Set("X-RateLimit-Remaining", "3")
	for _, status := range []int{0, 99, 100, 199, 200, 299, 300, 399, 400, 428, 429, 430, 499, 500, 599, 600, 1000} {
		headers := http.Header{}
		if status == 200 {
			headers = limited
		}
		ledger.attempt(requestTransportREST)
		ledger.observe(requestTransportREST, status, headers)
	}
	usage := ledger.takeDeltas()[requestTransportREST]
	if usage.Requests != 17 || usage.Responses != 17 || usage.Status2xx != 2 || usage.Status3xx != 2 ||
		usage.Status4xx != 4 || usage.Status429 != 1 || usage.Status5xx != 2 {
		t.Fatalf("usage=%+v", usage)
	}
	if usage.LatestStatus == nil || *usage.LatestStatus != 600 {
		t.Fatalf("latest status=%v (1000 is not an HTTP status and must not replace 600)", usage.LatestStatus)
	}
	if usage.RateLimit.Remaining == nil || *usage.RateLimit.Remaining != 3 {
		t.Fatalf("rate limit from the last response that carried one=%+v", usage.RateLimit)
	}
	fresh := NewRequestLedger()
	fresh.attempt(requestTransportREST)
	fresh.observe(requestTransportREST, 0, http.Header{})
	if got := fresh.takeDeltas()[requestTransportREST]; got.LatestStatus != nil {
		t.Fatalf("status 0 recorded as latest=%v", *got.LatestStatus)
	}
}

func TestExecuteWithoutARunnableRecorderStillReturns(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSession(t, now, false)
	sink := &memoryRequestUsageSink{}
	writer, _ := newTestRequestUsageWriter(t, sink)
	executor := completeRouteExecutor(now, &spendingCompleteRouteHandler{
		calls: 1, batch: completeRouteFixture(t, claim),
	}, &memoryEffectLedger{}, &memoryEffectSink{})
	executor.Doer = &scriptedDoer{}
	executor.RequestUsage = writer
	executor.HeartbeatInterval = 0
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
	result, err := executor.Execute(context.Background(), session, descriptor)
	writer.Close(time.Second)
	if !errors.Is(err, ErrInvalidConfiguration) || result.RequestUsage != (RequestUsageTotals{}) || len(sink.stored()) != 0 {
		t.Fatalf("err=%v usage=%+v stored=%d", err, result.RequestUsage, len(sink.stored()))
	}
	if _, err := executor.Execute(context.Background(), nil, descriptor); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil session err=%v", err)
	}
	var absent context.Context
	if _, err := executor.Execute(absent, session, descriptor); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("nil context err=%v", err)
	}
}

type fixedCredentialDecryptor struct{ plaintext []byte }

func (decryptor fixedCredentialDecryptor) Decrypt(secrets.Value) ([]byte, error) {
	return decryptor.plaintext, nil
}

// TestExecuteCountsTokenExchangesSentThroughTheRouteClient covers the calls
// an auth scheme sends on its own before the provider request: the GitHub App
// installation-token mint and the PagerDuty client-credentials exchange.
func TestExecuteCountsTokenExchangesSentThroughTheRouteClient(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	key, err := rsa.GenerateKey(rand.Reader, 1024)
	if err != nil {
		t.Fatal(err)
	}
	privateKey := string(pem.EncodeToMemory(&pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(key)}))
	githubApp, err := json.Marshal(map[string]string{"app_id": "1", "private_key": privateKey, "installation_id": "2"})
	if err != nil {
		t.Fatal(err)
	}
	cases := []struct {
		provider, dataset string
		plaintext         []byte
		token             string
	}{
		{"github", "repo-metadata", githubApp, `{"token":"installation-token","expires_at":"2030-01-01T00:00:00Z"}`},
		{"pagerduty", "services", []byte(`{"auth_mode":"client_credentials","client_id":"client-id","client_secret":"client-secret","subdomain":"acme","region":"us"}`),
			`{"access_token":"exchanged-token","expires_in":3600}`},
	}
	for _, tc := range cases {
		t.Run(tc.provider, func(t *testing.T) {
			t.Parallel()
			_, session := completeRouteSessionFor(t, now, false, tc.provider, tc.dataset)
			descriptor, ok := Descriptor(tc.provider, tc.dataset)
			if !ok || !descriptor.RouteReady || !descriptor.Plannable {
				t.Fatalf("descriptor=%+v ok=%t", descriptor, ok)
			}
			errRoute := errors.New("route stopped after one call")
			executor := completeRouteExecutor(now, &spendingCompleteRouteHandler{calls: 1, err: errRoute},
				&memoryEffectLedger{}, &memoryEffectSink{})
			executor.Credentials.Repository = &trackingCompleteRouteCredentialRepository{provider: tc.provider}
			executor.Credentials.Decryptor = fixedCredentialDecryptor{plaintext: tc.plaintext}
			executor.BudgetLimits = map[CostClass]int{session.Claim.CostClass: 1}
			doer := &scriptedDoer{replies: []scriptedReply{{status: 200, body: tc.token}, {status: 200}}}
			executor.Doer = doer
			sink := &memoryRequestUsageSink{}
			writer, _ := newTestRequestUsageWriter(t, sink)
			executor.RequestUsage = writer
			result, err := executor.Execute(context.Background(), session, descriptor)
			if !errors.Is(err, errRoute) {
				t.Fatalf("err=%v", err)
			}
			writer.Close(time.Second)
			want := RequestUsageTotals{Requests: 2, Responses: 2}
			if doer.calls != 2 || result.RequestUsage != want || sumRequestUsage(sink.stored()) != want {
				t.Fatalf("doer calls=%d result=%+v stored=%+v", doer.calls, result.RequestUsage, sumRequestUsage(sink.stored()))
			}
		})
	}
}

// refreshingCredentialHydrator sends one token refresh with the context the
// credential resolver hands it, the way the PagerDuty OAuth hydrator does.
type refreshingCredentialHydrator struct {
	doer providerfoundation.HTTPDoer
}

func (hydrator refreshingCredentialHydrator) Hydrate(
	ctx context.Context, _ providerfoundation.LeaseGuard, _ providerfoundation.TenantScope,
	credential providerfoundation.Credential,
) (providerfoundation.Credential, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, "https://identity.example.test/oauth/token", nil)
	if err != nil {
		return credential, err
	}
	response, err := hydrator.doer.Do(request)
	if err != nil {
		return credential, err
	}
	_ = response.Body.Close()
	return credential, nil
}

func TestExecuteCountsACredentialRefreshSentOnItsOwnDoer(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSession(t, now, false)
	refresh := &scriptedDoer{}
	executor := completeRouteExecutor(now, &spendingCompleteRouteHandler{
		calls: 1, batch: completeRouteFixture(t, claim),
	}, &memoryEffectLedger{}, &memoryEffectSink{})
	executor.Credentials.Hydrator = refreshingCredentialHydrator{doer: CountRequests(refresh)}
	route := &scriptedDoer{}
	executor.Doer = route
	sink := &memoryRequestUsageSink{}
	writer, _ := newTestRequestUsageWriter(t, sink)
	executor.RequestUsage = writer
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil {
		t.Fatal(err)
	}
	writer.Close(time.Second)
	want := RequestUsageTotals{Requests: 2, Responses: 2}
	if refresh.calls != 1 || route.calls != 1 || result.RequestUsage != want || sumRequestUsage(sink.stored()) != want {
		t.Fatalf("refresh=%d route=%d result=%+v stored=%+v", refresh.calls, route.calls, result.RequestUsage, sumRequestUsage(sink.stored()))
	}
}

// TestRecorderHandsOffOnEveryTickWithAdvancingFlushSeq drives the heartbeat
// ticks by hand: each tick with new spend hands one row per transport to the
// writer with the next flush_seq; a tick with no spend hands off nothing.
func TestRecorderHandsOffOnEveryTickWithAdvancingFlushSeq(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, _ := completeRouteSession(t, now, false)
	writer := NewRequestUsageWriter(&memoryRequestUsageSink{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	ledger := NewRequestLedger()
	recorder := newRequestUsageRecorder(writer, ledger, claim, func() time.Time { return now })
	ticks := make(chan time.Time)
	recorder.start(context.Background(), 0, ticks)
	tick := func() {
		select {
		case ticks <- time.Time{}:
		case <-time.After(5 * time.Second):
			t.Fatal("no recorder goroutine is reading the ticks")
		}
	}
	ledger.attempt(requestTransportREST)
	ledger.attempt(requestTransportGraphQL)
	tick()
	tick()
	// The third send completes only once the second, empty tick is handled.
	tick()
	ledger.attempt(requestTransportREST)
	recorder.finish()
	var batches [][]RequestUsageRow
	for len(writer.batches) > 0 {
		batches = append(batches, <-writer.batches)
	}
	if len(batches) != 2 || len(batches[0]) != 2 || batches[0][0].Transport != requestTransportGraphQL ||
		batches[0][0].FlushSeq != 0 || batches[0][1].FlushSeq != 0 ||
		len(batches[1]) != 1 || batches[1][0].FlushSeq != 1 || batches[1][0].Requests != 1 {
		t.Fatalf("batches=%+v", batches)
	}
}

// TestWriterResendsUnsentRowsUnchanged rejects the first write: the same rows
// go out again, byte-identical, then the next rows follow.
func TestWriterResendsUnsentRowsUnchanged(t *testing.T) {
	t.Parallel()
	sink := &memoryRequestUsageSink{failures: 1}
	writer, logs := newTestRequestUsageWriter(t, sink)
	first := []RequestUsageRow{{UnitID: "ABC-123", ExecutionID: "e1", Transport: requestTransportREST, Requests: 1}}
	writer.Offer(first)
	deadline := time.Now().Add(5 * time.Second)
	for sink.writeCount() < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	writer.Offer([]RequestUsageRow{{UnitID: "ABC-123", ExecutionID: "e1", FlushSeq: 1, Transport: requestTransportREST, Requests: 2}})
	writer.Close(time.Second)
	rows := sink.stored()
	if len(rows) != 2 || rows[0] != first[0] || rows[1].FlushSeq != 1 || sumRequestUsage(rows).Requests != 3 {
		t.Fatalf("rows=%+v", rows)
	}
	if requests, _, found := namedByLastLine(logs.String(), "flush_failed"); !found || requests != 1 ||
		!strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("failure line: %s", logs.String())
	}
}

// TestWriterKeepsItsBufferThroughAnOutageAndDropsBeyondItCounted keeps up to
// its buffer bound through an outage, drops the oldest beyond it with a named
// line and the counter, and stores the rest once the store is back.
func TestWriterKeepsItsBufferThroughAnOutageAndDropsBeyondItCounted(t *testing.T) {
	t.Parallel()
	sink := &memoryRequestUsageSink{failures: -1}
	logs := &syncBuffer{}
	writer := NewRequestUsageWriter(sink, slog.New(slog.NewTextHandler(logs, nil)))
	writer.bufferRows = 10
	writer.batchRows = 1000
	for index := range 12 {
		writer.buffer = append(writer.buffer, RequestUsageRow{ExecutionID: "e", FlushSeq: uint32(index), Transport: requestTransportREST, Requests: 1})
	}
	if writer.write(context.Background()) {
		t.Fatal("a rejected write reported success")
	}
	if len(writer.buffer) != 10 || writer.buffer[0].FlushSeq != 2 || writer.droppedRows.Load() != 2 ||
		writer.droppedRequests.Load() != 2 {
		t.Fatalf("buffer=%d first=%d dropped=%d", len(writer.buffer), writer.buffer[0].FlushSeq, writer.droppedRows.Load())
	}
	if requests, _, found := namedByLastLine(logs.String(), "buffer_dropped"); !found || requests != 2 {
		t.Fatalf("drop line: %s", logs.String())
	}
	sink.mu.Lock()
	sink.failures = 0
	sink.mu.Unlock()
	if !writer.write(context.Background()) || sumRequestUsage(sink.stored()).Requests != 10 || len(writer.buffer) != 0 {
		t.Fatalf("after recovery stored=%+v buffer=%d", sumRequestUsage(sink.stored()), len(writer.buffer))
	}
	if lines := strings.Count(logs.String(), "msg=provider_sync.request_usage_buffer_dropped"); lines != 1 {
		t.Fatalf("buffer_dropped lines=%d: %s", lines, logs.String())
	}
	if notice := writer.notices["provider_sync.request_usage_buffer_dropped"]; notice == nil || notice.suppressed != 0 {
		t.Fatalf("a write with no overflow counted as a suppressed drop: %+v", notice)
	}
	writes := sink.writeCount()
	if !writer.write(context.Background()) || sink.writeCount() != writes {
		t.Fatal("an empty buffer reached the store")
	}
}

// TestWriterWritesOnceItHoldsABatchWithoutWaitingForTheCadence writes as soon
// as it holds batchRows rows.
func TestWriterWritesOnceItHoldsABatchWithoutWaitingForTheCadence(t *testing.T) {
	t.Parallel()
	sink := &memoryRequestUsageSink{}
	writer := NewRequestUsageWriter(sink, slog.New(slog.NewTextHandler(io.Discard, nil)))
	writer.cadence = time.Hour
	writer.batchRows = 2
	writer.Start()
	t.Cleanup(func() { writer.Close(time.Second) })
	writer.Offer([]RequestUsageRow{{Transport: requestTransportREST, Requests: 1}, {Transport: requestTransportGraphQL, Requests: 1}})
	deadline := time.Now().Add(5 * time.Second)
	for len(sink.stored()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(sink.stored()) != 2 {
		t.Fatalf("stored=%d before the cadence", len(sink.stored()))
	}
}

// TestWriterCloseIsBoundedWhenItsLoggerIsStuck closes a writer whose store
// is down and whose log output never returns: Close still returns within its
// bound.
func TestWriterCloseIsBoundedWhenItsLoggerIsStuck(t *testing.T) {
	t.Parallel()
	output := blockingWriter{release: make(chan struct{})}
	t.Cleanup(func() { close(output.release) })
	writer := NewRequestUsageWriter(&memoryRequestUsageSink{failures: -1}, slog.New(slog.NewTextHandler(output, nil)))
	writer.writeTimeout = 10 * time.Millisecond
	writer.Start()
	writer.Offer([]RequestUsageRow{{Transport: requestTransportREST, Requests: 1}})
	started := time.Now()
	writer.Close(50 * time.Millisecond)
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("Close took %s", elapsed)
	}
}

// TestOfferDropsAndCountsWhenTheQueueIsFull never blocks: with nobody reading
// the queue, the hand-off past its capacity is dropped and counted, and the
// metric and the writer's next line report it.
func TestOfferDropsAndCountsWhenTheQueueIsFull(t *testing.T) {
	t.Parallel()
	logs := &syncBuffer{}
	writer := NewRequestUsageWriter(&memoryRequestUsageSink{}, slog.New(slog.NewTextHandler(logs, nil)))
	row := []RequestUsageRow{{Transport: requestTransportREST, Requests: 3}}
	started := time.Now()
	for range requestUsageQueueBatches + 5 {
		writer.Offer(row)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		t.Fatalf("offers took %s", elapsed)
	}
	if writer.droppedRows.Load() != 5 || writer.droppedRequests.Load() != 15 {
		t.Fatalf("dropped rows=%d requests=%d", writer.droppedRows.Load(), writer.droppedRequests.Load())
	}
	var metrics bytes.Buffer
	if err := writer.WritePrometheus(&metrics); err != nil ||
		!strings.Contains(metrics.String(), "dev_health_provider_request_usage_dropped_rows_total 5\n") ||
		!strings.Contains(metrics.String(), "dev_health_provider_request_usage_dropped_requests_total 15\n") {
		t.Fatalf("metrics=%s err=%v", metrics.String(), err)
	}
	writer.reportDropped(true)
	writer.reportDropped(true)
	if strings.Count(logs.String(), "msg=provider_sync.request_usage_dropped") != 1 ||
		!strings.Contains(logs.String(), "dropped_requests_total=15") {
		t.Fatalf("dropped line: %s", logs.String())
	}
	var nilWriter *RequestUsageWriter
	nilWriter.Offer(row)
}

// TestWriterCloseNamesWhatItCouldNotStore closes a writer whose store stays
// down: every buffered and queued row is named in the unsent line.
func TestWriterCloseNamesWhatItCouldNotStore(t *testing.T) {
	t.Parallel()
	sink := &memoryRequestUsageSink{failures: -1}
	writer, logs := newTestRequestUsageWriter(t, sink)
	writer.Offer([]RequestUsageRow{{Transport: requestTransportREST, Requests: 2, Responses: 1}, {Transport: requestTransportGraphQL, Requests: 3, Responses: 3}})
	writer.Close(time.Second)
	requests, responses, found := namedByLastLine(logs.String(), "unsent")
	if !found || requests != 5 || responses != 4 || !strings.Contains(logs.String(), "rest_requests=2 rest_responses=1 graphql_requests=3") || !strings.Contains(logs.String(), "level=ERROR msg=provider_sync.request_usage_unsent") {
		t.Fatalf("unsent line: %s", logs.String())
	}
	writer.Offer([]RequestUsageRow{{Transport: requestTransportREST, Requests: 1}})
}

type panickingRequestUsageSink struct{}

func (panickingRequestUsageSink) WriteRequestUsage(context.Context, []RequestUsageRow) error {
	panic("store driver bug")
}

// hangingRequestUsageSink never returns until released and ignores its
// context: a write that neither succeeds nor fails.
type hangingRequestUsageSink struct {
	release chan struct{}
	started chan struct{}
	once    sync.Once
}

func (sink *hangingRequestUsageSink) WriteRequestUsage(context.Context, []RequestUsageRow) error {
	sink.once.Do(func() { close(sink.started) })
	<-sink.release
	return nil
}

func TestWriterSurvivesAPanickingStoreAndAHungWrite(t *testing.T) {
	t.Parallel()
	writer, logs := newTestRequestUsageWriter(t, panickingRequestUsageSink{})
	writer.Offer([]RequestUsageRow{{Transport: requestTransportREST, Requests: 1}})
	writer.Close(time.Second)
	if !strings.Contains(logs.String(), "request usage write panicked: store driver bug") ||
		!strings.Contains(logs.String(), "msg=provider_sync.request_usage_unsent") {
		t.Fatalf("logs=%s", logs.String())
	}
	hung := &hangingRequestUsageSink{release: make(chan struct{}), started: make(chan struct{})}
	t.Cleanup(func() { close(hung.release) })
	hangingWriter, hangingLogs := newTestRequestUsageWriter(t, hung)
	hangingWriter.Offer([]RequestUsageRow{{Transport: requestTransportREST, Requests: 1}})
	<-hung.started
	started := time.Now()
	hangingWriter.Close(time.Second)
	if elapsed := time.Since(started); elapsed > 2*time.Second {
		t.Fatalf("close waited %s on a hung write", elapsed)
	}
	if !strings.Contains(hangingLogs.String(), errRequestUsageWriteTimeout.Error()) {
		t.Fatalf("logs=%s", hangingLogs.String())
	}
}

// panicOnceHandler panics on its first record, then logs normally.
type panicOnceHandler struct {
	slog.Handler
	once *sync.Once
}

func (handler panicOnceHandler) Handle(ctx context.Context, record slog.Record) error {
	panicked := false
	handler.once.Do(func() { panicked = true })
	if panicked {
		panic("handler bug")
	}
	return handler.Handler.Handle(ctx, record)
}

func TestWriterLoopContinuesAfterAPanic(t *testing.T) {
	t.Parallel()
	sink := &memoryRequestUsageSink{failures: 1}
	logs := &syncBuffer{}
	writer := NewRequestUsageWriter(sink, slog.New(panicOnceHandler{Handler: slog.NewTextHandler(logs, nil), once: &sync.Once{}}))
	writer.cadence = 5 * time.Millisecond
	writer.Start()
	writer.Offer([]RequestUsageRow{{Transport: requestTransportREST, Requests: 4}})
	deadline := time.Now().Add(5 * time.Second)
	for len(sink.stored()) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	writer.Close(time.Second)
	if got := sumRequestUsage(sink.stored()); got.Requests != 4 ||
		!strings.Contains(logs.String(), "msg=provider_sync.request_usage_writer_panicked") {
		t.Fatalf("stored=%+v logs=%s", got, logs.String())
	}
}

// TestManyExecutionsShareOneWriter runs executions concurrently through one
// writer: every call is stored once, under distinct execution keys.
func TestManyExecutionsShareOneWriter(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	sink := &memoryRequestUsageSink{}
	writer, _ := newTestRequestUsageWriter(t, sink)
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
	const executions = 16
	var wait sync.WaitGroup
	for range executions {
		wait.Add(1)
		go func() {
			defer wait.Done()
			claim, session := completeRouteSession(t, now, false)
			executor := completeRouteExecutor(now, &spendingCompleteRouteHandler{
				calls: 3, batch: completeRouteFixture(t, claim),
			}, &memoryEffectLedger{}, &memoryEffectSink{})
			executor.Doer = &scriptedDoer{}
			executor.RequestUsage = writer
			if _, err := executor.Execute(context.Background(), session, descriptor); err != nil {
				t.Error(err)
			}
		}()
	}
	wait.Wait()
	writer.Close(time.Second)
	rows := sink.stored()
	keys := map[string]struct{}{}
	for _, row := range rows {
		keys[fmt.Sprintf("%s/%s/%d", row.ExecutionID, row.Transport, row.FlushSeq)] = struct{}{}
	}
	if got := sumRequestUsage(rows); got.Requests != 3*executions || len(keys) != len(rows) || len(rows) != executions {
		t.Fatalf("stored=%+v rows=%d keys=%d", got, len(rows), len(keys))
	}
}

type panickingCompleteRouteHandler struct{ calls int }

func (handler panickingCompleteRouteHandler) Collect(
	ctx context.Context, _ Claim, _ providerfoundation.Credential,
	client *providerfoundation.HTTPClient, _ time.Time,
) (CompleteRouteBatch, error) {
	for range handler.calls {
		if response, err := client.Do(ctx, http.MethodGet, "/probe", nil); err == nil {
			_ = response.Body.Close()
		}
	}
	panic("normalizer bug after the provider answered")
}

// TestARecoveredRoutePanicStillHandsOffItsSpend runs a route that panics
// after two answered calls, recovered by the caller the way the job
// adapter's execute does: the two calls still reach the writer.
func TestARecoveredRoutePanicStillHandsOffItsSpend(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	_, session := completeRouteSession(t, now, false)
	sink := &memoryRequestUsageSink{}
	writer, _ := newTestRequestUsageWriter(t, sink)
	executor := completeRouteExecutor(now, panickingCompleteRouteHandler{calls: 2}, &memoryEffectLedger{}, &memoryEffectSink{})
	executor.Doer = &scriptedDoer{}
	executor.RequestUsage = writer
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
	recovered := func() (value any) {
		defer func() { value = recover() }()
		_, _ = executor.Execute(context.Background(), session, descriptor)
		return nil
	}()
	if recovered == nil {
		t.Fatal("the route panic did not reach the caller")
	}
	writer.Close(time.Second)
	if got := sumRequestUsage(sink.stored()); got != (RequestUsageTotals{Requests: 2, Responses: 2}) {
		t.Fatalf("stored=%+v", got)
	}
}

// TestShutdownDrainRetriesUntilItsBound closes a writer whose store rejects
// the first write and accepts the next: the drain retries and stores it all.
func TestShutdownDrainRetriesUntilItsBound(t *testing.T) {
	t.Parallel()
	sink := &memoryRequestUsageSink{failures: 1}
	logs := &syncBuffer{}
	writer := NewRequestUsageWriter(sink, slog.New(slog.NewTextHandler(logs, nil)))
	writer.cadence = time.Hour
	writer.retryPause = time.Millisecond
	writer.Start()
	writer.Offer([]RequestUsageRow{{Transport: requestTransportREST, Requests: 7}})
	writer.Close(time.Second)
	if got := sumRequestUsage(sink.stored()); got.Requests != 7 || sink.writeCount() != 2 ||
		strings.Contains(logs.String(), "request_usage_unsent") {
		t.Fatalf("stored=%+v writes=%d logs=%s", got, sink.writeCount(), logs.String())
	}
}

// TestRepeatingWriterNoticesAreRateLimited fails the store on every write:
// within one notice interval one failure line is logged; the next line,
// after the interval, reports the lines it suppressed and still names the
// whole unsent buffer.
func TestRepeatingWriterNoticesAreRateLimited(t *testing.T) {
	t.Parallel()
	sink := &memoryRequestUsageSink{failures: -1}
	logs := &syncBuffer{}
	writer := NewRequestUsageWriter(sink, slog.New(slog.NewTextHandler(logs, nil)))
	clock := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	writer.now = func() time.Time { return clock }
	for range 5 {
		writer.buffer = append(writer.buffer, RequestUsageRow{Transport: requestTransportREST, Requests: 1})
		writer.write(context.Background())
		clock = clock.Add(time.Second)
	}
	if lines := strings.Count(logs.String(), "msg=provider_sync.request_usage_flush_failed"); lines != 1 {
		t.Fatalf("failure lines within the interval=%d: %s", lines, logs.String())
	}
	clock = clock.Add(requestUsageNoticeInterval)
	writer.buffer = append(writer.buffer, RequestUsageRow{Transport: requestTransportREST, Requests: 1})
	writer.write(context.Background())
	if lines := strings.Count(logs.String(), "msg=provider_sync.request_usage_flush_failed"); lines != 2 ||
		!strings.Contains(logs.String(), "requests=6 responses=0 rest_requests=6") ||
		!strings.Contains(logs.String(), "suppressed=4") {
		t.Fatalf("second line: %s", logs.String())
	}
}

// lateWriteDoer answers at once and reports the request's write only when
// released, as HTTP/1 can when the response arrives before the final socket
// write returns.
type lateWriteDoer struct {
	release chan struct{}
	done    chan struct{}
}

func (doer lateWriteDoer) Do(request *http.Request) (*http.Response, error) {
	trace := httptrace.ContextClientTrace(request.Context())
	go func() {
		defer close(doer.done)
		<-doer.release
		if trace != nil && trace.WroteRequest != nil {
			trace.WroteRequest(httptrace.WroteRequestInfo{})
		}
	}()
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{}, Request: request,
		Body: io.NopCloser(strings.NewReader(`{}`))}, nil
}

// TestALateWriteReportIsHandedOffAfterTheExecutionReturned releases the
// transport's write report only after Execute returned: the send still
// reaches the store.
func TestALateWriteReportIsHandedOffAfterTheExecutionReturned(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	claim, session := completeRouteSession(t, now, false)
	sink := &memoryRequestUsageSink{}
	writer, _ := newTestRequestUsageWriter(t, sink)
	doer := lateWriteDoer{release: make(chan struct{}), done: make(chan struct{})}
	executor := completeRouteExecutor(now, &spendingCompleteRouteHandler{
		calls: 1, batch: completeRouteFixture(t, claim),
	}, &memoryEffectLedger{}, &memoryEffectSink{})
	executor.Doer = doer
	executor.RequestUsage = writer
	descriptor, _ := Descriptor("launchdarkly", "feature-flags")
	result, err := executor.Execute(context.Background(), session, descriptor)
	if err != nil || result.RequestUsage != (RequestUsageTotals{Requests: 0, Responses: 1}) {
		t.Fatalf("err=%v usage=%+v", err, result.RequestUsage)
	}
	close(doer.release)
	<-doer.done
	writer.Close(time.Second)
	if got := sumRequestUsage(sink.stored()); got != (RequestUsageTotals{Requests: 1, Responses: 1}) {
		t.Fatalf("stored=%+v", got)
	}
}

// TestTheQueueDropNoticeIsRateLimited drops hand-offs between writes: within
// one notice interval one drop line is logged; the final report at Close is
// logged regardless and names the cumulative totals.
func TestTheQueueDropNoticeIsRateLimited(t *testing.T) {
	t.Parallel()
	logs := &syncBuffer{}
	writer := NewRequestUsageWriter(&memoryRequestUsageSink{}, slog.New(slog.NewTextHandler(logs, nil)))
	clock := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	writer.now = func() time.Time { return clock }
	for range 3 {
		writer.countDropped([]RequestUsageRow{{Requests: 1}})
		writer.write(context.Background())
		clock = clock.Add(time.Second)
	}
	if lines := strings.Count(logs.String(), "msg=provider_sync.request_usage_dropped"); lines != 1 {
		t.Fatalf("drop lines within the interval=%d: %s", lines, logs.String())
	}
	writer.reportDropped(true)
	if lines := strings.Count(logs.String(), "msg=provider_sync.request_usage_dropped"); lines != 2 ||
		!strings.Contains(logs.String(), "dropped_requests_total=3") {
		t.Fatalf("final report: %s", logs.String())
	}
}

// TestLazySinkOpensOnTheFirstWriteAndRetriesAFailedOpen never opens at
// construction; a failed open fails that write and is tried again next time.
func TestLazySinkOpensOnTheFirstWriteAndRetriesAFailedOpen(t *testing.T) {
	t.Parallel()
	opens := 0
	sink := &LazyClickHouseRequestUsageSink{Open: func(context.Context) (driver.Conn, error) {
		opens++
		return nil, errScriptedTransport
	}}
	if err := sink.Close(); err != nil || opens != 0 {
		t.Fatalf("close before any write: err=%v opens=%d", err, opens)
	}
	if err := sink.WriteRequestUsage(context.Background(), nil); err != nil || opens != 0 {
		t.Fatalf("empty write opened: err=%v opens=%d", err, opens)
	}
	rows := []RequestUsageRow{{Requests: 1}}
	for range 2 {
		if err := sink.WriteRequestUsage(context.Background(), rows); !errors.Is(err, errScriptedTransport) {
			t.Fatalf("err=%v", err)
		}
	}
	if opens != 2 {
		t.Fatalf("opens=%d, want a retry per write", opens)
	}
	if err := (&LazyClickHouseRequestUsageSink{}).WriteRequestUsage(context.Background(), rows); !errors.Is(err, ErrInvalidConfiguration) {
		t.Fatalf("no opener: err=%v", err)
	}
}

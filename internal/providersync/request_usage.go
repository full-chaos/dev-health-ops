package providersync

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptrace"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/platform/logging"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

const (
	requestTransportREST    = "rest"
	requestTransportGraphQL = "graphql"

	// requestUsageWriteTimeout bounds every store write the writer makes,
	// whatever the store does with its context.
	requestUsageWriteTimeout = 5 * time.Second
	// requestUsageQueueBatches is how many execution hand-offs the writer's
	// channel holds before a hand-off is dropped and counted.
	requestUsageQueueBatches = 1024
	// requestUsageBufferRows is how many rows the writer keeps for a retry
	// while the store fails; beyond it the oldest are dropped and counted.
	requestUsageBufferRows = 4096
	// requestUsageBatchRows and requestUsageCadence decide when the writer
	// writes: at this many buffered rows, or this long after the last write.
	requestUsageBatchRows = 500
	requestUsageCadence   = time.Second
	// requestUsageRetryPause spaces the shutdown drain's retries.
	requestUsageRetryPause = 100 * time.Millisecond
	// requestUsageNoticeInterval is the least time between two lines of one
	// repeating writer notice (a failed write, a buffer overflow); the lines
	// between are counted and reported as suppressed on the next one.
	requestUsageNoticeInterval = 30 * time.Second
)

var errRequestUsageWriteTimeout = errors.New("request usage write exceeded its deadline")

// RequestUsageTotals is one execution's provider HTTP spend. Requests counts
// the sends the HTTP transport reports written without error (each retry and
// each re-send of the transport itself included, whether or not a response
// came back); Responses counts the Do calls that returned a response.
type RequestUsageTotals struct {
	Requests  int
	Responses int
}

type requestLedgerKey struct{}

// requestTracedKey marks a request an outer RequestCountingDoer already
// traces, so a nested one never counts the same write again.
type requestTracedKey struct{}

// WithRequestLedger makes every RequestCountingDoer call issued with the
// returned context (or a context derived from it) count against ledger.
func WithRequestLedger(ctx context.Context, ledger *RequestLedger) context.Context {
	return context.WithValue(ctx, requestLedgerKey{}, ledger)
}

func requestLedgerFrom(ctx context.Context) *RequestLedger {
	if ctx == nil {
		return nil
	}
	ledger, _ := ctx.Value(requestLedgerKey{}).(*RequestLedger)
	return ledger
}

// RequestCountingDoer counts, against the RequestLedger carried by the
// request's context, every write of the request that the HTTP transport
// reports through httptrace as written without error: one per send, the
// transport's own re-sends on a dead connection or an HTTP/2 replay included.
// A write reported with an error is not counted. A request
// whose context carries no ledger is sent uncounted: it was not made on behalf
// of a sync unit execution.
type RequestCountingDoer struct {
	Delegate providerfoundation.HTTPDoer
}

// CountRequests wraps doer in a RequestCountingDoer. Wrapping a doer that
// already counts is harmless: the inner one sees the request traced and adds
// nothing.
func CountRequests(doer providerfoundation.HTTPDoer) providerfoundation.HTTPDoer {
	if doer == nil {
		return nil
	}
	return RequestCountingDoer{Delegate: doer}
}

func (doer RequestCountingDoer) Do(request *http.Request) (*http.Response, error) {
	if request == nil {
		return doer.Delegate.Do(request)
	}
	ctx := request.Context()
	ledger := requestLedgerFrom(ctx)
	if ledger == nil || ctx.Value(requestTracedKey{}) != nil {
		return doer.Delegate.Do(request)
	}
	transport := requestTransport(request)
	trace := &httptrace.ClientTrace{
		WroteRequest: func(info httptrace.WroteRequestInfo) {
			if info.Err == nil {
				ledger.attempt(transport)
			}
		},
	}
	traced := request.WithContext(httptrace.WithClientTrace(
		context.WithValue(ctx, requestTracedKey{}, struct{}{}), trace,
	))
	response, err := doer.Delegate.Do(traced)
	if response != nil {
		ledger.observe(transport, response.StatusCode, response.Header)
	}
	return response, logging.TransportFailure(err)
}

func requestTransport(request *http.Request) string {
	if request.URL != nil && strings.HasSuffix(strings.TrimRight(request.URL.Path, "/"), "/graphql") {
		return requestTransportGraphQL
	}
	return requestTransportREST
}

type requestRateLimit struct {
	Remaining  *int64
	Limit      *int64
	Used       *int64
	Reset      string
	Resource   string
	RetryAfter string
}

type transportUsage struct {
	Requests     uint64
	Responses    uint64
	Status2xx    uint64
	Status3xx    uint64
	Status4xx    uint64
	Status429    uint64
	Status5xx    uint64
	LatestStatus *uint16
	RateLimit    requestRateLimit
}

// RequestLedger accumulates one execution's provider HTTP calls by transport.
// It is safe for concurrent use.
type RequestLedger struct {
	mu      sync.Mutex
	current map[string]*transportUsage
	flushed map[string]transportUsage
	// late, once set, runs after every send counted from then on: the
	// transport can report a write after the call that made it returned.
	late func()
}

func NewRequestLedger() *RequestLedger {
	return &RequestLedger{current: map[string]*transportUsage{}, flushed: map[string]transportUsage{}}
}

func (ledger *RequestLedger) usage(transport string) *transportUsage {
	usage := ledger.current[transport]
	if usage == nil {
		usage = &transportUsage{}
		ledger.current[transport] = usage
	}
	return usage
}

func (ledger *RequestLedger) attempt(transport string) {
	ledger.mu.Lock()
	ledger.usage(transport).Requests++
	late := ledger.late
	ledger.mu.Unlock()
	if late != nil {
		late()
	}
}

func (ledger *RequestLedger) onLate(late func()) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	ledger.late = late
}

func (ledger *RequestLedger) observe(transport string, status int, headers http.Header) {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	usage := ledger.usage(transport)
	usage.Responses++
	switch {
	case status == http.StatusTooManyRequests:
		usage.Status429++
	case status >= 200 && status < 300:
		usage.Status2xx++
	case status >= 300 && status < 400:
		usage.Status3xx++
	case status >= 400 && status < 500:
		usage.Status4xx++
	case status >= 500 && status < 600:
		usage.Status5xx++
	}
	if status >= 100 && status <= 999 {
		value := uint16(status)
		usage.LatestStatus = &value
	}
	if rateLimit, present := parseRequestRateLimit(headers); present {
		usage.RateLimit = rateLimit
	}
}

func parseRequestRateLimit(headers http.Header) (requestRateLimit, bool) {
	first := func(names ...string) string {
		for _, name := range names {
			if value := strings.TrimSpace(headers.Get(name)); value != "" {
				return value
			}
		}
		return ""
	}
	integer := func(raw string) *int64 {
		if raw == "" {
			return nil
		}
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return nil
		}
		return &value
	}
	remaining := first("X-RateLimit-Remaining", "RateLimit-Remaining")
	limit := first("X-RateLimit-Limit", "RateLimit-Limit")
	used := first("X-RateLimit-Used")
	reset := first("X-RateLimit-Reset", "RateLimit-Reset")
	resource := first("X-RateLimit-Resource")
	retryAfter := first("Retry-After")
	if remaining == "" && limit == "" && used == "" && reset == "" && resource == "" && retryAfter == "" {
		return requestRateLimit{}, false
	}
	return requestRateLimit{
		Remaining: integer(remaining), Limit: integer(limit), Used: integer(used),
		Reset: reset, Resource: resource, RetryAfter: retryAfter,
	}, true
}

// Totals is the execution's spend so far, across every transport.
func (ledger *RequestLedger) Totals() RequestUsageTotals {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	var totals RequestUsageTotals
	if len(ledger.current) == 0 {
		return totals
	}
	for _, usage := range ledger.current {
		totals.Requests += int(usage.Requests)
		totals.Responses += int(usage.Responses)
	}
	return totals
}

// takeDeltas returns, per transport in a stable order, the spend recorded
// since the previous call, and marks it taken.
func (ledger *RequestLedger) takeDeltas() map[string]transportUsage {
	ledger.mu.Lock()
	defer ledger.mu.Unlock()
	deltas := map[string]transportUsage{}
	if len(ledger.current) == 0 {
		return deltas
	}
	for transport, usage := range ledger.current {
		previous := ledger.flushed[transport]
		if usage.Requests == previous.Requests && usage.Responses == previous.Responses {
			continue
		}
		deltas[transport] = transportUsage{
			Requests:     usage.Requests - previous.Requests,
			Responses:    usage.Responses - previous.Responses,
			Status2xx:    usage.Status2xx - previous.Status2xx,
			Status3xx:    usage.Status3xx - previous.Status3xx,
			Status4xx:    usage.Status4xx - previous.Status4xx,
			Status429:    usage.Status429 - previous.Status429,
			Status5xx:    usage.Status5xx - previous.Status5xx,
			LatestStatus: usage.LatestStatus,
			RateLimit:    usage.RateLimit,
		}
		ledger.flushed[transport] = *usage
	}
	return deltas
}

// RequestUsageRow is one row of provider_request_usage: the spend of one
// execution on one transport since that execution's previous flush.
type RequestUsageRow struct {
	OrgID              string
	Provider           string
	Dataset            string
	IntegrationID      string
	SyncRunID          string
	UnitID             string
	ExecutionID        string
	Attempt            uint32
	FlushSeq           uint32
	Transport          string
	Requests           uint64
	Responses          uint64
	Status2xx          uint64
	Status3xx          uint64
	Status4xx          uint64
	Status429          uint64
	Status5xx          uint64
	LatestStatus       *uint16
	RateLimitRemaining *int64
	RateLimitLimit     *int64
	RateLimitUsed      *int64
	RateLimitReset     string
	RateLimitResource  string
	RetryAfter         string
	WindowStartedAt    time.Time
	RecordedAt         time.Time
}

// RequestUsageSink stores provider request spend rows.
type RequestUsageSink interface {
	WriteRequestUsage(ctx context.Context, rows []RequestUsageRow) error
}

const requestUsageInsert = `INSERT INTO provider_request_usage (
org_id, provider, dataset, integration_id, sync_run_id, unit_id, execution_id,
attempt, flush_seq, transport, requests, responses,
status_2xx, status_3xx, status_4xx, status_429, status_5xx, latest_status,
rate_limit_remaining, rate_limit_limit, rate_limit_used, rate_limit_reset,
rate_limit_resource, retry_after, window_started_at, recorded_at)`

// ClickHouseRequestUsageSink appends rows to provider_request_usage.
type ClickHouseRequestUsageSink struct {
	Conn driver.Conn
}

func (sink ClickHouseRequestUsageSink) WriteRequestUsage(ctx context.Context, rows []RequestUsageRow) error {
	if len(rows) == 0 {
		return nil
	}
	if sink.Conn == nil {
		return ErrInvalidConfiguration
	}
	batch, err := sink.Conn.PrepareBatch(ctx, requestUsageInsert)
	if err != nil {
		return err
	}
	defer func() { _ = batch.Abort() }()
	for _, row := range rows {
		if err := batch.Append(
			row.OrgID, row.Provider, row.Dataset, row.IntegrationID, row.SyncRunID,
			row.UnitID, row.ExecutionID, row.Attempt, row.FlushSeq, row.Transport,
			row.Requests, row.Responses, row.Status2xx, row.Status3xx, row.Status4xx,
			row.Status429, row.Status5xx, row.LatestStatus, row.RateLimitRemaining,
			row.RateLimitLimit, row.RateLimitUsed, row.RateLimitReset,
			row.RateLimitResource, row.RetryAfter, row.WindowStartedAt, row.RecordedAt,
		); err != nil {
			return err
		}
	}
	return batch.Send()
}

// LazyClickHouseRequestUsageSink opens its ClickHouse connection on the first
// write rather than at construction, so a store that cannot be reached never
// stops the worker from starting: the write fails, and the writer keeps the
// rows and retries. A failed open is retried on the next write.
type LazyClickHouseRequestUsageSink struct {
	Open func(context.Context) (driver.Conn, error)

	mu   sync.Mutex
	conn driver.Conn
}

func (sink *LazyClickHouseRequestUsageSink) WriteRequestUsage(ctx context.Context, rows []RequestUsageRow) error {
	if len(rows) == 0 {
		return nil
	}
	conn, err := sink.connection(ctx)
	if err != nil {
		return err
	}
	return ClickHouseRequestUsageSink{Conn: conn}.WriteRequestUsage(ctx, rows)
}

func (sink *LazyClickHouseRequestUsageSink) connection(ctx context.Context) (driver.Conn, error) {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.conn != nil {
		return sink.conn, nil
	}
	if sink.Open == nil {
		return nil, ErrInvalidConfiguration
	}
	conn, err := sink.Open(ctx)
	if err != nil {
		return nil, err
	}
	sink.conn = conn
	return conn, nil
}

// Close closes the connection if one was opened.
func (sink *LazyClickHouseRequestUsageSink) Close() error {
	sink.mu.Lock()
	defer sink.mu.Unlock()
	if sink.conn == nil {
		return nil
	}
	err := sink.conn.Close()
	sink.conn = nil
	return err
}

// RequestUsageWriter is the one process-level owner of every request-usage
// store write and log line. Executions hand rows to it with Offer, which
// never blocks: a full queue drops the rows and counts them. The writer
// batches, writes under its own deadline, keeps failed rows for a byte-
// identical re-send (the store collapses a duplicate on its sorting key), and
// logs through its own handler so it never holds a lock a unit's own logging
// waits on.
type RequestUsageWriter struct {
	sink           RequestUsageSink
	logger         *slog.Logger
	batches        chan []RequestUsageRow
	writeTimeout   time.Duration
	cadence        time.Duration
	batchRows      int
	bufferRows     int
	retryPause     time.Duration
	noticeInterval time.Duration
	now            func() time.Time
	notices        map[string]*requestUsageNotice
	drainTimeout   time.Duration

	droppedRows     atomic.Uint64
	droppedRequests atomic.Uint64

	buffer   []RequestUsageRow
	reported uint64

	startOnce sync.Once
	stopOnce  sync.Once
	stop      chan struct{}
	done      chan struct{}
}

// NewRequestUsageWriter builds a writer over sink. logger must be a handler
// instance of its own, not one a unit's code path also logs through.
func NewRequestUsageWriter(sink RequestUsageSink, logger *slog.Logger) *RequestUsageWriter {
	return &RequestUsageWriter{
		sink: sink, logger: logger,
		batches:      make(chan []RequestUsageRow, requestUsageQueueBatches),
		writeTimeout: requestUsageWriteTimeout, cadence: requestUsageCadence,
		batchRows: requestUsageBatchRows, bufferRows: requestUsageBufferRows,
		retryPause: requestUsageRetryPause, noticeInterval: requestUsageNoticeInterval,
		now: time.Now, notices: map[string]*requestUsageNotice{},
		stop: make(chan struct{}), done: make(chan struct{}),
	}
}

// Offer hands rows to the writer without blocking. When the queue is full,
// or the writer has stopped reading it, the rows are dropped and counted.
func (writer *RequestUsageWriter) Offer(rows []RequestUsageRow) {
	if writer == nil || len(rows) == 0 {
		return
	}
	select {
	case writer.batches <- rows:
	default:
		writer.countDropped(rows)
	}
}

func (writer *RequestUsageWriter) countDropped(rows []RequestUsageRow) {
	requests := uint64(0)
	for _, row := range rows {
		requests += row.Requests
	}
	writer.droppedRows.Add(uint64(len(rows)))
	writer.droppedRequests.Add(requests)
}

// Start runs the writer until Close.
func (writer *RequestUsageWriter) Start() {
	writer.startOnce.Do(func() { go writer.run() })
}

// Close stops the writer after one last drain that retries until timeout,
// naming any rows it could not store. It returns once the writer has stopped,
// or after timeout plus one write deadline if the writer is stuck.
func (writer *RequestUsageWriter) Close(timeout time.Duration) {
	writer.stopOnce.Do(func() {
		writer.drainTimeout = timeout
		close(writer.stop)
	})
	writer.Start()
	select {
	case <-writer.done:
	case <-time.After(timeout + writer.writeTimeout):
	}
}

func (writer *RequestUsageWriter) run() {
	defer close(writer.done)
	ticker := time.NewTicker(writer.cadence)
	defer ticker.Stop()
	for !writer.step(ticker.C) {
	}
}

// step handles one event; it reports true when the writer has stopped. A
// panic inside is logged and the loop continues with its buffer intact.
func (writer *RequestUsageWriter) step(ticks <-chan time.Time) (stopped bool) {
	defer func() {
		if recovered := recover(); recovered != nil {
			writer.logger.Error("provider_sync.request_usage_writer_panicked", "panic", fmt.Sprint(recovered))
			stopped = false
		}
	}()
	select {
	case rows := <-writer.batches:
		writer.buffer = append(writer.buffer, rows...)
		if len(writer.buffer) >= writer.batchRows {
			writer.write(context.Background())
		}
	case <-ticks:
		writer.write(context.Background())
	case <-writer.stop:
		writer.drain()
		return true
	}
	return false
}

func (writer *RequestUsageWriter) drain() {
	deadline := time.Now().Add(writer.drainTimeout)
	for {
		select {
		case rows := <-writer.batches:
			writer.buffer = append(writer.buffer, rows...)
			continue
		default:
		}
		break
	}
	for len(writer.buffer) > 0 && time.Now().Before(deadline) {
		if !writer.write(context.Background()) {
			time.Sleep(min(writer.retryPause, time.Until(deadline)))
		}
	}
	if len(writer.buffer) > 0 {
		writer.logRows(slog.LevelError, "provider_sync.request_usage_unsent", writer.buffer, nil)
	}
	writer.reportDropped(true)
}

// write stores the buffer and reports whether it was accepted. It waits at
// most the write deadline whatever the sink does; a write still running then
// is abandoned and its rows stay buffered.
func (writer *RequestUsageWriter) write(ctx context.Context) bool {
	writer.reportDropped(false)
	if overflow := len(writer.buffer) - writer.bufferRows; overflow > 0 {
		dropped := writer.buffer[:overflow]
		writer.countDropped(dropped)
		writer.notice(slog.LevelError, "provider_sync.request_usage_buffer_dropped", dropped, nil)
		writer.buffer = append([]RequestUsageRow(nil), writer.buffer[overflow:]...)
	}
	if len(writer.buffer) == 0 {
		return true
	}
	writeContext, cancel := context.WithTimeout(ctx, writer.writeTimeout)
	defer cancel()
	batch := append([]RequestUsageRow(nil), writer.buffer...)
	result := make(chan error, 1)
	go func() {
		defer func() {
			if recovered := recover(); recovered != nil {
				result <- fmt.Errorf("request usage write panicked: %v", recovered)
			}
		}()
		result <- writer.sink.WriteRequestUsage(writeContext, batch)
	}()
	var err error
	select {
	case err = <-result:
	case <-writeContext.Done():
		err = errRequestUsageWriteTimeout
	}
	if err != nil {
		writer.notice(slog.LevelWarn, "provider_sync.request_usage_flush_failed", writer.buffer, err)
		return false
	}
	writer.buffer = nil
	return true
}

// reportDropped logs the hand-offs dropped since the last report, at most
// once per notice interval unless final; the totals it names are cumulative,
// so a suppressed report loses nothing.
func (writer *RequestUsageWriter) reportDropped(final bool) {
	rows := writer.droppedRows.Load()
	if rows == writer.reported {
		return
	}
	suppressed, allowed := writer.allow("provider_sync.request_usage_dropped")
	if !allowed && !final {
		return
	}
	writer.logger.Error("provider_sync.request_usage_dropped",
		"dropped_rows_total", rows, "dropped_requests_total", writer.droppedRequests.Load(),
		"suppressed", suppressed)
	writer.reported = rows
}

type requestUsageNotice struct {
	last       time.Time
	suppressed int
}

// notice logs a repeating writer line at most once per notice interval; the
// lines skipped in between are reported as suppressed on the next one. A
// failed-write line names the whole unsent buffer, so the next one emitted
// still names every call that is not stored.
func (writer *RequestUsageWriter) notice(level slog.Level, message string, rows []RequestUsageRow, err error) {
	if suppressed, allowed := writer.allow(message); allowed {
		writer.logRows(level, message, rows, err, "suppressed", suppressed)
	}
}

// allow reports whether message may be logged now, and how many of its lines
// were suppressed since the last one; a refusal counts as one more.
func (writer *RequestUsageWriter) allow(message string) (int, bool) {
	state := writer.notices[message]
	if state == nil {
		state = &requestUsageNotice{}
		writer.notices[message] = state
	}
	now := writer.now()
	if now.Sub(state.last) < writer.noticeInterval {
		state.suppressed++
		return 0, false
	}
	suppressed := state.suppressed
	state.last, state.suppressed = now, 0
	return suppressed, true
}

// logRows names the spend in rows, in total and per transport.
func (writer *RequestUsageWriter) logRows(level slog.Level, message string, rows []RequestUsageRow, err error, extra ...any) {
	var requests, responses, restRequests, restResponses, graphqlRequests, graphqlResponses uint64
	executions := map[string]struct{}{}
	for _, row := range rows {
		requests += row.Requests
		responses += row.Responses
		executions[row.ExecutionID] = struct{}{}
		if row.Transport == requestTransportGraphQL {
			graphqlRequests += row.Requests
			graphqlResponses += row.Responses
		} else {
			restRequests += row.Requests
			restResponses += row.Responses
		}
	}
	attrs := []any{
		"rows", len(rows), "executions", len(executions),
		"requests", requests, "responses", responses,
		"rest_requests", restRequests, "rest_responses", restResponses,
		"graphql_requests", graphqlRequests, "graphql_responses", graphqlResponses,
	}
	attrs = append(attrs, extra...)
	if err != nil {
		attrs = append(attrs, "error", err.Error())
	}
	writer.logger.Log(context.Background(), level, message, attrs...)
}

// WritePrometheus exposes the dropped hand-offs.
func (writer *RequestUsageWriter) WritePrometheus(output io.Writer) error {
	_, err := fmt.Fprintf(output,
		"# HELP dev_health_provider_request_usage_dropped_rows_total Request-usage rows dropped before the store: a full hand-off queue or retry buffer.\n"+
			"# TYPE dev_health_provider_request_usage_dropped_rows_total counter\n"+
			"dev_health_provider_request_usage_dropped_rows_total %d\n"+
			"# HELP dev_health_provider_request_usage_dropped_requests_total Provider requests carried by the dropped request-usage rows.\n"+
			"# TYPE dev_health_provider_request_usage_dropped_requests_total counter\n"+
			"dev_health_provider_request_usage_dropped_requests_total %d\n",
		writer.droppedRows.Load(), writer.droppedRequests.Load())
	return err
}

// requestUsageRecorder turns one execution's ledger into rows on each
// heartbeat tick and once when the execution returns, and hands them to the
// writer. It does no I/O and never waits on the writer.
type requestUsageRecorder struct {
	writer      *RequestUsageWriter
	ledger      *RequestLedger
	claim       Claim
	executionID string
	startedAt   time.Time
	now         func() time.Time

	mu  sync.Mutex
	seq uint32

	stop chan struct{}
	done chan struct{}
}

func newRequestUsageRecorder(
	writer *RequestUsageWriter, ledger *RequestLedger, claim Claim, now func() time.Time,
) *requestUsageRecorder {
	return &requestUsageRecorder{
		writer: writer, ledger: ledger, claim: claim,
		executionID: uuid.NewString(), startedAt: now(), now: now,
	}
}

// start hands off on every tick: from ticks when it is set, otherwise from a
// ticker at interval.
func (recorder *requestUsageRecorder) start(ctx context.Context, interval time.Duration, ticks <-chan time.Time) {
	if ticks == nil && interval <= 0 {
		return
	}
	recorder.stop = make(chan struct{})
	recorder.done = make(chan struct{})
	go func() {
		defer close(recorder.done)
		if ticks == nil {
			ticker := time.NewTicker(interval)
			defer ticker.Stop()
			ticks = ticker.C
		}
		for {
			select {
			case <-recorder.stop:
				return
			case <-ctx.Done():
				return
			case <-ticks:
				recorder.handOff()
			}
		}
	}()
}

// finish stops the tick goroutine, whose work never blocks, and hands off
// what is left; a send the transport reports after this is handed off on its
// own.
func (recorder *requestUsageRecorder) finish() {
	if recorder.stop != nil {
		close(recorder.stop)
		<-recorder.done
	}
	recorder.ledger.onLate(recorder.handOff)
	recorder.handOff()
}

func (recorder *requestUsageRecorder) handOff() {
	recorder.mu.Lock()
	defer recorder.mu.Unlock()
	deltas := recorder.ledger.takeDeltas()
	if len(deltas) == 0 {
		return
	}
	transports := make([]string, 0, len(deltas))
	for transport := range deltas {
		transports = append(transports, transport)
	}
	sort.Strings(transports)
	recordedAt := recorder.now()
	rows := make([]RequestUsageRow, 0, len(transports))
	for _, transport := range transports {
		rows = append(rows, recorder.row(transport, deltas[transport], recordedAt))
	}
	recorder.seq++
	recorder.writer.Offer(rows)
}

func (recorder *requestUsageRecorder) row(transport string, usage transportUsage, recordedAt time.Time) RequestUsageRow {
	claim := recorder.claim
	attempt := uint32(0)
	if claim.Attempt > 0 {
		attempt = uint32(claim.Attempt)
	}
	return RequestUsageRow{
		OrgID: claim.OrgID, Provider: claim.Provider, Dataset: claim.Dataset,
		IntegrationID: claim.IntegrationID, SyncRunID: claim.SyncRunID, UnitID: claim.ID,
		ExecutionID: recorder.executionID, Attempt: attempt, FlushSeq: recorder.seq,
		Transport: transport, Requests: usage.Requests, Responses: usage.Responses,
		Status2xx: usage.Status2xx, Status3xx: usage.Status3xx, Status4xx: usage.Status4xx,
		Status429: usage.Status429, Status5xx: usage.Status5xx, LatestStatus: usage.LatestStatus,
		RateLimitRemaining: usage.RateLimit.Remaining, RateLimitLimit: usage.RateLimit.Limit,
		RateLimitUsed: usage.RateLimit.Used, RateLimitReset: usage.RateLimit.Reset,
		RateLimitResource: usage.RateLimit.Resource, RetryAfter: usage.RateLimit.RetryAfter,
		WindowStartedAt: recorder.startedAt, RecordedAt: recordedAt,
	}
}

// Execute runs one complete-route execution with every provider HTTP call it
// makes counted: the executor's Doer and any Doer wrapped by CountRequests
// that is called with the execution's context. The spend is returned on every
// exit and, when RequestUsage is set, handed to that writer as it accrues.
func (executor CompleteRouteExecutor) Execute(
	ctx context.Context,
	session *LeaseSession,
	descriptor CompleteRouteDescriptor,
) (CompleteRouteExecutionResult, error) {
	if ctx == nil || session == nil {
		return executor.executeRoute(ctx, session, descriptor)
	}
	ledger := NewRequestLedger()
	ctx = WithRequestLedger(ctx, ledger)
	executor.Doer = CountRequests(executor.Doer)
	if executor.RequestUsage != nil {
		recorder := newRequestUsageRecorder(executor.RequestUsage, ledger, session.Claim, executor.now)
		recorder.start(ctx, executor.HeartbeatInterval, executor.requestUsageTicks)
		// Deferred so a route panic that a caller recovers still hands off
		// the spend it made.
		defer recorder.finish()
	}
	result, err := executor.executeRoute(ctx, session, descriptor)
	result.RequestUsage = ledger.Totals()
	return result, err
}

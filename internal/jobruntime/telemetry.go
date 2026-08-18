package jobruntime

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const (
	maxMetricJobs               = 512
	maxMetricQueues             = 32
	maxMetricDomains            = 128
	maxMetricSyncLeases         = 128
	maxMetricStreams            = 64
	maxMetricBudgets            = 128
	maxMetricConcurrencyBudgets = 128
	poolDomain                  = "domain"
	poolQueueControl            = "queue_control"
	poolResultAcquired          = "acquired"
	poolResultTimeout           = "timeout"
	poolResultCancelled         = "cancelled"
	poolResultError             = "error"
)

// SyncLeaseResult is the bounded result vocabulary for expired sync-lease
// recovery. A failed compare-and-swap is not a recovery result and must not be
// observed.
type SyncLeaseResult string

const (
	SyncLeaseResultRetrying SyncLeaseResult = "retrying"
	SyncLeaseResultFailed   SyncLeaseResult = "failed"
)

// ReportRunLeaseResult is the bounded durable outcome of one expired report
// execution lease. Retrying means a new holder was fenced in; failed means the
// persisted reclaim ceiling terminalized the ReportRun.
type ReportRunLeaseResult string

const (
	ReportRunLeaseResultRetrying ReportRunLeaseResult = "retrying"
	ReportRunLeaseResultFailed   ReportRunLeaseResult = "failed"
)

var durationBuckets = []float64{
	0.01, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60, 300, 900, 3600,
}

// StreamLabels are pre-registered so stream telemetry cannot create an
// unbounded consumer-group label set.
type StreamLabels struct {
	Stream        string
	ConsumerGroup string
}

// BudgetLabels are pre-registered provider/cost-class dimensions. They never
// contain an organization, repository, or credential identity.
type BudgetLabels struct {
	Provider  string
	CostClass string
}

// ConcurrencyBudgetLabels identify one registry-owned durable budget without
// including organization identity. Tenant identity is a lease-key concern,
// never a metric label.
type ConcurrencyBudgetLabels struct {
	Kind  string
	Scope string
}

// SyncLeaseLabels are pre-registered provider/dataset-family dimensions for
// expired sync-lease recovery. They never contain tenant or repository data.
type SyncLeaseLabels struct {
	Provider      string
	DatasetFamily string
}

// MetricDimensions is the complete low-cardinality vocabulary accepted by a
// MetricsCollector. Jobs normally come from registry-selected queues;
// sync-lease, stream, and budget pairs come from static deployment
// configuration.
type MetricDimensions struct {
	Jobs               []JobLabels
	DomainTypes        []string
	SyncLeases         []SyncLeaseLabels
	Streams            []StreamLabels
	Budgets            []BudgetLabels
	ConcurrencyBudgets []ConcurrencyBudgetLabels
}

type queueLabels struct {
	Queue string
}

type jobResultLabels struct {
	Job    JobLabels
	Result Result
}

type attemptLabels struct {
	Kind     string
	Result   Result
	Category ErrorCategory
}

type cancellationLabels struct {
	Kind   string
	Reason ErrorCategory
}

type syncLeaseResultLabels struct {
	Lease  SyncLeaseLabels
	Result SyncLeaseResult
}

type concurrencyBudgetResultLabels struct {
	Budget ConcurrencyBudgetLabels
	Result string
}

type poolAcquireLabels struct {
	Pool   string
	Result string
}

type histogram struct {
	buckets []uint64
	count   uint64
	sum     float64
}

func newHistogram() *histogram {
	return &histogram{buckets: make([]uint64, len(durationBuckets)+1)}
}

func (histogram *histogram) observe(value float64) {
	index := len(durationBuckets)
	for candidate, upperBound := range durationBuckets {
		if value <= upperBound {
			index = candidate
			break
		}
	}
	histogram.buckets[index]++
	histogram.count++
	histogram.sum += value
}

// MetricsCollector is a dependency-free Prometheus collector and implements
// Observer. All mutable state is protected by one mutex so related running and
// attempt updates are observed consistently during exposition.
type MetricsCollector struct {
	mu sync.RWMutex

	allowedJobs               map[JobLabels]struct{}
	allowedQueues             map[queueLabels]struct{}
	allowedKinds              map[string]struct{}
	allowedDomains            map[string]struct{}
	allowedSyncLeases         map[SyncLeaseLabels]struct{}
	allowedStreams            map[StreamLabels]struct{}
	allowedBudgets            map[BudgetLabels]struct{}
	allowedConcurrencyBudgets map[ConcurrencyBudgetLabels]struct{}

	runtimeInfo *RuntimeInfo

	jobsAvailable         map[JobLabels]int64
	jobOldestAge          map[queueLabels]float64
	jobsRunning           map[JobLabels]int64
	executionSaturation   map[queueLabels]float64
	jobWait               map[JobLabels]*histogram
	jobDuration           map[jobResultLabels]*histogram
	jobAttempts           map[attemptLabels]uint64
	jobPanics             map[string]uint64
	cancellations         map[cancellationLabels]uint64
	domainMismatch        map[string]uint64
	syncLeaseExpired      map[syncLeaseResultLabels]uint64
	reportRunLeaseExpired map[ReportRunLeaseResult]uint64

	streamLag                 map[StreamLabels]int64
	streamPending             map[StreamLabels]int64
	streamOldestPending       map[StreamLabels]float64
	budgetWait                map[BudgetLabels]*histogram
	concurrencyBudgetCapacity map[ConcurrencyBudgetLabels]int64
	concurrencyBudgetLeased   map[ConcurrencyBudgetLabels]int64
	concurrencyBudgetWait     map[ConcurrencyBudgetLabels]*histogram
	concurrencyBudgetEvents   map[concurrencyBudgetResultLabels]uint64
	poolSaturation            map[string]float64
	poolAcquire               map[poolAcquireLabels]*histogram
}

var _ Observer = (*MetricsCollector)(nil)
var _ SyncLeaseObserver = (*MetricsCollector)(nil)
var _ ReportRunLeaseObserver = (*MetricsCollector)(nil)

func NewMetricsCollector(dimensions MetricDimensions) (*MetricsCollector, error) {
	if len(dimensions.Jobs) > maxMetricJobs {
		return nil, errors.New("metric job dimensions exceed bounds")
	}
	if len(dimensions.DomainTypes) > maxMetricDomains ||
		len(dimensions.SyncLeases) > maxMetricSyncLeases || len(dimensions.Streams) > maxMetricStreams ||
		len(dimensions.Budgets) > maxMetricBudgets || len(dimensions.ConcurrencyBudgets) > maxMetricConcurrencyBudgets {
		return nil, errors.New("metric dimensions exceed cardinality bounds")
	}

	collector := &MetricsCollector{
		allowedJobs:               make(map[JobLabels]struct{}, len(dimensions.Jobs)),
		allowedQueues:             make(map[queueLabels]struct{}),
		allowedKinds:              make(map[string]struct{}),
		allowedDomains:            make(map[string]struct{}, len(dimensions.DomainTypes)),
		allowedSyncLeases:         make(map[SyncLeaseLabels]struct{}, len(dimensions.SyncLeases)),
		allowedStreams:            make(map[StreamLabels]struct{}, len(dimensions.Streams)),
		allowedBudgets:            make(map[BudgetLabels]struct{}, len(dimensions.Budgets)),
		allowedConcurrencyBudgets: make(map[ConcurrencyBudgetLabels]struct{}, len(dimensions.ConcurrencyBudgets)),
		jobsAvailable:             make(map[JobLabels]int64, len(dimensions.Jobs)),
		jobOldestAge:              make(map[queueLabels]float64),
		jobsRunning:               make(map[JobLabels]int64, len(dimensions.Jobs)),
		executionSaturation:       make(map[queueLabels]float64),
		jobWait:                   make(map[JobLabels]*histogram, len(dimensions.Jobs)),
		jobDuration:               make(map[jobResultLabels]*histogram),
		jobAttempts:               make(map[attemptLabels]uint64),
		jobPanics:                 make(map[string]uint64),
		cancellations:             make(map[cancellationLabels]uint64),
		domainMismatch:            make(map[string]uint64, len(dimensions.DomainTypes)),
		syncLeaseExpired:          make(map[syncLeaseResultLabels]uint64, len(dimensions.SyncLeases)*len(syncLeaseResults())),
		reportRunLeaseExpired:     make(map[ReportRunLeaseResult]uint64, len(reportRunLeaseResults())),
		streamLag:                 make(map[StreamLabels]int64, len(dimensions.Streams)),
		streamPending:             make(map[StreamLabels]int64, len(dimensions.Streams)),
		streamOldestPending:       make(map[StreamLabels]float64, len(dimensions.Streams)),
		budgetWait:                make(map[BudgetLabels]*histogram, len(dimensions.Budgets)),
		concurrencyBudgetCapacity: make(map[ConcurrencyBudgetLabels]int64, len(dimensions.ConcurrencyBudgets)),
		concurrencyBudgetLeased:   make(map[ConcurrencyBudgetLabels]int64, len(dimensions.ConcurrencyBudgets)),
		concurrencyBudgetWait:     make(map[ConcurrencyBudgetLabels]*histogram, len(dimensions.ConcurrencyBudgets)),
		concurrencyBudgetEvents:   make(map[concurrencyBudgetResultLabels]uint64, len(dimensions.ConcurrencyBudgets)*2),
		poolSaturation:            map[string]float64{poolDomain: 0, poolQueueControl: 0},
		poolAcquire:               make(map[poolAcquireLabels]*histogram, 8),
	}
	for _, labels := range dimensions.Jobs {
		if err := validateJobLabels(labels); err != nil {
			return nil, err
		}
		if _, duplicate := collector.allowedJobs[labels]; duplicate {
			return nil, errors.New("duplicate metric job dimensions")
		}
		collector.allowedJobs[labels] = struct{}{}
		queue := queueLabels{Queue: labels.Queue}
		collector.allowedQueues[queue] = struct{}{}
		collector.allowedKinds[labels.Kind] = struct{}{}
		collector.executionSaturation[queue] = 0
		collector.jobsAvailable[labels] = 0
		collector.jobsRunning[labels] = 0
		collector.jobWait[labels] = newHistogram()
		collector.jobPanics[labels.Kind] = 0
	}
	for labels := range collector.allowedQueues {
		collector.jobOldestAge[labels] = 0
	}
	if len(collector.allowedQueues) > maxMetricQueues {
		return nil, errors.New("metric queue dimensions exceed bounds")
	}
	for _, domainType := range dimensions.DomainTypes {
		if !metricIdentifier(domainType, 64) {
			return nil, errors.New("invalid metric domain dimension")
		}
		if _, duplicate := collector.allowedDomains[domainType]; duplicate {
			return nil, errors.New("duplicate metric domain dimension")
		}
		collector.allowedDomains[domainType] = struct{}{}
		collector.domainMismatch[domainType] = 0
	}
	for _, labels := range dimensions.SyncLeases {
		if !metricIdentifier(labels.Provider, 64) || !metricIdentifier(labels.DatasetFamily, 96) {
			return nil, errors.New("invalid metric sync lease dimensions")
		}
		if _, duplicate := collector.allowedSyncLeases[labels]; duplicate {
			return nil, errors.New("duplicate metric sync lease dimensions")
		}
		collector.allowedSyncLeases[labels] = struct{}{}
		for _, result := range syncLeaseResults() {
			collector.syncLeaseExpired[syncLeaseResultLabels{Lease: labels, Result: result}] = 0
		}
	}
	for _, result := range reportRunLeaseResults() {
		collector.reportRunLeaseExpired[result] = 0
	}
	for _, labels := range dimensions.Streams {
		if !metricIdentifier(labels.Stream, 96) || !metricIdentifier(labels.ConsumerGroup, 96) {
			return nil, errors.New("invalid metric stream dimensions")
		}
		if _, duplicate := collector.allowedStreams[labels]; duplicate {
			return nil, errors.New("duplicate metric stream dimensions")
		}
		collector.allowedStreams[labels] = struct{}{}
		collector.streamLag[labels] = 0
		collector.streamPending[labels] = 0
		collector.streamOldestPending[labels] = 0
	}
	for _, labels := range dimensions.Budgets {
		if !metricIdentifier(labels.Provider, 64) || !metricIdentifier(labels.CostClass, 64) {
			return nil, errors.New("invalid metric budget dimensions")
		}
		if _, duplicate := collector.allowedBudgets[labels]; duplicate {
			return nil, errors.New("duplicate metric budget dimensions")
		}
		collector.allowedBudgets[labels] = struct{}{}
		collector.budgetWait[labels] = newHistogram()
	}
	for _, labels := range dimensions.ConcurrencyBudgets {
		if !metricIdentifier(labels.Kind, 96) ||
			(labels.Scope != "fleet" && labels.Scope != "organization") {
			return nil, errors.New("invalid concurrency budget dimensions")
		}
		if _, duplicate := collector.allowedConcurrencyBudgets[labels]; duplicate {
			return nil, errors.New("duplicate concurrency budget dimensions")
		}
		collector.allowedConcurrencyBudgets[labels] = struct{}{}
		collector.concurrencyBudgetCapacity[labels] = 0
		collector.concurrencyBudgetLeased[labels] = 0
		collector.concurrencyBudgetWait[labels] = newHistogram()
		for _, result := range []string{"expired", "recovered"} {
			collector.concurrencyBudgetEvents[concurrencyBudgetResultLabels{Budget: labels, Result: result}] = 0
		}
	}
	for _, pool := range []string{poolDomain, poolQueueControl} {
		for _, result := range poolAcquireResults() {
			collector.poolAcquire[poolAcquireLabels{Pool: pool, Result: result}] = newHistogram()
		}
	}
	return collector, nil
}

// DimensionsForQueues derives job and domain dimensions from the validated
// runtime registry. Sync-lease, stream, and budget pairs remain explicit
// inputs here since they come from static deployment configuration rather
// than the job registry.
func DimensionsForQueues(registry *Registry, queues []string, streams []StreamLabels, budgets []BudgetLabels, syncLeases []SyncLeaseLabels, concurrencyBudgets ...[]ConcurrencyBudgetLabels) (MetricDimensions, error) {
	if registry == nil {
		return MetricDimensions{}, errors.New("runtime registry is required")
	}
	descriptors, err := registry.SelectedQueues(queues)
	if err != nil {
		return MetricDimensions{}, err
	}
	if len(descriptors) == 0 {
		return MetricDimensions{}, errors.New("runtime queues have no registered jobs")
	}
	dimensions := MetricDimensions{
		Streams:    append([]StreamLabels(nil), streams...),
		Budgets:    append([]BudgetLabels(nil), budgets...),
		SyncLeases: append([]SyncLeaseLabels(nil), syncLeases...),
	}
	if len(concurrencyBudgets) > 0 {
		dimensions.ConcurrencyBudgets = append([]ConcurrencyBudgetLabels(nil), concurrencyBudgets[0]...)
	}
	domains := make(map[string]struct{})
	for _, descriptor := range descriptors {
		dimensions.Jobs = append(dimensions.Jobs, JobLabels{
			Queue: descriptor.Queue,
			Kind:  descriptor.Kind,
		})
		domains[descriptor.DomainLink] = struct{}{}
	}
	for domainType := range domains {
		dimensions.DomainTypes = append(dimensions.DomainTypes, domainType)
	}
	sort.Strings(dimensions.DomainTypes)
	return dimensions, nil
}

func (collector *MetricsCollector) RuntimeRegistered(_ context.Context, info RuntimeInfo) {
	if !boundedIdentity(info.Version, 128) || !boundedIdentity(info.Commit, 128) {
		return
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	copy := info
	collector.runtimeInfo = &copy
}

func (collector *MetricsCollector) JobStarted(_ context.Context, labels JobLabels) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedJobs[labels]; !ok {
		return
	}
	collector.jobsRunning[labels]++
}

func (collector *MetricsCollector) JobFinished(_ context.Context, labels JobLabels, result Result, category ErrorCategory, duration time.Duration) {
	if !validOutcome(result, category) || duration < 0 {
		return
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedJobs[labels]; !ok {
		return
	}
	if collector.jobsRunning[labels] > 0 {
		collector.jobsRunning[labels]--
	}
	durationLabels := jobResultLabels{Job: labels, Result: result}
	metric := collector.jobDuration[durationLabels]
	if metric == nil {
		metric = newHistogram()
		collector.jobDuration[durationLabels] = metric
	}
	metric.observe(duration.Seconds())
	collector.jobAttempts[attemptLabels{Kind: labels.Kind, Result: result, Category: category}]++
}

func (collector *MetricsCollector) JobPanicked(_ context.Context, labels JobLabels) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedJobs[labels]; ok {
		collector.jobPanics[labels.Kind]++
	}
}

func (collector *MetricsCollector) JobCancelled(_ context.Context, labels JobLabels, reason ErrorCategory) {
	if !validErrorCategory(reason) || reason == CategoryNone {
		return
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedJobs[labels]; ok {
		collector.cancellations[cancellationLabels{Kind: labels.Kind, Reason: reason}]++
	}
}

func (collector *MetricsCollector) DomainMismatch(_ context.Context, domainType string) {
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedDomains[domainType]; ok {
		collector.domainMismatch[domainType]++
	}
}

// BudgetWait satisfies Observer. Generic worker middleware has no safe
// provider/cost-class dimensions, so it cannot populate the provider-labelled
// metric. Concrete budget implementations call ObserveProviderBudgetWait.
func (*MetricsCollector) BudgetWait(context.Context, JobLabels, time.Duration, string) {}

func (collector *MetricsCollector) SetJobsAvailable(labels JobLabels, count int64) error {
	if count < 0 {
		return errors.New("available job count cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedJobs[labels]; !ok {
		return errors.New("job metric dimensions are not registered")
	}
	collector.jobsAvailable[labels] = count
	return nil
}

func (collector *MetricsCollector) SetJobOldestAge(queue string, age time.Duration) error {
	if age < 0 {
		return errors.New("oldest job age cannot be negative")
	}
	labels := queueLabels{Queue: queue}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedQueues[labels]; !ok {
		return errors.New("queue metric dimensions are not registered")
	}
	collector.jobOldestAge[labels] = age.Seconds()
	return nil
}

func (collector *MetricsCollector) ObserveJobWait(labels JobLabels, wait time.Duration) error {
	if wait < 0 {
		return errors.New("job wait cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	metric, ok := collector.jobWait[labels]
	if !ok {
		return errors.New("job metric dimensions are not registered")
	}
	metric.observe(wait.Seconds())
	return nil
}

// JobWait satisfies Observer. It is the generic middleware entry point the
// adapter calls on every execution; negative or unregistered observations are
// dropped rather than surfaced, exactly like the other Observer methods.
func (collector *MetricsCollector) JobWait(_ context.Context, labels JobLabels, wait time.Duration) {
	_ = collector.ObserveJobWait(labels, wait)
}

// ObserveSyncLeaseExpired records an expired-lease recovery that successfully
// changed durable state to RETRYING or FAILED. Callers must not record failed
// compare-and-swap attempts.
func (collector *MetricsCollector) ObserveSyncLeaseExpired(labels SyncLeaseLabels, result SyncLeaseResult) error {
	if !validSyncLeaseResult(result) {
		return errors.New("sync lease result is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedSyncLeases[labels]; !ok {
		return errors.New("sync lease metric dimensions are not registered")
	}
	collector.syncLeaseExpired[syncLeaseResultLabels{Lease: labels, Result: result}]++
	return nil
}

// ObserveReportRunLeaseExpired records only durable expired-lease outcomes.
// A worker that loses the row-lock race must not call this method.
func (collector *MetricsCollector) ObserveReportRunLeaseExpired(result ReportRunLeaseResult) error {
	if !validReportRunLeaseResult(result) {
		return errors.New("report run lease result is not registered")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	collector.reportRunLeaseExpired[result]++
	return nil
}

func (collector *MetricsCollector) SetExecutionSaturation(queue string, ratio float64) error {
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
		return errors.New("execution saturation must be between zero and one")
	}
	labels := queueLabels{Queue: queue}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedQueues[labels]; !ok {
		return errors.New("execution saturation queue is not registered")
	}
	collector.executionSaturation[labels] = ratio
	return nil
}

func (collector *MetricsCollector) SetStreamLag(labels StreamLabels, lag int64) error {
	if lag < 0 {
		return errors.New("stream lag cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedStreams[labels]; !ok {
		return errors.New("stream metric dimensions are not registered")
	}
	collector.streamLag[labels] = lag
	return nil
}

func (collector *MetricsCollector) SetStreamPending(labels StreamLabels, pending int64) error {
	if pending < 0 {
		return errors.New("stream pending count cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedStreams[labels]; !ok {
		return errors.New("stream metric dimensions are not registered")
	}
	collector.streamPending[labels] = pending
	return nil
}

func (collector *MetricsCollector) SetStreamOldestPending(labels StreamLabels, age time.Duration) error {
	if age < 0 {
		return errors.New("oldest pending age cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedStreams[labels]; !ok {
		return errors.New("stream metric dimensions are not registered")
	}
	collector.streamOldestPending[labels] = age.Seconds()
	return nil
}

func (collector *MetricsCollector) ObserveProviderBudgetWait(labels BudgetLabels, wait time.Duration) error {
	if wait < 0 {
		return errors.New("budget wait cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	metric, ok := collector.budgetWait[labels]
	if !ok {
		return errors.New("budget metric dimensions are not registered")
	}
	metric.observe(wait.Seconds())
	return nil
}

func (collector *MetricsCollector) SetConcurrencyBudgetCapacity(labels ConcurrencyBudgetLabels, capacity int) error {
	if capacity < 1 {
		return errors.New("concurrency budget capacity must be positive")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedConcurrencyBudgets[labels]; !ok {
		return errors.New("concurrency budget dimensions are not registered")
	}
	collector.concurrencyBudgetCapacity[labels] = int64(capacity)
	return nil
}

func (collector *MetricsCollector) SetConcurrencyBudgetLeased(labels ConcurrencyBudgetLabels, leased int) error {
	if leased < 0 {
		return errors.New("concurrency budget leased capacity cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.allowedConcurrencyBudgets[labels]; !ok {
		return errors.New("concurrency budget dimensions are not registered")
	}
	collector.concurrencyBudgetLeased[labels] = int64(leased)
	return nil
}

func (collector *MetricsCollector) ObserveConcurrencyBudgetWait(labels ConcurrencyBudgetLabels, wait time.Duration) error {
	if wait < 0 {
		return errors.New("concurrency budget wait cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	metric, ok := collector.concurrencyBudgetWait[labels]
	if !ok {
		return errors.New("concurrency budget dimensions are not registered")
	}
	metric.observe(wait.Seconds())
	return nil
}

func (collector *MetricsCollector) ObserveConcurrencyBudgetExpiry(labels ConcurrencyBudgetLabels, result string) error {
	if result != "expired" && result != "recovered" {
		return errors.New("invalid concurrency budget lease event")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	key := concurrencyBudgetResultLabels{Budget: labels, Result: result}
	if _, ok := collector.concurrencyBudgetEvents[key]; !ok {
		return errors.New("concurrency budget dimensions are not registered")
	}
	collector.concurrencyBudgetEvents[key]++
	return nil
}

func (collector *MetricsCollector) SetDatabasePoolSaturation(pool string, ratio float64) error {
	if math.IsNaN(ratio) || math.IsInf(ratio, 0) || ratio < 0 || ratio > 1 {
		return errors.New("database pool saturation must be between zero and one")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	if _, ok := collector.poolSaturation[pool]; !ok {
		return errors.New("database pool metric dimension is not registered")
	}
	collector.poolSaturation[pool] = ratio
	return nil
}

func (collector *MetricsCollector) ObserveDatabasePoolAcquire(pool, result string, duration time.Duration) error {
	if duration < 0 {
		return errors.New("database pool acquisition duration cannot be negative")
	}
	collector.mu.Lock()
	defer collector.mu.Unlock()
	metric, ok := collector.poolAcquire[poolAcquireLabels{Pool: pool, Result: result}]
	if !ok {
		return errors.New("database pool metric dimensions are not registered")
	}
	metric.observe(duration.Seconds())
	return nil
}

// PrometheusText returns deterministic Prometheus text exposition. It never
// includes timestamps, job IDs, organizations, payloads, or error strings.
func (collector *MetricsCollector) PrometheusText() string {
	collector.mu.RLock()
	defer collector.mu.RUnlock()
	var output strings.Builder
	collector.writeRuntime(&output)
	collector.writeJobs(&output)
	collector.writeSyncLeases(&output)
	collector.writeReportRunLeases(&output)
	collector.writeStreams(&output)
	collector.writeBudgets(&output)
	collector.writeConcurrencyBudgets(&output)
	collector.writePools(&output)
	return output.String()
}

// WritePrometheus writes one deterministic snapshot to output.
func (collector *MetricsCollector) WritePrometheus(output io.Writer) error {
	if output == nil {
		return errors.New("Prometheus output is required")
	}
	_, err := io.WriteString(output, collector.PrometheusText())
	return err
}

func (collector *MetricsCollector) writeRuntime(output *strings.Builder) {
	writeMetadata(output, "worker_runtime_info", "Build identity for this worker runtime.", "gauge")
	if collector.runtimeInfo != nil {
		writeFloatSample(output, "worker_runtime_info", []metricLabel{
			{"version", collector.runtimeInfo.Version},
			{"commit", collector.runtimeInfo.Commit},
		}, 1)
	}
}

func (collector *MetricsCollector) writeJobs(output *strings.Builder) {
	jobs := sortedJobs(collector.allowedJobs)
	writeMetadata(output, "worker_jobs_available", "Current jobs available to the worker by queue and kind.", "gauge")
	for _, labels := range jobs {
		writeIntSample(output, "worker_jobs_available", jobMetricLabels(labels), collector.jobsAvailable[labels])
	}

	queues := sortedQueues(collector.allowedQueues)
	writeMetadata(output, "worker_job_oldest_age_seconds", "Age of the oldest available job by queue.", "gauge")
	for _, labels := range queues {
		writeFloatSample(output, "worker_job_oldest_age_seconds", queueMetricLabels(labels), collector.jobOldestAge[labels])
	}

	writeMetadata(output, "worker_jobs_running", "Current jobs executing by queue and kind.", "gauge")
	for _, labels := range jobs {
		writeIntSample(output, "worker_jobs_running", jobMetricLabels(labels), collector.jobsRunning[labels])
	}

	writeMetadata(output, "worker_execution_saturation_ratio", "Fraction of configured worker execution capacity currently in use.", "gauge")
	for _, labels := range queues {
		writeFloatSample(output, "worker_execution_saturation_ratio", queueMetricLabels(labels), collector.executionSaturation[labels])
	}

	writeMetadata(output, "worker_job_wait_seconds", "Time from job availability to execution start.", "histogram")
	for _, labels := range jobs {
		writeHistogram(output, "worker_job_wait_seconds", jobMetricLabels(labels), collector.jobWait[labels])
	}

	writeMetadata(output, "worker_job_duration_seconds", "Job execution duration by safe result.", "histogram")
	durationKeys := make([]jobResultLabels, 0, len(collector.jobDuration))
	for labels := range collector.jobDuration {
		durationKeys = append(durationKeys, labels)
	}
	sort.Slice(durationKeys, func(left, right int) bool {
		if compareJobs(durationKeys[left].Job, durationKeys[right].Job) != 0 {
			return compareJobs(durationKeys[left].Job, durationKeys[right].Job) < 0
		}
		return durationKeys[left].Result < durationKeys[right].Result
	})
	for _, labels := range durationKeys {
		metricLabels := append(jobMetricLabels(labels.Job), metricLabel{"result", string(labels.Result)})
		writeHistogram(output, "worker_job_duration_seconds", metricLabels, collector.jobDuration[labels])
	}

	writeMetadata(output, "worker_job_attempts_total", "Completed worker execution attempts by kind and safe outcome.", "counter")
	attemptKeys := make([]attemptLabels, 0, len(collector.jobAttempts))
	for labels := range collector.jobAttempts {
		attemptKeys = append(attemptKeys, labels)
	}
	sort.Slice(attemptKeys, func(left, right int) bool {
		if attemptKeys[left].Kind != attemptKeys[right].Kind {
			return attemptKeys[left].Kind < attemptKeys[right].Kind
		}
		if attemptKeys[left].Result != attemptKeys[right].Result {
			return attemptKeys[left].Result < attemptKeys[right].Result
		}
		return attemptKeys[left].Category < attemptKeys[right].Category
	})
	for _, labels := range attemptKeys {
		writeUintSample(output, "worker_job_attempts_total", []metricLabel{
			{"kind", labels.Kind}, {"result", string(labels.Result)}, {"error_category", string(labels.Category)},
		}, collector.jobAttempts[labels])
	}

	writeMetadata(output, "worker_job_panics_total", "Recovered worker panics by kind.", "counter")
	for _, kind := range sortedStrings(collector.allowedKinds) {
		writeUintSample(output, "worker_job_panics_total", []metricLabel{{"kind", kind}}, collector.jobPanics[kind])
	}

	writeMetadata(output, "worker_job_cancellations_total", "Worker cancellations by kind and bounded reason.", "counter")
	cancellationKeys := make([]cancellationLabels, 0, len(collector.cancellations))
	for labels := range collector.cancellations {
		cancellationKeys = append(cancellationKeys, labels)
	}
	sort.Slice(cancellationKeys, func(left, right int) bool {
		if cancellationKeys[left].Kind != cancellationKeys[right].Kind {
			return cancellationKeys[left].Kind < cancellationKeys[right].Kind
		}
		return cancellationKeys[left].Reason < cancellationKeys[right].Reason
	})
	for _, labels := range cancellationKeys {
		writeUintSample(output, "worker_job_cancellations_total", []metricLabel{
			{"kind", labels.Kind}, {"reason", string(labels.Reason)},
		}, collector.cancellations[labels])
	}

	writeMetadata(output, "worker_domain_state_mismatch_total", "Domain precondition mismatches by bounded domain type.", "counter")
	for _, domainType := range sortedStrings(collector.allowedDomains) {
		writeUintSample(output, "worker_domain_state_mismatch_total", []metricLabel{{"domain_type", domainType}}, collector.domainMismatch[domainType])
	}
}

func (collector *MetricsCollector) writeStreams(output *strings.Builder) {
	streams := sortedStreams(collector.allowedStreams)
	writeMetadata(output, "worker_stream_lag", "Current stream consumer lag.", "gauge")
	for _, labels := range streams {
		writeIntSample(output, "worker_stream_lag", streamMetricLabels(labels), collector.streamLag[labels])
	}
	writeMetadata(output, "worker_stream_pending", "Current pending stream entries.", "gauge")
	for _, labels := range streams {
		writeIntSample(output, "worker_stream_pending", streamMetricLabels(labels), collector.streamPending[labels])
	}
	writeMetadata(output, "worker_stream_oldest_pending_seconds", "Age of the oldest pending stream entry.", "gauge")
	for _, labels := range streams {
		writeFloatSample(output, "worker_stream_oldest_pending_seconds", streamMetricLabels(labels), collector.streamOldestPending[labels])
	}
}

func (collector *MetricsCollector) writeSyncLeases(output *strings.Builder) {
	writeMetadata(output, "worker_sync_lease_expired_total", "Expired sync leases recovered by bounded provider, dataset family, and durable result.", "counter")
	for _, labels := range sortedSyncLeases(collector.allowedSyncLeases) {
		for _, result := range syncLeaseResults() {
			writeUintSample(output, "worker_sync_lease_expired_total", []metricLabel{
				{"provider", labels.Provider}, {"dataset_family", labels.DatasetFamily}, {"result", string(result)},
			}, collector.syncLeaseExpired[syncLeaseResultLabels{Lease: labels, Result: result}])
		}
	}
}

func (collector *MetricsCollector) writeReportRunLeases(output *strings.Builder) {
	writeMetadata(output, "worker_report_run_lease_expired_total", "Expired report execution leases by bounded durable result.", "counter")
	for _, result := range reportRunLeaseResults() {
		writeUintSample(output, "worker_report_run_lease_expired_total", []metricLabel{
			{"result", string(result)},
		}, collector.reportRunLeaseExpired[result])
	}
}

func (collector *MetricsCollector) writeBudgets(output *strings.Builder) {
	writeMetadata(output, "worker_budget_wait_seconds", "Time spent waiting for a provider cost budget.", "histogram")
	for _, labels := range sortedBudgets(collector.allowedBudgets) {
		writeHistogram(output, "worker_budget_wait_seconds", []metricLabel{
			{"provider", labels.Provider}, {"cost_class", labels.CostClass},
		}, collector.budgetWait[labels])
	}
}

func (collector *MetricsCollector) writeConcurrencyBudgets(output *strings.Builder) {
	labels := make([]ConcurrencyBudgetLabels, 0, len(collector.allowedConcurrencyBudgets))
	for value := range collector.allowedConcurrencyBudgets {
		labels = append(labels, value)
	}
	sort.Slice(labels, func(i, j int) bool {
		if labels[i].Kind == labels[j].Kind {
			return labels[i].Scope < labels[j].Scope
		}
		return labels[i].Kind < labels[j].Kind
	})
	writeMetadata(output, "worker_concurrency_budget_capacity", "Configured durable concurrency budget capacity.", "gauge")
	for _, labels := range labels {
		values := []metricLabel{{"kind", labels.Kind}, {"scope", labels.Scope}}
		writeIntSample(output, "worker_concurrency_budget_capacity", values, collector.concurrencyBudgetCapacity[labels])
	}
	writeMetadata(output, "worker_concurrency_budget_leased", "Currently leased durable concurrency capacity.", "gauge")
	for _, labels := range labels {
		values := []metricLabel{{"kind", labels.Kind}, {"scope", labels.Scope}}
		writeIntSample(output, "worker_concurrency_budget_leased", values, collector.concurrencyBudgetLeased[labels])
	}
	writeMetadata(output, "worker_concurrency_budget_wait_seconds", "Time spent waiting for durable concurrency capacity.", "histogram")
	for _, labels := range labels {
		writeHistogram(output, "worker_concurrency_budget_wait_seconds", []metricLabel{{"kind", labels.Kind}, {"scope", labels.Scope}}, collector.concurrencyBudgetWait[labels])
	}
	writeMetadata(output, "worker_concurrency_budget_lease_events_total", "Expired and recovered durable concurrency leases.", "counter")
	for _, labels := range labels {
		for _, result := range []string{"expired", "recovered"} {
			writeUintSample(output, "worker_concurrency_budget_lease_events_total", []metricLabel{{"kind", labels.Kind}, {"scope", labels.Scope}, {"result", result}}, collector.concurrencyBudgetEvents[concurrencyBudgetResultLabels{Budget: labels, Result: result}])
		}
	}
}

func (collector *MetricsCollector) writePools(output *strings.Builder) {
	writeMetadata(output, "worker_database_pool_saturation_ratio", "Fraction of configured database pool capacity currently acquired.", "gauge")
	for _, pool := range []string{poolDomain, poolQueueControl} {
		writeFloatSample(output, "worker_database_pool_saturation_ratio", []metricLabel{{"pool", pool}}, collector.poolSaturation[pool])
	}
	writeMetadata(output, "worker_database_pool_acquire_seconds", "Database pool acquisition latency by bounded result.", "histogram")
	for _, pool := range []string{poolDomain, poolQueueControl} {
		for _, result := range poolAcquireResults() {
			labels := poolAcquireLabels{Pool: pool, Result: result}
			writeHistogram(output, "worker_database_pool_acquire_seconds", []metricLabel{
				{"pool", pool}, {"result", result},
			}, collector.poolAcquire[labels])
		}
	}
}

type metricLabel struct {
	name  string
	value string
}

func writeMetadata(output *strings.Builder, name, help, metricType string) {
	fmt.Fprintf(output, "# HELP %s %s\n# TYPE %s %s\n", name, help, name, metricType)
}

func writeIntSample(output *strings.Builder, name string, labels []metricLabel, value int64) {
	writeSamplePrefix(output, name, labels)
	output.WriteString(strconv.FormatInt(value, 10))
	output.WriteByte('\n')
}

func writeUintSample(output *strings.Builder, name string, labels []metricLabel, value uint64) {
	writeSamplePrefix(output, name, labels)
	output.WriteString(strconv.FormatUint(value, 10))
	output.WriteByte('\n')
}

func writeFloatSample(output *strings.Builder, name string, labels []metricLabel, value float64) {
	writeSamplePrefix(output, name, labels)
	output.WriteString(formatMetricFloat(value))
	output.WriteByte('\n')
}

func writeHistogram(output *strings.Builder, name string, labels []metricLabel, metric *histogram) {
	cumulative := uint64(0)
	for index, bound := range durationBuckets {
		cumulative += metric.buckets[index]
		bucketLabels := append(append([]metricLabel(nil), labels...), metricLabel{"le", formatMetricFloat(bound)})
		writeUintSample(output, name+"_bucket", bucketLabels, cumulative)
	}
	cumulative += metric.buckets[len(durationBuckets)]
	infLabels := append(append([]metricLabel(nil), labels...), metricLabel{"le", "+Inf"})
	writeUintSample(output, name+"_bucket", infLabels, cumulative)
	writeFloatSample(output, name+"_sum", labels, metric.sum)
	writeUintSample(output, name+"_count", labels, metric.count)
}

func writeSamplePrefix(output *strings.Builder, name string, labels []metricLabel) {
	output.WriteString(name)
	if len(labels) > 0 {
		output.WriteByte('{')
		for index, label := range labels {
			if index > 0 {
				output.WriteByte(',')
			}
			output.WriteString(label.name)
			output.WriteString("=\"")
			output.WriteString(escapeMetricLabel(label.value))
			output.WriteByte('"')
		}
		output.WriteByte('}')
	}
	output.WriteByte(' ')
}

func escapeMetricLabel(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, "\n", `\n`)
	return strings.ReplaceAll(value, `"`, `\"`)
}

func formatMetricFloat(value float64) string {
	if value == 0 {
		return "0"
	}
	return strconv.FormatFloat(value, 'g', -1, 64)
}

func jobMetricLabels(labels JobLabels) []metricLabel {
	return []metricLabel{{"queue", labels.Queue}, {"kind", labels.Kind}}
}

func queueMetricLabels(labels queueLabels) []metricLabel {
	return []metricLabel{{"queue", labels.Queue}}
}

func streamMetricLabels(labels StreamLabels) []metricLabel {
	return []metricLabel{{"stream", labels.Stream}, {"consumer_group", labels.ConsumerGroup}}
}

func validateJobLabels(labels JobLabels) error {
	if !metricIdentifier(labels.Queue, 96) || !metricIdentifier(labels.Kind, 96) {
		return errors.New("invalid metric job dimensions")
	}
	return nil
}

func metricIdentifier(value string, maximum int) bool {
	if len(value) == 0 || len(value) > maximum {
		return false
	}
	for _, character := range value {
		if (character < 'a' || character > 'z') &&
			(character < 'A' || character > 'Z') &&
			(character < '0' || character > '9') &&
			character != '.' && character != '_' && character != '-' && character != ':' {
			return false
		}
	}
	return true
}

func validResult(result Result) bool {
	return result == ResultSuccess || result == ResultDuplicate || result == ResultRetry || result == ResultDiscard || result == ResultCancel
}

func validOutcome(result Result, category ErrorCategory) bool {
	if !validResult(result) || !validErrorCategory(category) {
		return false
	}
	if result == ResultSuccess || result == ResultDuplicate {
		return category == CategoryNone
	}
	return category != CategoryNone
}

func validErrorCategory(category ErrorCategory) bool {
	switch category {
	case CategoryNone, CategoryValidation, CategoryPanic, CategoryTimeout, CategoryCancelled,
		CategoryRetryable, CategoryPermanent, CategoryTerminalDomain, CategoryTenant,
		CategoryBudget, CategoryRateLimited, CategoryIdempotency:
		return true
	default:
		return false
	}
}

func validSyncLeaseResult(result SyncLeaseResult) bool {
	return result == SyncLeaseResultRetrying || result == SyncLeaseResultFailed
}

func syncLeaseResults() []SyncLeaseResult {
	return []SyncLeaseResult{SyncLeaseResultFailed, SyncLeaseResultRetrying}
}

func validReportRunLeaseResult(result ReportRunLeaseResult) bool {
	return result == ReportRunLeaseResultRetrying || result == ReportRunLeaseResultFailed
}

func reportRunLeaseResults() []ReportRunLeaseResult {
	return []ReportRunLeaseResult{ReportRunLeaseResultFailed, ReportRunLeaseResultRetrying}
}

func poolAcquireResults() []string {
	return []string{poolResultAcquired, poolResultCancelled, poolResultError, poolResultTimeout}
}

func sortedJobs(values map[JobLabels]struct{}) []JobLabels {
	result := make([]JobLabels, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool { return compareJobs(result[left], result[right]) < 0 })
	return result
}

func compareJobs(left, right JobLabels) int {
	if left.Queue != right.Queue {
		return strings.Compare(left.Queue, right.Queue)
	}
	return strings.Compare(left.Kind, right.Kind)
}

func sortedQueues(values map[queueLabels]struct{}) []queueLabels {
	result := make([]queueLabels, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool {
		return result[left].Queue < result[right].Queue
	})
	return result
}

func sortedStreams(values map[StreamLabels]struct{}) []StreamLabels {
	result := make([]StreamLabels, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Stream != result[right].Stream {
			return result[left].Stream < result[right].Stream
		}
		return result[left].ConsumerGroup < result[right].ConsumerGroup
	})
	return result
}

func sortedSyncLeases(values map[SyncLeaseLabels]struct{}) []SyncLeaseLabels {
	result := make([]SyncLeaseLabels, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Provider != result[right].Provider {
			return result[left].Provider < result[right].Provider
		}
		return result[left].DatasetFamily < result[right].DatasetFamily
	})
	return result
}

func sortedBudgets(values map[BudgetLabels]struct{}) []BudgetLabels {
	result := make([]BudgetLabels, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Slice(result, func(left, right int) bool {
		if result[left].Provider != result[right].Provider {
			return result[left].Provider < result[right].Provider
		}
		return result[left].CostClass < result[right].CostClass
	})
	return result
}

func sortedStrings(values map[string]struct{}) []string {
	result := make([]string, 0, len(values))
	for value := range values {
		result = append(result, value)
	}
	sort.Strings(result)
	return result
}

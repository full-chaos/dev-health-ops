// Package workersignals holds the worker-plane failure counters that were, until
// CHAOS-6920, visible only as log lines: the worker presence heartbeat failing, a
// dispatch pass giving up on an advisory lock inside its wait budget, and a provider
// sync lease renewal failing.
//
// A Prometheus counter that has never incremented is absent from a scrape, so a clean
// window looked exactly like "not exposed" (the rev 189 prod sampler found none of these
// series on go-sync at all). Every series here is therefore written on EVERY scrape,
// including the ones still at 0, from a closed vocabulary; nothing is labelled by a value
// that can grow (no run, unit, org, host or error text).
//
// One PROCESS-WIDE source, registered once per process (workerservice/dependencies.go): a
// metric name may carry only one HELP/TYPE declaration per scrape, and a worker process can
// run several component families at once, the same reason
// providerfoundation.SyncRunRollupBumpedMetricsSource is a singleton.
//
// The package is a leaf (standard library only) so the three packages that record into it,
// internal/jobruntime, internal/syncdispatchruntime and internal/providersync, can import it
// without a cycle.
package workersignals

import (
	"fmt"
	"io"
	"sync/atomic"
)

// Dispatch advisory-lock wait outcomes. "other" absorbs an unknown value so a caller cannot
// mint a series.
const (
	LockOutcomeAcquired = "acquired"
	LockOutcomeBusy     = "busy"
	outcomeOther        = "other"
)

// Lease renewal failure reasons: "error" is a database failure on the renewal statement,
// "lease_lost" is a statement that ran but no longer owned the unit's lease.
const (
	LeaseReasonError     = "error"
	LeaseReasonLeaseLost = "lease_lost"
	reasonOther          = "other"
)

var (
	lockOutcomes  = [...]string{LockOutcomeAcquired, LockOutcomeBusy, outcomeOther}
	leaseReasons  = [...]string{LeaseReasonError, LeaseReasonLeaseLost, reasonOther}
	heartbeatFail atomic.Uint64
	lockWait      [len(lockOutcomes)]atomic.Uint64
	leaseRenewal  [len(leaseReasons)]atomic.Uint64
)

func indexOf(values []string, value string) int {
	for index, candidate := range values {
		if candidate == value {
			return index
		}
	}
	return len(values) - 1 // the "other" slot, always last
}

// RecordHeartbeatFailed counts one failed worker presence heartbeat renewal.
func RecordHeartbeatFailed() { heartbeatFail.Add(1) }

// RecordDispatchLockWait counts one dispatch advisory-lock acquisition (all the locks a
// pass needs) by outcome: acquired inside the wait budget, or busy (the pass gave the
// connection back and was requeued).
func RecordDispatchLockWait(outcome string) {
	lockWait[indexOf(lockOutcomes[:], outcome)].Add(1)
}

// RecordLeaseRenewalFailed counts one failed provider sync lease renewal by reason.
func RecordLeaseRenewalFailed(reason string) {
	leaseRenewal[indexOf(leaseReasons[:], reason)].Add(1)
}

type source struct{}

// MetricsSource returns the process-wide source. It satisfies health.MetricsSource
// (WritePrometheus(io.Writer) error) structurally.
func MetricsSource() source { return source{} }

// WritePrometheus writes every series, at 0 when nothing has happened yet.
func (source) WritePrometheus(writer io.Writer) error {
	if _, err := fmt.Fprintf(writer,
		"# HELP worker_presence_heartbeat_failed_total Worker presence heartbeat renewals that failed (CHAOS-6920). A failing heartbeat means the domain pool could not serve a one-row update inside its deadline.\n"+
			"# TYPE worker_presence_heartbeat_failed_total counter\n"+
			"worker_presence_heartbeat_failed_total %d\n", heartbeatFail.Load()); err != nil {
		return err
	}
	if _, err := io.WriteString(writer,
		"# HELP dev_health_sync_dispatch_advisory_lock_wait_total Dispatch passes by advisory-lock outcome: acquired inside the wait budget, or busy (the pass returned its connection and was requeued) (CHAOS-6920).\n"+
			"# TYPE dev_health_sync_dispatch_advisory_lock_wait_total counter\n"); err != nil {
		return err
	}
	for index, outcome := range lockOutcomes {
		if _, err := fmt.Fprintf(writer, "dev_health_sync_dispatch_advisory_lock_wait_total{outcome=%q} %d\n", outcome, lockWait[index].Load()); err != nil {
			return err
		}
	}
	if _, err := io.WriteString(writer,
		"# HELP dev_health_provider_sync_lease_renewal_failed_total Provider sync lease renewals that failed, by reason: a database error on the renewal statement, or a statement that no longer owned the lease (CHAOS-6920).\n"+
			"# TYPE dev_health_provider_sync_lease_renewal_failed_total counter\n"); err != nil {
		return err
	}
	for index, reason := range leaseReasons {
		if _, err := fmt.Fprintf(writer, "dev_health_provider_sync_lease_renewal_failed_total{reason=%q} %d\n", reason, leaseRenewal[index].Load()); err != nil {
			return err
		}
	}
	return nil
}

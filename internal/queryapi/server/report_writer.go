package server

import (
	"log"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/joboutbox"
	"github.com/full-chaos/dev-health-ops/internal/jobruntime"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/reports"
	schedulersync "github.com/full-chaos/dev-health-ops/internal/scheduler/sync"
)

// jobContractRoot is where the dho image stages the job contracts, the same
// path the api and scheduler services load.
const jobContractRoot = "contracts/jobs/v1"

// newReportWriter builds the saved-report mutation writer over the service's
// Postgres pool. The cron evaluator is the scheduler's reviewed port of the
// Python helper, so a schedule this writes is due when the fixed scheduler
// computes it. A job-contract registry that cannot be loaded leaves
// triggerReport unavailable (it answers an error) without taking the other four
// mutations, or the read plane, down with it.
func newReportWriter(pgPool *pgxpool.Pool, contractRoot string) *reports.Writer {
	if pgPool == nil {
		return nil
	}
	writer := &reports.Writer{Pool: pgPool, NextOccurrence: schedulersync.NextOccurrence}
	registry, err := jobruntime.Load(contractRoot)
	if err != nil {
		log.Printf("query-api: triggerReport unavailable: the job contract registry did not load from %s: %v", contractRoot, err)
		return writer
	}
	producer, err := joboutbox.NewTransactionProducer(registry)
	if err != nil {
		log.Printf("query-api: triggerReport unavailable: outbox producer: %v", err)
		return writer
	}
	writer.Outbox = producer
	return writer
}

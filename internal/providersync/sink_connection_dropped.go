package providersync

import (
	"context"

	"github.com/full-chaos/dev-health-ops/internal/platform/connerr"
)

// SinkConnectionDroppedError is a chunk effect write whose ClickHouse
// connection dropped under the INSERT (CHAOS-14493). The effect stays Writing,
// the chunk checkpoint does not move, and the next attempt runs the effect's
// recovery (EffectCommitter.commitPrepared) before it writes again. Error()
// is the driver text unchanged and Unwrap keeps errors.Is on the driver error.
type SinkConnectionDroppedError struct {
	Destination string
	Err         error
}

func (err *SinkConnectionDroppedError) Error() string { return err.Err.Error() }

func (err *SinkConnectionDroppedError) Unwrap() error { return err.Err }

// sinkDropWrappableDestinations are the effect destinations whose dropped
// write may be named a dropped connection (and so get the registry ceiling of
// attempts, not the unclassified cap). The server can still apply a dropped
// INSERT after the client is gone; a readback that runs before that says
// "absent" and the rewrite then stores a second copy. That copy is harmless
// only on a ReplacingMergeTree, so each entry is a ReplacingMergeTree table
// (engine cited from internal/chmigrate/baseline/head.json, line of its CREATE).
// Every materialized view fed by these tables keeps an idempotent state
// (ReplacingMergeTree, argMaxIf, min). A destination not listed stays unwrapped:
// a new destination must be added here, or to sinkDropNotWrappedDestinations,
// on purpose (TestEveryRouteDestinationHasASinkDropDecision).
var sinkDropWrappableDestinations = map[string]struct{}{
	"ai_attribution":                          {}, // ReplacingMergeTree(computed_at): head.json:161
	"ci_acceptance_checks":                    {}, // ReplacingMergeTree(last_synced): head.json:231
	"ci_job_runs":                             {}, // ReplacingMergeTree(last_synced): head.json:236
	"ci_pipeline_runs":                        {}, // ReplacingMergeTree(last_synced): head.json:241
	"coverage_snapshots":                      {}, // ReplacingMergeTree(last_synced): head.json:261
	"deployments":                             {}, // ReplacingMergeTree(last_synced): head.json:281
	"feature_flag":                            {}, // ReplacingMergeTree(last_synced): head.json:296
	"feature_flag_link":                       {}, // ReplacingMergeTree(last_synced): head.json:306
	"git_blame":                               {}, // ReplacingMergeTree(last_synced): head.json:326
	"git_commit_stats":                        {}, // ReplacingMergeTree(last_synced): head.json:346
	"git_commits":                             {}, // ReplacingMergeTree(last_synced): head.json:351
	"git_files":                               {}, // ReplacingMergeTree(last_synced): head.json:356
	"git_pull_request_reviews":                {}, // ReplacingMergeTree(last_synced): head.json:361
	"git_pull_requests":                       {}, // ReplacingMergeTree(last_synced): head.json:366
	"github_blame_path_progress":              {}, // ReplacingMergeTree(attempted_at): head.json:371
	"operational_alerts":                      {}, // ReplacingMergeTree(ingest_revision): head.json:426
	"operational_escalation_policies":         {}, // ReplacingMergeTree(ingest_revision): head.json:431
	"operational_incident_notes":              {}, // ReplacingMergeTree(ingest_revision): head.json:436
	"operational_incident_timeline_events":    {}, // ReplacingMergeTree(ingest_revision): head.json:446
	"operational_incidents":                   {}, // ReplacingMergeTree(ingest_revision): head.json:451
	"operational_on_call_assignments":         {}, // ReplacingMergeTree(ingest_revision): head.json:456
	"operational_on_call_schedules":           {}, // ReplacingMergeTree(ingest_revision): head.json:461
	"operational_service_repository_mappings": {}, // ReplacingMergeTree(ingest_revision): head.json:466
	"operational_services":                    {}, // ReplacingMergeTree(ingest_revision): head.json:471
	"operational_teams":                       {}, // ReplacingMergeTree(ingest_revision): head.json:476
	"operational_users":                       {}, // ReplacingMergeTree(ingest_revision): head.json:481
	"project_membership_transitions":          {}, // ReplacingMergeTree(last_synced): head.json:496
	"projects":                                {}, // ReplacingMergeTree(updated_at): head.json:501
	"repos":                                   {}, // ReplacingMergeTree(last_synced): head.json:546
	"security_alerts":                         {}, // ReplacingMergeTree(last_synced): head.json:561
	"sprints":                                 {}, // ReplacingMergeTree(last_synced): head.json:566
	"test_case_results":                       {}, // ReplacingMergeTree(last_synced): head.json:626
	"test_suite_results":                      {}, // ReplacingMergeTree(last_synced): head.json:631
	"testops_run_report_completeness":         {}, // ReplacingMergeTree(verdict_version): chmigrate/sql/153_testops_run_report_completeness.sql:37
	"work_graph_edges":                        {}, // ReplacingMergeTree(last_synced): head.json:721
	"work_graph_pr_commit":                    {}, // ReplacingMergeTree(version_rank): head.json:731
	"work_item_dependencies":                  {}, // ReplacingMergeTree(last_synced): head.json:776
	"work_item_interactions":                  {}, // ReplacingMergeTree(last_synced): head.json:791
	"work_item_reopen_events":                 {}, // ReplacingMergeTree(last_synced): head.json:821
	"work_item_transitions":                   {}, // ReplacingMergeTree(last_synced): head.json:836
	"work_items":                              {}, // ReplacingMergeTree(last_synced): head.json:846
	"worklogs":                                {}, // ReplacingMergeTree(last_synced): head.json:896
}

// sinkDropNotWrappedDestinations are route destinations whose dropped write
// keeps the unclassified cap on purpose, with the reason.
var sinkDropNotWrappedDestinations = map[string]string{
	// MergeTree: head.json:301. A second copy of a dropped INSERT is a real
	// duplicate row here.
	"feature_flag_event": "plain MergeTree",
}

// sinkConnectionDropped names err a dropped connection when the work context
// is still live (a done context closes the connection itself, as retryConn
// rules), the effect is recoverable, and the destination is wrappable.
// Every other error is returned unchanged.
func sinkConnectionDropped(ctx context.Context, effect EffectLedgerEntry, batch EffectBatch, err error) error {
	if err == nil || ctx.Err() != nil || effect.Recovery == EffectRecoveryBlocked {
		return err
	}
	if _, wrappable := sinkDropWrappableDestinations[batch.Destination]; !wrappable {
		return err
	}
	if !connerr.IsDropped(err) {
		return err
	}
	return &SinkConnectionDroppedError{Destination: batch.Destination, Err: err}
}

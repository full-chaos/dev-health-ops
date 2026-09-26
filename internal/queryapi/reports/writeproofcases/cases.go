// Package writeproofcases holds the five saved-report write proofs (CHAOS-6098):
// createSavedReport, updateSavedReport, deleteSavedReport, cloneSavedReport and
// triggerReport, each as a writeproof.Case the `goapi prove-write` verb runs once
// in the per-environment Fixture Org.
//
// It is a data-only package on purpose: a case is a seed, a variables builder and
// the SQL that reads the effects, and both the live verb and the CI oracle
// (internal/queryapi/server, TestSavedReportWriteProofBaselinesVenueOracle) run
// the SAME cases through writeproof.Execute. The oracle runs each case once
// against the real Python resolver and once against the real Go path on two
// database copies, and requires both digests to equal the committed baseline
// (baselines.json). The live verb runs only the Go plane and compares with the
// same committed digest.
//
// What a case observes, and what it deliberately does not:
//
//   - Every row a case reads is addressed by org and RunTag. The RunTag is part of
//     every name the case writes or seeds ("wp-<run>-<role>"), so nothing depends
//     on a fixed id, one run can never read or delete another's data, and the
//     Normalizer's run masking makes the digest independent of the tag.
//   - Timestamps the mutation stamps are time.Time near now (masked); a schedule's
//     next_run_at is a function of the wall clock, so it is read as the wall-clock
//     time IN THE JOB'S ZONE ("MM-DD HH24:MI") plus "is in the future", which is
//     stable for a yearly cron and still pins cron, zone and DST handling.
//   - created_by is not read: it is the caller's identity, which differs between
//     the venue's user token and the deployed environment's principal. The venue
//     oracle (reports_mutations_venue_oracle_integration_test.go) compares it.
//   - payload_hash and trace_parent of the outbox row are not read either: the
//     first is a hash over a canonicalisation the two planes need not share, the
//     second is a per-request trace value. The oracle names both as unpinned.
package writeproofcases

import (
	"context"
	_ "embed"
	"encoding/json"
	"fmt"
	"os"
	"sort"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/goapiproof"
	"github.com/full-chaos/dev-health-ops/internal/goapiproof/writeproof"
)

//go:embed baselines.json
var baselinesJSON []byte

// UpdateEnv, when set to "1" by the oracle, writes baselines.json from the run
// instead of comparing with it.
const UpdateEnv = "UPDATE_SAVED_REPORT_WRITE_PROOF_BASELINES"

// Operations are the mutations these cases prove, in the order of Names.
var Operations = []string{"createSavedReport", "updateSavedReport", "deleteSavedReport", "cloneSavedReport", "triggerReport"}

func name(run writeproof.RunTag, role string) string { return "wp-" + string(run) + "-" + role }

// reportID is a report the case seeds. It is derived from the run, so it never
// collides across runs and needs no KeepIDs entry: the Normalizer numbers it by
// first appearance like any other generated id.
func reportID(run writeproof.RunTag, role string) string {
	return uuid.NewSHA1(uuid.NameSpaceOID, []byte("writeproof:"+string(run)+":"+role)).String()
}

func quote(text string) string {
	encoded, err := json.Marshal(text)
	if err != nil {
		panic(err)
	}
	return string(encoded)
}

const seedInsert = `INSERT INTO saved_reports (id, org_id, name, description, report_plan, is_template, parameters, is_active, last_run_status, created_by, created_at, updated_at)
VALUES ($1::uuid, $2, $3, $4, $5::json, false, $6::json, true, 'success', 'write-proof', '2026-01-02T03:04:05+00:00', '2026-01-02T03:04:05+00:00')`

// seeder builds one case's dataset: rows created by insertRows, removed by the
// shared teardown, which addresses everything by org and RunTag.
type seeder struct {
	insertRows func(ctx context.Context, db goapiproof.Querier, org string, run writeproof.RunTag) error
}

func (s seeder) Seed(ctx context.Context, db goapiproof.Querier, org string, run writeproof.RunTag) error {
	if s.insertRows == nil {
		return nil
	}
	return s.insertRows(ctx, db, org, run)
}

// Teardown removes what the case seeded and what the mutation persisted:
// outbox rows of the runs it created, then the reports (their runs go with them
// by cascade), then its jobs.
func (seeder) Teardown(ctx context.Context, db goapiproof.Querier, org string, run writeproof.RunTag) error {
	for _, statement := range []string{
		`DELETE FROM worker_job_outbox o USING report_runs r JOIN saved_reports s ON s.id = r.report_id
		 WHERE s.org_id = $1 AND s.name LIKE 'wp-' || $2 || '-%' AND o.dedupe_key = 'report.run:' || r.id::text`,
		`DELETE FROM saved_reports WHERE org_id = $1 AND name LIKE 'wp-' || $2 || '-%'`,
		`DELETE FROM scheduled_jobs WHERE org_id = $1 AND name LIKE 'report:wp-' || $2 || '-%'`,
	} {
		if _, err := db.Exec(ctx, statement, org, string(run)); err != nil {
			return fmt.Errorf("teardown: %w", err)
		}
	}
	return nil
}

func insertReport(ctx context.Context, db goapiproof.Querier, org string, run writeproof.RunTag, role, plan, parameters string) error {
	_, err := db.Exec(ctx, seedInsert, reportID(run, role), org, name(run, role), "seeded for "+role, plan, parameters)
	return err
}

// The tables. Every statement takes $1 = org, $2 = run tag and orders by a name.
const (
	savedReportsSQL = `SELECT r.name, r.description, r.report_plan::text AS report_plan, r.is_template,
  (SELECT s.name FROM saved_reports s WHERE s.id = r.template_source_id) AS template_source,
  r.parameters::text AS parameters,
  (SELECT j.name FROM scheduled_jobs j WHERE j.id = r.schedule_id) AS schedule_job,
  r.is_active, r.last_run_at, r.last_run_status, r.created_at, r.updated_at
FROM saved_reports r WHERE r.org_id = $1 AND r.name LIKE 'wp-' || $2 || '-%' ORDER BY r.name`

	scheduledJobsSQL = `SELECT j.name, j.job_type, j.provider, j.schedule_cron, j.timezone,
  regexp_replace(j.job_config::text, '[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}', '<report>') AS job_config,
  j.status, j.is_running, j.run_count, j.failure_count,
  to_char(j.next_run_at AT TIME ZONE j.timezone, 'MM-DD HH24:MI') AS next_run_local,
  j.next_run_at > now() AS next_run_in_future, j.created_at, j.updated_at
FROM scheduled_jobs j WHERE j.org_id = $1 AND j.name LIKE 'report:wp-' || $2 || '-%' ORDER BY j.name`

	reportRunsSQL = `SELECT s.name AS report, r.status, r.triggered_by, r.started_at, r.completed_at,
  r.provenance_records::text AS provenance_records, r.attempt_count, r.execution_reclaim_count,
  r.notification_status, r.scheduled_occurrence_id IS NULL AS no_occurrence, r.created_at
FROM report_runs r JOIN saved_reports s ON s.id = r.report_id
WHERE s.org_id = $1 AND s.name LIKE 'wp-' || $2 || '-%' ORDER BY s.name, r.triggered_by`

	outboxSQL = `SELECT o.job_kind, o.contract_version, o.queue, o.priority, o.max_attempts, o.status, o.attempt_count,
  o.dedupe_key, o.args::text AS args, o.prerequisite_completion_key IS NULL AS no_prerequisite
FROM worker_job_outbox o
WHERE o.dedupe_key IN (SELECT 'report.run:' || r.id::text FROM report_runs r JOIN saved_reports s ON s.id = r.report_id
                        WHERE s.org_id = $1 AND s.name LIKE 'wp-' || $2 || '-%')
ORDER BY o.args::text`

	// deleteObservedSQL is ONE row of counts: a delete leaves nothing to read, and
	// a proof that observes no row proves nothing (Execute refuses it), so what
	// the delete did is read as what is left of the target, of its runs and of the
	// untouched sibling.
	deleteObservedSQL = `SELECT
  (SELECT count(*) FROM saved_reports WHERE org_id = $1 AND name = 'wp-' || $2 || '-delete') AS target_left,
  (SELECT count(*) FROM report_runs WHERE triggered_by = 'wp-' || $2) AS target_runs_left,
  (SELECT count(*) FROM saved_reports WHERE org_id = $1 AND name = 'wp-' || $2 || '-keep') AS sibling_left`
)

func tables(labels ...string) []writeproof.Table {
	all := map[string]string{
		"saved_reports":     savedReportsSQL,
		"scheduled_jobs":    scheduledJobsSQL,
		"report_runs":       reportRunsSQL,
		"worker_job_outbox": outboxSQL,
		"delete_observed":   deleteObservedSQL,
	}
	out := make([]writeproof.Table, 0, len(labels))
	for _, label := range labels {
		out = append(out, writeproof.Table{Label: label, SQL: all[label]})
	}
	return out
}

// Build returns the five cases with the given baseline digests (by case name).
// A missing digest is left blank, which Validate refuses: the caller decides
// whether that is a failure (the registry) or the thing being measured (the
// oracle passes a placeholder).
func Build(baselines map[string]string) []writeproof.Case {
	cases := []writeproof.Case{
		{
			Name: "saved-report-create", Operation: "createSavedReport",
			Variables: func(org string, run writeproof.RunTag) string {
				return `{"orgId": ` + quote(org) + `, "input": {"name": ` + quote(name(run, "created")) +
					`, "description": "created by the write proof", "reportPlan": {"b": 1, "a": [1, 2.5]}, "parameters": {"z": null, "y": "é"}, "scheduleCron": "30 3 15 6 *", "scheduleTimezone": "America/New_York"}}`
			},
			Seeder: seeder{},
			Tables: tables("saved_reports", "scheduled_jobs"),
		},
		{
			Name: "saved-report-update", Operation: "updateSavedReport",
			Variables: func(org string, run writeproof.RunTag) string {
				return `{"orgId": ` + quote(org) + `, "reportId": ` + quote(reportID(run, "update")) + `, "input": {"name": ` + quote(name(run, "updated")) +
					`, "description": "updated by the write proof", "reportPlan": {"y": [2, 1], "x": null}, "isActive": false, "scheduleCron": "0 6 * * 1", "scheduleTimezone": "Europe/London"}}`
			},
			Seeder: seeder{insertRows: func(ctx context.Context, db goapiproof.Querier, org string, run writeproof.RunTag) error {
				return insertReport(ctx, db, org, run, "update", `{"sections": [1, 2]}`, `{"team": "core"}`)
			}},
			Tables: tables("saved_reports", "scheduled_jobs"),
		},
		{
			Name: "saved-report-delete", Operation: "deleteSavedReport",
			Variables: func(org string, run writeproof.RunTag) string {
				return `{"orgId": ` + quote(org) + `, "reportId": ` + quote(reportID(run, "delete")) + `}`
			},
			Seeder: seeder{insertRows: func(ctx context.Context, db goapiproof.Querier, org string, run writeproof.RunTag) error {
				for _, role := range []string{"delete", "keep"} {
					if err := insertReport(ctx, db, org, run, role, `{}`, `{}`); err != nil {
						return err
					}
				}
				// A run of the report that goes: the delete must take it along.
				_, err := db.Exec(ctx, `INSERT INTO report_runs (id, report_id, status, triggered_by, provenance_records, attempt_count, execution_reclaim_count, notification_status, created_at)
VALUES ($1::uuid, $2::uuid, 'success', $3, '[]'::json, 0, 0, 'pending', '2026-01-02T03:04:05+00:00')`,
					reportID(run, "run"), reportID(run, "delete"), "wp-"+string(run))
				return err
			}},
			Tables: tables("delete_observed", "saved_reports"),
		},
		{
			Name: "saved-report-clone", Operation: "cloneSavedReport",
			Variables: func(org string, run writeproof.RunTag) string {
				return `{"orgId": ` + quote(org) + `, "input": {"sourceReportId": ` + quote(reportID(run, "source")) + `, "newName": ` + quote(name(run, "cloned")) +
					`, "parameterOverrides": {"b": 3, "n": {"x": 1}}}}`
			},
			Seeder: seeder{insertRows: func(ctx context.Context, db goapiproof.Querier, org string, run writeproof.RunTag) error {
				return insertReport(ctx, db, org, run, "source", `{"z": 1, "a": {"y": [1.5, "é"]}}`, `{"b": 1, "a": 2}`)
			}},
			Tables: tables("saved_reports"),
		},
		{
			Name: "saved-report-trigger", Operation: "triggerReport",
			Variables: func(org string, run writeproof.RunTag) string {
				return `{"orgId": ` + quote(org) + `, "reportId": ` + quote(reportID(run, "trigger")) + `}`
			},
			Seeder: seeder{insertRows: func(ctx context.Context, db goapiproof.Querier, org string, run writeproof.RunTag) error {
				return insertReport(ctx, db, org, run, "trigger", `{"sections": [1]}`, `{"team": "core"}`)
			}},
			Tables: tables("report_runs", "worker_job_outbox"),
		},
	}
	for i := range cases {
		cases[i].BaselineDigest = baselines[cases[i].Name]
	}
	return cases
}

// Baselines are the committed digests, by case name.
func Baselines() map[string]string {
	digests := map[string]string{}
	if err := json.Unmarshal(baselinesJSON, &digests); err != nil {
		panic(fmt.Sprintf("writeproofcases: baselines.json is not a JSON object of digests: %v", err))
	}
	return digests
}

// Names are the case names, sorted.
func Names() []string {
	cases := Build(nil)
	names := make([]string, len(cases))
	for i, c := range cases {
		names[i] = c.Name
	}
	sort.Strings(names)
	return names
}

// WriteBaselines writes baselines.json for the given digests (oracle only,
// behind UpdateEnv).
func WriteBaselines(path string, digests map[string]string) error {
	encoded, err := json.MarshalIndent(digests, "", "  ")
	if err != nil {
		return err
	}
	return os.WriteFile(path, append(encoded, '\n'), 0o644)
}

// init registers every case that has a committed baseline. A case without one is
// not registered (the verb then refuses its name), and TestEveryMutationHasARegisteredCase
// fails the build: a panic here would also break the oracle that PRODUCES the
// baseline.
func init() {
	baselines := Baselines()
	for _, c := range Build(baselines) {
		if goapiproof.NamesNothing(c.BaselineDigest) {
			continue
		}
		writeproof.Register(c)
	}
}

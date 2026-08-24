package sync

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"
)

// Scheduler eligibility parity, pinned.
//
// WHY THIS FILE EXISTS. Between 2026-08-22 and 2026-08-23 the Go scheduler was
// audited against the retired Python scheduler four or five separate times, and
// each pass checked one symptom that had just been reported. A predicate that
// nobody had thought to ask about that day survived every pass. The cure is not
// another audit; it is to make the parity CONTRACT executable, so the next
// divergence fails here instead of waiting for someone to notice odd rows in a
// fixture database.
//
// The contract has two phases on both sides, and a predicate can agree in one
// phase and diverge in the other:
//
//	PHASE A -- candidate selection / minting an occurrence.
//	  Python: workers/sync_scheduler.py dispatch_scheduled_syncs (:394-439)
//	          and _maybe_dispatch_config (:165-391).
//	  Go:     schedulerHandoffCandidatesSQL (transaction.go:446) and
//	          evaluateContext (evaluate.go:19).
//
//	PHASE B -- locked materialization of a minted occurrence.
//	  Python: sync/execution_trigger.py create_scheduled_sync_execution_trigger
//	          (:74), _require_locked_schedule_contract (:358),
//	          _require_locked_scheduled_eligibility (:319), plus sync/planner.py.
//	  Go:     lockPendingOccurrenceSQL (occurrence_reconciler.go:586),
//	          Materialize (materializer.go:384) and loadMaterializationPlan
//	          (materializer.go:484).
//
// The line numbers above and in the table below are anchors for a reader, not
// assertions -- they were correct at origin/main 3ece6a799 and will rot. What
// is ASSERTED is the behavior and the predicate surface, below.

// parityVerdict is the audited relationship between the Python predicate and
// the Go one.
type parityVerdict string

const (
	// verdictMatch: Go applies the predicate with the same meaning as Python.
	verdictMatch parityVerdict = "MATCH"
	// verdictGap: Python applies the predicate and Go does not. Every gap
	// carries a ticket. A gap is PINNED here rather than silently tolerated so
	// that closing it is a deliberate edit to this table, and so that nobody
	// re-discovers it as if it were news.
	verdictGap parityVerdict = "GAP"
	// verdictByDesign: the two differ deliberately, and the decision is
	// recorded in-code at the cited Go site.
	verdictByDesign parityVerdict = "DIVERGENT-BY-DESIGN"
)

// eligibilityPredicate is one row of the audited parity table.
type eligibilityPredicate struct {
	// Name is the predicate in plain words.
	Name string
	// Phase is "A" (minting) or "B" (locked materialization).
	Phase string
	// PythonSite is where Python applies it, file:line.
	PythonSite string
	// GoSite is where Go applies it, file:line, or the reason it is absent.
	GoSite string
	// Verdict is the audited relationship.
	Verdict parityVerdict
	// Note carries the decision record for verdictByDesign, or the ticket and
	// consequence for verdictGap.
	Note string
	// Decision, when set, is the phase-A Decision this predicate produces. The
	// exhaustiveness test below requires every declared Decision constant to be
	// claimed by exactly one row, so a new decision cannot be added to the
	// scheduler without landing in this table.
	Decision Decision
}

// schedulerEligibilityParity is the audited table. Adding a predicate to the
// scheduler means adding a row here; the tests below fail until you do.
var schedulerEligibilityParity = []eligibilityPredicate{
	{
		Name:       "config.is_active",
		Phase:      "A",
		PythonSite: "workers/sync_scheduler.py:410-411",
		GoSite:     "transaction.go:474 (candidate SQL); evaluate.go:36-39",
		Verdict:    verdictMatch,
		Decision:   DecisionInactive,
	},
	{
		Name:       "config.is_active re-read under lock before materializing",
		Phase:      "B",
		PythonSite: "sync/execution_trigger.py:363-366",
		GoSite:     "occurrence_reconciler.go:595 with FOR UPDATE at :611; materializer.go:392",
		Verdict:    verdictMatch,
	},
	{
		Name:       "config sync_options.schedule_cron is present",
		Phase:      "A",
		PythonSite: "workers/sync_scheduler.py:224-226",
		GoSite:     "transaction.go:475 (candidate SQL); evaluate.go:40-43",
		Verdict:    verdictMatch,
		Decision:   DecisionManual,
	},
	{
		Name:       "config sync_options.schedule_cron re-read under lock before materializing",
		Phase:      "B",
		PythonSite: "sync/execution_trigger.py:367-370, raises \"manual-only\"",
		GoSite:     "ABSENT: lockPendingOccurrenceSQL selects no cron and materializer.go:392 checks only ConfigActive/ConfigPlannerManaged/JobStatus/JobType",
		Verdict:    verdictGap,
		Note: "GAP-1. A config switched to manual-only between minting and " +
			"materialization still materializes in Go; Python refused it. " +
			"Needs a red-first test before any fix.",
	},
	{
		Name:       "config.planner_managed as an ELIGIBILITY gate",
		Phase:      "A",
		PythonSite: "ABSENT: sync_scheduler.py:410-411 filters on is_active only; _maybe_dispatch_config never reads the column",
		GoSite:     "transaction.go:460 (candidate SQL); evaluate.go planner_managed check, first gate in evaluateContext. The Phase B re-check under lock is a separate row below (\"config.planner_managed re-read under lock before materializing\").",
		Verdict:    verdictByDesign,
		Decision:   DecisionNotPlannerManaged,
		Note: "CHAOS-4174 (chris, 2026-08-23): \"That column is useless past " +
			"something being a fixture trigger to not use. Fixtures will " +
			"never be able to be run on a schedule.\" This row previously " +
			"read verdictMatch \"MATCH-BY-ABSENCE\" and was enforced by " +
			"TestPlannerManagedIsNotAnEligibilityPredicate, whose own doc " +
			"comment anticipated exactly this reversal: \"If a " +
			"planner_managed eligibility gate is ever genuinely wanted, it " +
			"is a product decision that needs a backfill migration in the " +
			"same change. Delete this test deliberately at that point; do " +
			"not let it be deleted as collateral.\" That test is deleted " +
			"here, deliberately, not as collateral -- superseded by " +
			"TestPlannerManagedGateAndSourceScopingBothHold below. Its " +
			"stated risk (every pre-0018 config has planner_managed=false " +
			"with no backfill) is addressed, not ignored: a chris-granted " +
			"read-only prod SELECT sized the legacy population created " +
			"before migration 0018 with integration_id IS NOT NULL at ZERO " +
			"rows, so no backfill migration is needed and none is added in " +
			"this change. This is a deliberate Go-only divergence from " +
			"Python, which is why the verdict is DIVERGENT-BY-DESIGN and " +
			"not MATCH: Python is not being mirrored here, it is being " +
			"knowingly diverged from. Python itself is retired in prod " +
			"(CHAOS-4026, 2026-08-21: zero Python celery services run in " +
			"prod since the 2026-08-19 stop; compose.py.workers.yml, which " +
			"defines the beat service, is not included by the default " +
			"compose.yml+compose.go.workers.yml stack, and " +
			"deploy/values-go-workers-only.yaml sets beat.enabled: false) " +
			"so there is no live Python surface to mirror this gate into.",
	},
	{
		Name:       "planner_managed as SOURCE SCOPING for a planner-managed parent",
		Phase:      "B",
		PythonSite: "sync/trigger_routing.py:184-199 and _planner_scoped_source_ids :143-167",
		GoSite:     "materializer.go:1000 via loadPlanSources :994",
		Verdict:    verdictMatch,
		Note: "Unrelated to the CHAOS-4174 eligibility gate below: this is " +
			"routing (which sources a planner-managed parent's run covers), " +
			"not a schedulability decision.",
	},
	{
		Name:       "config.planner_managed re-read under lock before materializing",
		Phase:      "B",
		PythonSite: "ABSENT -- Python never gated on this column (see the Phase-A row above)",
		GoSite:     "occurrence_reconciler.go lockPendingOccurrenceSQL + PendingOccurrence.ConfigPlannerManaged; materializer.go:392 (alongside ConfigActive)",
		Verdict:    verdictByDesign,
		Note: "Added after a codex review of this changeset (2026-08-23): Phase " +
			"A's refusal in evaluateContext cannot protect an occurrence that " +
			"was already minted before this gate existed, or minted by an old " +
			"binary still running mid-rollout. Without this re-check that " +
			"pending occurrence would still materialize into a real sync run " +
			"for a fixture/legacy config. Mirrors \"config.is_active re-read " +
			"under lock before materializing\" above exactly: same locked " +
			"config row, same ineligibility branch in materializer.go:392.",
	},
	{
		Name:       "PagerDuty is exempt from planner tag scoping",
		Phase:      "B",
		PythonSite: "sync/trigger_routing.py:191-197, explicit provider != \"pagerduty\" with rationale",
		GoSite:     "materializer.go:548-561 and :572-574; PagerDuty bypasses loadPlanSources and uses the repaired account source",
		Verdict:    verdictMatch,
		Note:       "Different mechanism, same outcome: the account-scoped source is used, not a tag-scoped set.",
	},
	{
		Name:       "a scheduled_jobs marker exists for the config",
		Phase:      "A",
		PythonSite: "workers/sync_scheduler.py:118-150 _ensure_due_job_marker, called at :279 -- Python CREATES the marker when missing",
		GoSite:     "transaction.go:470-473 requires it via inner JOIN; no non-test INSERT into scheduled_jobs exists in internal/",
		Verdict:    verdictGap,
		Note: "GAP-2, latent. A config with a cron but no marker can never be " +
			"scheduled by Go. Currently unreachable because the sole " +
			"production write path creates the anchor " +
			"(api/admin/routers/sync.py:1503 _create_planner_managed_config).",
	},
	{
		Name:       "job.status is ACTIVE",
		Phase:      "A",
		PythonSite: "workers/sync_scheduler.py:243-244",
		GoSite:     "transaction.go:476 (candidate SQL); evaluate.go:48-51",
		Verdict:    verdictMatch,
		Decision:   DecisionInactiveJob,
	},
	{
		Name:       "job.status is ACTIVE, re-read under lock",
		Phase:      "B",
		PythonSite: "sync/execution_trigger.py:376",
		GoSite:     "occurrence_reconciler.go:596; materializer.go:392",
		Verdict:    verdictMatch,
	},
	{
		Name:       "job.is_running blocks, with a 2h staleness escape",
		Phase:      "A",
		PythonSite: "workers/sync_scheduler.py:39 TTL, :53-65 _running_marker_is_stale, applied :246-256",
		GoSite:     "transaction.go:478-482; evaluate.go:52-56; runningMarkerState :124-135",
		Verdict:    verdictMatch,
		Decision:   DecisionFreshRunning,
	},
	{
		Name:       "job.next_run_at is a do-not-re-dispatch-before marker",
		Phase:      "A",
		PythonSite: "workers/sync_scheduler.py:259-263",
		GoSite:     "transaction.go:477 (candidate SQL); evaluate.go:60-64",
		Verdict:    verdictMatch,
		Decision:   DecisionNextRunGate,
	},
	{
		Name:       "job.job_type is 'sync'",
		Phase:      "A+B",
		PythonSite: "sync/execution_trigger.py:375",
		GoSite:     "transaction.go:473; occurrence_reconciler.go:597; materializer.go:392",
		Verdict:    verdictMatch,
	},
	{
		Name:       "config and job agree on org_id and sync_config_id",
		Phase:      "B",
		PythonSite: "sync/execution_trigger.py:371-380",
		GoSite:     "occurrence_reconciler.go:599-605, enforced structurally by the JOIN",
		Verdict:    verdictMatch,
	},
	{
		Name:       "the cron occurrence is due",
		Phase:      "A",
		PythonSite: "workers/sync_scheduler.py:265-278",
		GoSite:     "evaluate.go:97-121",
		Verdict:    verdictMatch,
		Decision:   DecisionNotDue,
	},
	{
		Name:       "the due-ness base instant",
		Phase:      "A",
		PythonSite: "workers/sync_scheduler.py:265-278: last_sync_at, else created_at",
		GoSite:     "evaluate.go:78-95: the LATER of last_sync_at and the occurrence ledger, clamped to observedAt",
		Verdict:    verdictByDesign,
		Note: "Decision recorded in-code at evaluate.go:71-77 citing CHAOS-3936: " +
			"last_sync_at advances only when a run COMPLETES, so a run that " +
			"never completes freezes the base forever. Clamp rationale at :84-91.",
	},
	{
		Name:       "the cron expression parses and is supported",
		Phase:      "A",
		PythonSite: "workers/task_utils.py cron_next_run, raising into sync_scheduler.py:429-435",
		GoSite:     "evaluate.go:102-112",
		Verdict:    verdictMatch,
		Decision:   DecisionInvalidCron,
	},
	{
		Name:       "the cron expression uses supported grammar",
		Phase:      "A",
		PythonSite: "workers/task_utils.py cron_next_run",
		GoSite:     "evaluate.go:106-109",
		Verdict:    verdictMatch,
		Decision:   DecisionUnsupportedCron,
	},
	{
		Name:       "everything passed: the schedule is due and may be handed off",
		Phase:      "A",
		PythonSite: "workers/sync_scheduler.py:303-326",
		GoSite:     "evaluate.go:119-121",
		Verdict:    verdictMatch,
		Decision:   DecisionScheduleDue,
	},
	{
		Name:       "the organization exists",
		Phase:      "A",
		PythonSite: "workers/sync_scheduler.py:204-205 organization_exists_sync, which skips BEFORE minting",
		GoSite:     "coordinator.go organizationExists, via coordinatorEligibility; refusal counted as HandoffResult.SkippedOrgMissing",
		Verdict:    verdictMatch,
		Decision:   DecisionOrgMissing,
		Note: "Was GAP-3. Fixed at the Coordinator, which transaction.go's " +
			"Coordinator doc comment already named as the owner of " +
			"\"organization existence and feature entitlement\" -- the contract " +
			"was documented and unimplemented, not missing.",
	},
	{
		Name:       "the organization guard's behaviour on a database error",
		Phase:      "A",
		PythonSite: "workers/org_guard.py:31-36 catches SQLAlchemyError and returns True, i.e. fails OPEN and schedules anyway",
		GoSite:     "coordinator.go organizationExists returns the error, i.e. fails CLOSED",
		Verdict:    verdictByDesign,
		Note: "Deliberate. In Go the error aborts the window before any marker " +
			"advances, so the schedule stays due and the next tick retries -- " +
			"nothing is lost, and declining to mint for an organization that " +
			"could not be verified is safer than minting for one that may " +
			"already be deleted. Python had no such guarantee because its " +
			"guard ran outside the marker transaction.",
	},
	{
		Name:       "the organization exists, locked FOR KEY SHARE",
		Phase:      "B",
		PythonSite: "sync/execution_trigger.py:327-345",
		GoSite:     "materializer.go:621-637 lockScheduledOrganization, same lock mode and same non-UUID escape",
		Verdict:    verdictMatch,
	},
	{
		Name:       "the canonical-incident feature is enabled for the org",
		Phase:      "A",
		PythonSite: "workers/sync_scheduler.py:207-219, which skips BEFORE minting",
		GoSite:     "coordinator.go coordinatorEligibility via canonicalIncidentAllowed(..., lockRows=false); counted as HandoffResult.SkippedFeatureDisabled",
		Verdict:    verdictMatch,
		Decision:   DecisionFeatureDisabled,
		Note: "Was GAP-4. Phase A reads the entitlement WITHOUT locking and " +
			"phase B keeps FOR UPDATE, which is Python's split too " +
			"(is_canonical_incident_feature_enabled_sync before minting, " +
			"require_canonical_incident_feature_for_update_sync at " +
			"materialization). One resolver, two call sites -- expressing this " +
			"entitlement a second time in SQL is how the two would drift.",
	},
	{
		Name:       "the canonical-incident gate applies only to gated sync targets",
		Phase:      "A+B",
		PythonSite: "sync/canonical_incident_gate.py:25 and :37-42, applied at workers/sync_scheduler.py:207",
		GoSite:     "materializer.go syncTargetsRequireCanonicalIncident, consulted by coordinatorEligibility before the entitlement lookup",
		Verdict:    verdictMatch,
		Note: "Load-bearing: consulting the feature unconditionally would turn " +
			"one disabled flag into a fleet-wide scheduling outage.",
	},
	{
		Name:       "the canonical-incident feature is enabled, locked FOR UPDATE",
		Phase:      "B",
		PythonSite: "sync/execution_trigger.py:348-355; terminalized at sync_scheduler.py:350-382",
		GoSite:     "materializer.go:537-545 and :585-592 via canonicalIncidentAllowedForUpdate :907-990",
		Verdict:    verdictMatch,
	},
	{
		Name:       "the phase-A entitlement read does not lock",
		Phase:      "A",
		PythonSite: "sync/canonical_incident_gate.py:163-175 is_canonical_incident_feature_enabled_sync, which takes no locks",
		GoSite:     "coordinator.go calls canonicalIncidentAllowed with lockRows=false",
		Verdict:    verdictMatch,
		Note: "Raised in review as a race: the entitlement can change between " +
			"this read and the commit that mints. True, and Python has the " +
			"identical race for the identical reason -- locking the global " +
			"feature_flags row on every scheduler window would serialize every " +
			"replica against one row. Both sides make phase A best-effort and " +
			"phase B authoritative under FOR UPDATE, so an occurrence minted " +
			"against a stale entitlement is refused at materialization. The " +
			"gate narrows the window; it was never claimed to close it.",
	},
	{
		Name:       "the instant used to judge entitlement expiry",
		Phase:      "A+B",
		PythonSite: "licensing feature decision uses wall-clock datetime.now(UTC)",
		GoSite:     "coordinator.go passes occurrence.ObservedAt; materializer.go:538,586 pass occurrence.ScheduledFor",
		Verdict:    verdictByDesign,
		Note: "This scheduler is observedAt-parameterized end to end so its " +
			"windows are reproducible; reaching for wall-clock time inside it " +
			"would make the decision untestable and non-deterministic. The " +
			"resulting skew is bounded by one window, and phase A's ObservedAt " +
			"is already FRESHER than phase B's ScheduledFor, so the phase-A " +
			"gate cannot admit something phase B would then admit on a staler " +
			"clock. Raised in review; recorded rather than changed.",
	},
	{
		Name:       "how a sync target string is normalized before the gate",
		Phase:      "A+B",
		PythonSite: "sync/canonical_incident_gate.py:37-42 lowercases only",
		GoSite:     "materializer.go syncTargetsRequireCanonicalIncident lowercases AND trims whitespace",
		Verdict:    verdictByDesign,
		Note: "Pre-existing Go behaviour, not introduced here -- this change " +
			"reuses that function rather than writing a second normalizer, " +
			"because phase A and phase B disagreeing about what \"operational\" " +
			"means would be a worse defect than either rule alone. Direction of " +
			"the divergence: Go gates a padded target that Python would let " +
			"through. Raised in review; if the trim is ever judged wrong it " +
			"must be changed in that one function, for both phases at once.",
	},
	{
		Name:       "the integration exists and is active",
		Phase:      "B",
		PythonSite: "sync/execution_trigger.py:141-144 \"no planner route\", from trigger_routing.plan_request_for_config returning None",
		GoSite:     "materializer.go:494-497 JOIN integrations ... AND integration.is_active; ErrOccurrenceIneligible at :502",
		Verdict:    verdictMatch,
	},
	{
		Name:       "the credential is present, active, and matches org and provider",
		Phase:      "B",
		PythonSite: "sync/planner.py:492-540",
		GoSite:     "materializer.go:639-666 resolveCredentialStamp, org/provider/is_active at :654",
		Verdict:    verdictMatch,
	},
	{
		Name:       "the tier backfill_days cap",
		Phase:      "B",
		PythonSite: "sync/planner.py:1241 and _get_tier_backfill_days_cap :1248-1269",
		GoSite:     "materializer.go:788-871 loadPlanLimits",
		Verdict:    verdictMatch,
	},
	{
		Name:       "the tier max_sync_units cap",
		Phase:      "B",
		PythonSite: "sync/guard.py:488-506 _resolve_total_unit_cap, imported by planner.py:133",
		GoSite:     "materializer.go:598 with rejection at :412-414",
		Verdict:    verdictMatch,
	},
	{
		Name:       "the occurrence identity still matches its config and marker",
		Phase:      "B",
		PythonSite: "sync/execution_trigger.py:101,113 and _verify_scheduled_occurrence :383-405",
		GoSite:     "occurrence_reconciler.go:486-501 occurrenceIdentityIsValid",
		Verdict:    verdictMatch,
	},
}

// TestParityTableClaimsEveryDeclaredDecision is the anti-"one symptom per pass"
// ratchet for the phase-A decision surface. It reads the Decision constants out
// of types.go rather than from a list maintained here, so adding a new decision
// to the scheduler fails this test until the parity table gains a row for it.
// A hand-maintained list would defeat the point: it would be one more place
// somebody forgets to update, which is the exact failure this file exists for.
func TestParityTableClaimsEveryDeclaredDecision(t *testing.T) {
	declared := declaredDecisions(t)
	if len(declared) == 0 {
		t.Fatal("no Decision constants parsed out of types.go: the parser below " +
			"has rotted and this ratchet is silently passing")
	}

	claimed := map[Decision]string{}
	for _, predicate := range schedulerEligibilityParity {
		if predicate.Decision == "" {
			continue
		}
		if previous, duplicate := claimed[predicate.Decision]; duplicate {
			t.Errorf("Decision %q is claimed by two parity rows (%q and %q); "+
				"each decision must have exactly one owning predicate",
				predicate.Decision, previous, predicate.Name)
		}
		claimed[predicate.Decision] = predicate.Name
	}

	for _, decision := range declared {
		if _, ok := claimed[decision]; !ok {
			t.Errorf("Decision %q is declared in types.go but no row of "+
				"schedulerEligibilityParity claims it. A new scheduler decision "+
				"is a new eligibility predicate: add its row (with the Python "+
				"site it mirrors, or an explicit GAP/DIVERGENT-BY-DESIGN "+
				"verdict) before landing it.", decision)
		}
	}
	for decision, owner := range claimed {
		if !containsDecision(declared, decision) {
			t.Errorf("parity row %q claims Decision %q, which no longer exists "+
				"in types.go", owner, decision)
		}
	}
}

// TestCandidateSQLPredicateSurfaceIsPinned pins WHICH COLUMNS the phase-A
// candidate query filters on. Adding or removing a filtered column changes the
// set and fails here, forcing the parity table to be updated in the same
// change.
//
// Honest limit: this pins the predicate SURFACE, not its sense. Flipping
// `config.is_active = TRUE` to `= FALSE` keeps the same column set and would
// slip past this test -- that inversion is caught by
// TestPhaseADecisionsMatchThePythonGates below and by the Postgres-backed
// tests in transaction_integration_test.go. The three together are the pin.
//
// config.planner_managed (CHAOS-4174) is deliberately NOT in this list.
// predicateColumns only scans the FROM-through-WHERE region, and
// planner_managed is read in the SELECT list but never filtered in SQL --
// like org_missing and feature_disabled before it, the refusal happens after
// the row is read (evaluateContext, not a WHERE clause), so every candidate
// still gets counted rather than silently vanishing from the window. See
// TestPlannerManagedGateAndSourceScopingBothHold for the column's own pin.
func TestCandidateSQLPredicateSurfaceIsPinned(t *testing.T) {
	want := []string{
		"config.id",
		"config.is_active",
		"config.org_id",
		"config.sync_options",
		"job.is_running",
		"job.job_type",
		"job.last_run_at",
		"job.next_run_at",
		"job.org_id",
		"job.status",
		"job.sync_config_id",
		"job.updated_at",
	}
	got := predicateColumns(schedulerHandoffCandidatesSQL)
	assertColumnSet(t, "schedulerHandoffCandidatesSQL", got, want)
}

// TestLockedMaterializationPredicateSurfaceIsPinned does the same for the
// phase-B claim query. The absence of a schedule_cron column here IS GAP-1:
// the row is pinned so that closing the gap is a deliberate edit to both this
// list and the parity table, and so the gap cannot quietly become folklore.
//
// config.planner_managed (CHAOS-4174) IS present here, unlike GAP-1's missing
// schedule_cron: a pending occurrence minted before this ticket's Phase-A gate
// existed (or by an old binary mid-rollout) must not be allowed to
// materialize a real sync run for a fixture/legacy config just because it was
// already sitting in the table. See materializer.go:392, which re-checks it
// exactly like ConfigActive.
func TestLockedMaterializationPredicateSurfaceIsPinned(t *testing.T) {
	want := []string{
		"config.id",
		"config.is_active",
		"config.org_id",
		"config.planner_managed",
		"job.id",
		"job.job_type",
		"job.org_id",
		"job.status",
		"job.sync_config_id",
		"occurrence.identity_version",
		"occurrence.job_run_id",
		"occurrence.occurrence_id",
		"occurrence.org_id",
		"occurrence.reconcile_attempt_count",
		"occurrence.reconcile_next_attempt_at",
		"occurrence.reconcile_status",
		"occurrence.scheduled_for",
		"occurrence.scheduled_job_id",
		"occurrence.sync_config_id",
		"occurrence.sync_run_id",
	}
	got := referencedColumns(lockPendingOccurrenceSQL)
	assertColumnSet(t, "lockPendingOccurrenceSQL", got, want)

	for _, column := range got {
		if strings.Contains(column, "schedule_cron") {
			t.Errorf("lockPendingOccurrenceSQL now filters on %s. That closes "+
				"GAP-1; update the schedulerEligibilityParity row "+
				"\"config sync_options.schedule_cron re-read under lock before "+
				"materializing\" from GAP to MATCH and add its red-first test.",
				column)
		}
	}
}

// TestPlannerManagedGateAndSourceScopingBothHold supersedes (and deliberately
// deletes) TestPlannerManagedIsNotAnEligibilityPredicate.
//
// That test was written on 2026-08-23 after an audit found the Go scheduler
// never read planner_managed at all and concluded -- correctly, at the time --
// that Python didn't either, so adding a gate would CREATE a divergence, not
// close one. Its own doc comment named the exact condition that would flip
// the answer: "If a planner_managed eligibility gate is ever genuinely
// wanted, it is a product decision that needs a backfill migration in the
// same change." Later that same day chris ruled it genuinely wanted
// (CHAOS-4174): "That column is useless past something being a fixture
// trigger to not use. Fixtures will never be able to be run on a schedule."
// The backfill-migration condition is satisfied by proof rather than by a
// migration: a chris-granted read-only prod SELECT sized the legacy
// population (configs created before migration 0018 with integration_id IS
// NOT NULL) at ZERO rows, so there is nothing for a backfill to fix and none
// is added here.
//
// This test asserts the new state directly instead of leaving a hole where
// the old test's protections were:
//  1. The gate exists where the old test forbade it (schedulerHandoffCandidatesSQL
//     now DOES reference planner_managed).
//  2. The one property the old test protected alongside the absence -- that
//     materializer.go still scopes a planner-managed parent to its tagged
//     sources -- still holds. That source-scoping behaviour is UNCHANGED by
//     this ticket.
//  3. lockPendingOccurrenceSQL (Phase B) ALSO now references the column, added
//     after a codex review of this changeset found that Phase A's refusal
//     could not protect a pending occurrence minted before this gate existed
//     (or by an old binary mid-rollout): without a Phase B re-check, that
//     occurrence would still materialize into a real sync run. This mirrors
//     config.is_active exactly (see "config.is_active re-read under lock"
//     above) -- Phase A refuses before minting, Phase B refuses again before
//     materializing, because a decision made once at mint time is not
//     guaranteed to still hold by the time a pending row is consumed.
func TestPlannerManagedGateAndSourceScopingBothHold(t *testing.T) {
	if !strings.Contains(schedulerHandoffCandidatesSQL, "planner_managed") {
		t.Error("schedulerHandoffCandidatesSQL no longer references " +
			"planner_managed. CHAOS-4174 requires the candidate query to read " +
			"the column so evaluateContext can refuse a false value.")
	}
	if !strings.Contains(lockPendingOccurrenceSQL, "planner_managed") {
		t.Error("lockPendingOccurrenceSQL no longer references planner_managed. " +
			"A pending occurrence for a fixture/legacy config must be refused " +
			"again under lock before materialization, not just at mint time -- " +
			"see materializer.go:392's ConfigPlannerManaged check.")
	}
	if !sourceContainsStringLiteral(t, "materializer.go", "planner_managed_sync_config_id") {
		t.Error("materializer.go no longer scopes a planner-managed parent to " +
			"its tagged sources (sync/trigger_routing.py:143-167). CHAOS-4174 " +
			"adds an eligibility gate; it must not regress this unrelated " +
			"routing behaviour.")
	}
	materializerSource, err := os.ReadFile("materializer.go")
	if err != nil {
		t.Fatalf("read materializer.go: %v", err)
	}
	if !strings.Contains(string(materializerSource), "ConfigPlannerManaged") {
		t.Error("materializer.go no longer re-checks ConfigPlannerManaged before " +
			"materializing a locked pending occurrence.")
	}
}

// TestUnpopulatedPlannerManagedFieldRefusesRatherThanAdmits pins the DEFAULT
// DIRECTION of Candidate.PlannerManaged, not just its explicit-false case
// (which TestPhaseADecisionsMatchThePythonGates already covers). Go's zero
// value for a bool is false, and false is CHAOS-4174's refusing value, so a
// caller that ever forgets to populate this field -- a new Candidate
// construction path added in a future refactor, say -- fails CLOSED: the
// config is refused, not silently treated as eligible. This candidate
// deliberately never sets PlannerManaged, so the field is read at its Go zero
// value, not an explicit literal false, to prove that direction.
func TestUnpopulatedPlannerManagedFieldRefusesRatherThanAdmits(t *testing.T) {
	candidate := Candidate{
		ConfigID:     "unpopulated",
		Active:       true,
		ScheduleCron: "0 * * * *",
		CreatedAt:    at("2026-01-01T00:00:00Z"),
	}
	if candidate.PlannerManaged {
		t.Fatal("test setup bug: PlannerManaged is not at its Go zero value")
	}
	got := Evaluate(candidate, at("2026-01-02T00:00:00Z"))
	if got.Decision != DecisionNotPlannerManaged {
		t.Fatalf("Evaluate() with an unpopulated PlannerManaged field = %q, "+
			"want %q: an omitted field must refuse, never silently admit",
			got.Decision, DecisionNotPlannerManaged)
	}
}

// TestPhaseADecisionsMatchThePythonGates exercises every phase-A gate through
// the real Evaluate entry point, in the order Python applies them. Ordering is
// part of the contract, not an accident: Python classifies a config with a
// wedged next_run_at as next-run-gated even when its cron is malformed, so the
// marker gate must precede cron parsing on both sides.
func TestPhaseADecisionsMatchThePythonGates(t *testing.T) {
	observedAt := time.Date(2026, 8, 23, 12, 0, 0, 0, time.UTC)
	hourAgo := observedAt.Add(-time.Hour)
	future := observedAt.Add(time.Hour)
	longAgo := observedAt.Add(-72 * time.Hour)
	activeJob := func(mutate func(*Job)) *Job {
		job := &Job{
			ID:           "job",
			ScheduleCron: "0 * * * *",
			Timezone:     "UTC",
			Status:       0,
			UpdatedAt:    &hourAgo,
		}
		if mutate != nil {
			mutate(job)
		}
		return job
	}

	for _, test := range []struct {
		name      string
		predicate string
		candidate Candidate
		want      Decision
	}{
		{
			name:      "fixture-style config, planner_managed false",
			predicate: "config.planner_managed as an ELIGIBILITY gate",
			candidate: Candidate{
				ConfigID: "c", Active: true, PlannerManaged: false, ScheduleCron: "0 * * * *",
				CreatedAt: longAgo, Job: activeJob(nil),
			},
			want: DecisionNotPlannerManaged,
		},
		{
			name:      "inactive config",
			predicate: "config.is_active",
			candidate: Candidate{
				ConfigID: "c", Active: false, PlannerManaged: true, ScheduleCron: "0 * * * *",
				CreatedAt: longAgo, Job: activeJob(nil),
			},
			want: DecisionInactive,
		},
		{
			name:      "manual-only config",
			predicate: "config sync_options.schedule_cron is present",
			candidate: Candidate{
				ConfigID: "c", Active: true, PlannerManaged: true, ScheduleCron: "",
				CreatedAt: longAgo, Job: activeJob(nil),
			},
			want: DecisionManual,
		},
		{
			name:      "paused job marker",
			predicate: "job.status is ACTIVE",
			candidate: Candidate{
				ConfigID: "c", Active: true, PlannerManaged: true, ScheduleCron: "0 * * * *",
				CreatedAt: longAgo,
				Job:       activeJob(func(job *Job) { job.Status = 1 }),
			},
			want: DecisionInactiveJob,
		},
		{
			name:      "fresh running marker",
			predicate: "job.is_running blocks, with a 2h staleness escape",
			candidate: Candidate{
				ConfigID: "c", Active: true, PlannerManaged: true, ScheduleCron: "0 * * * *",
				CreatedAt: longAgo,
				Job: activeJob(func(job *Job) {
					job.IsRunning = true
					job.LastRunAt = &hourAgo
				}),
			},
			want: DecisionFreshRunning,
		},
		{
			name:      "stale running marker does not block",
			predicate: "job.is_running blocks, with a 2h staleness escape",
			candidate: Candidate{
				ConfigID: "c", Active: true, PlannerManaged: true, ScheduleCron: "0 * * * *",
				CreatedAt: longAgo,
				Job: activeJob(func(job *Job) {
					job.IsRunning = true
					job.LastRunAt = &longAgo
				}),
			},
			want: DecisionScheduleDue,
		},
		{
			name:      "next_run_at in the future",
			predicate: "job.next_run_at is a do-not-re-dispatch-before marker",
			candidate: Candidate{
				ConfigID: "c", Active: true, PlannerManaged: true, ScheduleCron: "0 * * * *",
				CreatedAt: longAgo,
				Job:       activeJob(func(job *Job) { job.NextRunAt = &future }),
			},
			want: DecisionNextRunGate,
		},
		{
			name:      "next_run_at gate precedes cron parsing, as in Python",
			predicate: "job.next_run_at is a do-not-re-dispatch-before marker",
			candidate: Candidate{
				ConfigID: "c", Active: true, PlannerManaged: true, ScheduleCron: "0 * * * *",
				CreatedAt: longAgo,
				Job: activeJob(func(job *Job) {
					job.NextRunAt = &future
					job.ScheduleCron = "not a cron"
				}),
			},
			want: DecisionNextRunGate,
		},
		{
			name:      "malformed cron",
			predicate: "the cron expression parses and is supported",
			candidate: Candidate{
				ConfigID: "c", Active: true, PlannerManaged: true, ScheduleCron: "0 * * * *",
				CreatedAt: longAgo,
				Job:       activeJob(func(job *Job) { job.ScheduleCron = "not a cron" }),
			},
			want: DecisionInvalidCron,
		},
		{
			name:      "not yet due",
			predicate: "the cron occurrence is due",
			candidate: Candidate{
				ConfigID: "c", Active: true, PlannerManaged: true, ScheduleCron: "0 * * * *",
				LastSyncAt: &observedAt, CreatedAt: longAgo,
				Job: activeJob(nil),
			},
			want: DecisionNotDue,
		},
		{
			name:      "due",
			predicate: "everything passed: the schedule is due and may be handed off",
			candidate: Candidate{
				ConfigID: "c", Active: true, PlannerManaged: true, ScheduleCron: "0 * * * *",
				CreatedAt: longAgo, Job: activeJob(nil),
			},
			want: DecisionScheduleDue,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if !parityTableHasPredicate(test.predicate) {
				t.Fatalf("this case pins predicate %q, which is not a row of "+
					"schedulerEligibilityParity; the table and the tests have "+
					"drifted apart", test.predicate)
			}
			got := Evaluate(test.candidate, observedAt)
			if got.Decision != test.want {
				t.Fatalf("Evaluate() decision = %q, want %q (predicate: %s)",
					got.Decision, test.want, test.predicate)
			}
		})
	}
}

// TestEveryParityGapCarriesATicketNote keeps a gap from being downgraded to a
// bare table entry that nobody acts on.
func TestEveryParityGapCarriesATicketNote(t *testing.T) {
	for _, predicate := range schedulerEligibilityParity {
		switch predicate.Verdict {
		case verdictGap:
			if !strings.Contains(predicate.Note, "GAP-") {
				t.Errorf("parity row %q is a GAP but its Note does not name a "+
					"GAP identifier; a gap without a tracked identifier is a "+
					"gap nobody closes", predicate.Name)
			}
		case verdictByDesign:
			if predicate.Note == "" {
				t.Errorf("parity row %q is DIVERGENT-BY-DESIGN but cites no "+
					"decision record", predicate.Name)
			}
		case verdictMatch:
		default:
			t.Errorf("parity row %q has unknown verdict %q", predicate.Name, predicate.Verdict)
		}
		if predicate.PythonSite == "" || predicate.GoSite == "" {
			t.Errorf("parity row %q must cite both a Python site and a Go site "+
				"(or state explicitly that one is ABSENT)", predicate.Name)
		}
	}
}

func parityTableHasPredicate(name string) bool {
	for _, predicate := range schedulerEligibilityParity {
		if predicate.Name == name {
			return true
		}
	}
	return false
}

func containsDecision(decisions []Decision, want Decision) bool {
	for _, decision := range decisions {
		if decision == want {
			return true
		}
	}
	return false
}

// declaredDecisions returns the Decision constants declared in types.go.
//
// It parses the Go DECLARATIONS, not the file's text. A review of this
// changeset pointed out that the first version used a regular expression, which
// would happily match a Decision-shaped string inside a comment or an unrelated
// string literal -- so a constant could be "found" that did not exist, or a
// commented-out one could keep the ratchet quiet. Reading the AST removes the
// whole class of that mistake.
//
// Parsing the source rather than keeping a list here is deliberate: a
// hand-maintained list would be one more place to forget, which is the exact
// failure mode this file exists to remove.
func declaredDecisions(t *testing.T) []Decision {
	t.Helper()
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "types.go", nil, 0)
	if err != nil {
		t.Fatalf("parse types.go to enumerate Decision constants: %v", err)
	}
	var decisions []Decision
	for _, declaration := range parsed.Decls {
		general, ok := declaration.(*ast.GenDecl)
		if !ok || general.Tok != token.CONST {
			continue
		}
		for _, spec := range general.Specs {
			value, ok := spec.(*ast.ValueSpec)
			if !ok {
				continue
			}
			typeName, ok := value.Type.(*ast.Ident)
			if !ok || typeName.Name != "Decision" {
				continue
			}
			for _, expression := range value.Values {
				literal, ok := expression.(*ast.BasicLit)
				if !ok || literal.Kind != token.STRING {
					continue
				}
				decisions = append(decisions, Decision(strings.Trim(literal.Value, `"`)))
			}
		}
	}
	return decisions
}

// sourceContainsStringLiteral reports whether a Go file contains the given text
// inside an actual string literal. Used instead of a plain substring search so
// that a comment mentioning a predicate cannot stand in as proof the predicate
// is still in the query -- another review finding against the first version of
// this file.
func sourceContainsStringLiteral(t *testing.T, path, needle string) bool {
	t.Helper()
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, path, nil, 0)
	if err != nil {
		t.Fatalf("parse %s: %v", path, err)
	}
	found := false
	ast.Inspect(parsed, func(node ast.Node) bool {
		literal, ok := node.(*ast.BasicLit)
		if !ok || literal.Kind != token.STRING {
			return true
		}
		if strings.Contains(literal.Value, needle) {
			found = true
			return false
		}
		return true
	})
	return found
}

var columnReferencePattern = regexp.MustCompile(`\b([a-z_]+)\.([a-z_]+)\b`)

// predicateColumns returns the sorted, de-duplicated set of qualified columns a
// query CONSTRAINS -- everything from the top-level FROM (so JOIN conditions
// count, they are predicates too) through the WHERE clause. Correlated
// subqueries in the SELECT list are excluded: the top-level FROM is the one at
// the start of a line, subquery FROMs are indented.
func predicateColumns(query string) []string {
	region := query
	if index := strings.LastIndex(region, "\nFROM "); index >= 0 {
		region = region[index:]
	} else {
		return nil
	}
	for _, terminator := range []string{"ORDER BY", "FOR UPDATE", "GROUP BY", "LIMIT"} {
		if index := strings.Index(region, terminator); index >= 0 {
			region = region[:index]
		}
	}
	return qualifiedColumns(region)
}

// referencedColumns returns every qualified column a query touches at all,
// SELECT list included. Phase B needs this wider view: the claim query READS
// config.is_active and job.status and Go checks them in Go code afterwards
// (materializer.go:392) rather than in SQL, so a pin that looked only at the
// WHERE clause would miss them being dropped.
func referencedColumns(query string) []string {
	return qualifiedColumns(query)
}

func qualifiedColumns(region string) []string {
	seen := map[string]bool{}
	var columns []string
	for _, match := range columnReferencePattern.FindAllStringSubmatch(region, -1) {
		// "public.sync_configurations" and friends are table names, not columns.
		if match[1] == "public" {
			continue
		}
		qualified := match[1] + "." + match[2]
		if seen[qualified] {
			continue
		}
		seen[qualified] = true
		columns = append(columns, qualified)
	}
	sort.Strings(columns)
	return columns
}

func assertColumnSet(t *testing.T, name string, got, want []string) {
	t.Helper()
	if strings.Join(got, ",") == strings.Join(want, ",") {
		return
	}
	t.Errorf("%s filters on a different set of columns than the audited "+
		"parity table records.\n  got:  %v\n  want: %v\n"+
		"A changed eligibility predicate must be reflected in "+
		"schedulerEligibilityParity in the same change -- that table is the "+
		"contract, this test is only its enforcement.", name, got, want)
}

// TestOrganizationGuardMirrorsPythonBranchForBranch pins the org guard's four
// branches directly, because three of them are decided WITHOUT touching the
// database and one of them (the empty org id) cannot be reached through
// Handoff at all -- Handoff's own validation rejects an empty OrgID first. A
// review of this changeset flagged that branch as dead code presented as live;
// this test is where its real behaviour is recorded.
//
// The nil transaction is the assertion, not a shortcut: each of these three
// inputs must be answered by the branch alone. If any of them ever starts
// issuing a query, this panics rather than quietly gaining a database
// dependency Python does not have.
func TestOrganizationGuardMirrorsPythonBranchForBranch(t *testing.T) {
	for _, test := range []struct {
		name   string
		orgID  string
		python string
	}{
		{
			name:   "empty org id",
			orgID:  "",
			python: "org_guard.py:15-16, `if not org_id ... return True`",
		},
		{
			name:   "the literal default",
			orgID:  "default",
			python: "org_guard.py:15-16, the `org_id == \"default\"` arm",
		},
		{
			name:   "a non-UUID org id",
			orgID:  "org-integration",
			python: "org_guard.py:18-20, ValueError from uuid.UUID() returns True",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			exists, err := organizationExists(context.Background(), nil, test.orgID)
			if err != nil {
				t.Fatalf("organizationExists(%q) error = %v", test.orgID, err)
			}
			if !exists {
				t.Fatalf("organizationExists(%q) = false, want true: Python "+
					"admits this without a lookup (%s)", test.orgID, test.python)
			}
		})
	}
}

// TestCoordinatorOwnedDecisionsAreCoveredWhereTheyLive closes a gap a review of
// this changeset found in the ratchet itself: TestPhaseADecisionsMatchThePython
// Gates drives Evaluate, and Evaluate can never return the two Coordinator-owned
// decisions, so those two could have been added to the parity table with no
// behavioural coverage anywhere and the table would still have looked complete.
//
// Every Decision the parity table claims must therefore be covered by SOMETHING:
// the Evaluate-driven table for kernel decisions, and the Postgres-backed gate
// suite for Coordinator decisions. This asserts the second half by name.
func TestCoordinatorOwnedDecisionsAreCoveredWhereTheyLive(t *testing.T) {
	source, err := os.ReadFile("eligibility_gate_integration_test.go")
	if err != nil {
		t.Fatalf("read the gate suite to confirm Coordinator decisions are covered: %v", err)
	}
	for _, required := range []struct {
		decision Decision
		test     string
	}{
		{decision: DecisionOrgMissing, test: "TestHandoffRefusesAConfigWhoseOrganizationDoesNotExist"},
		{decision: DecisionFeatureDisabled, test: "TestHandoffRefusesAConfigWhoseCanonicalIncidentFeatureIsDisabled"},
	} {
		if !parityTableClaimsDecision(required.decision) {
			t.Errorf("Decision %q is no longer claimed by the parity table", required.decision)
			continue
		}
		if !strings.Contains(string(source), "func "+required.test+"(") {
			t.Errorf("Decision %q is a Coordinator-owned decision whose covering "+
				"test %s is missing from eligibility_gate_integration_test.go. "+
				"Evaluate cannot produce this decision, so without that test it "+
				"has no behavioural coverage at all.", required.decision, required.test)
		}
	}
}

func parityTableClaimsDecision(decision Decision) bool {
	for _, predicate := range schedulerEligibilityParity {
		if predicate.Decision == decision {
			return true
		}
	}
	return false
}

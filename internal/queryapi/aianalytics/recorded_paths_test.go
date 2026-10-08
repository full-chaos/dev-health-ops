package aianalytics

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-go/clickhouse"

	"github.com/full-chaos/dev-health-ops/internal/queryapi/graph/model"
	"github.com/full-chaos/dev-health-ops/internal/queryapi/graphqldate"
	"github.com/full-chaos/dev-health-ops/internal/testsupport/recordedpaths"
)

// CHAOS-7473: every scalar path of oracle_cases.json and oracle_b_cases.json is declared as a CLAIM
// (a change makes a test of this package fail; the mutation run in the pull request that added
// this file backs that) or a NOT-A-CLAIM with its reason. The reasons cite the Python resolvers at
// 6121e851f4^ (they were deleted by #2794; `git show 6121e851f4^:src/dev_health_ops/api/graphql/
// resolvers/ai.py`), and the probe tests below check that the Go answer really does not move when
// those fields are perturbed. Nothing here needs Python or a Python file at test time.
//
// Four paths are named OPEN: Python reads them and no case moves, and the recorder that made the
// file is not in the tree (testdata/README.md names the ops sha it was generated at).

var oracleCasesClaims = []string{
	".cases[].name",
	".cases[].dataset",
	".cases[].engagementError",
	".cases[].expected.agentCreatedPrs",
	".cases[].expected.aiAssistedPrRatio",
	".cases[].expected.aiAssistedPrs",
	".cases[].expected.aiSide.bucket",
	".cases[].expected.aiSide.cycleTimeAvgHours",
	".cases[].expected.aiSide.incidentRate",
	".cases[].expected.aiSide.prsMerged",
	".cases[].expected.aiSide.prsTotal",
	".cases[].expected.aiSide.revertRate",
	".cases[].expected.aiSide.reviewsPerPr",
	".cases[].expected.aiSide.reworkRate",
	".cases[].expected.aiSide.testGapRate",
	".cases[].expected.baselineSide.bucket",
	".cases[].expected.baselineSide.cycleTimeAvgHours",
	".cases[].expected.baselineSide.incidentRate",
	".cases[].expected.baselineSide.prsMerged",
	".cases[].expected.baselineSide.prsTotal",
	".cases[].expected.baselineSide.revertRate",
	".cases[].expected.baselineSide.reviewsPerPr",
	".cases[].expected.baselineSide.reworkRate",
	".cases[].expected.baselineSide.testGapRate",
	".cases[].expected.byBucket[].agentCreatedPrCount",
	".cases[].expected.byBucket[].aiAssistedPrRatio",
	".cases[].expected.byBucket[].aiCycleTimeDeltaHours",
	".cases[].expected.byBucket[].aiReviewAmplification",
	".cases[].expected.byBucket[].bucket",
	".cases[].expected.byBucket[].changesRequestedPerPr",
	".cases[].expected.byBucket[].cycleTimeAvgHours",
	".cases[].expected.byBucket[].incidentDragRate",
	".cases[].expected.byBucket[].leverage.cycleTimeComponent",
	".cases[].expected.byBucket[].leverage.incidentComponent",
	".cases[].expected.byBucket[].leverage.prsComponent",
	".cases[].expected.byBucket[].leverage.reviewComponent",
	".cases[].expected.byBucket[].leverage.reworkComponent",
	".cases[].expected.byBucket[].leverage.testComponent",
	".cases[].expected.byBucket[].pickupLatencyHours",
	".cases[].expected.byBucket[].postFirstReviewPushesCount",
	".cases[].expected.byBucket[].postFirstReviewPushesPerPr",
	".cases[].expected.byBucket[].prsMerged",
	".cases[].expected.byBucket[].prsTotal",
	".cases[].expected.byBucket[].revertRate",
	".cases[].expected.byBucket[].reviewAmplification",
	".cases[].expected.byBucket[].reviewCommentsPerLoc",
	".cases[].expected.byBucket[].reviewsPerPr",
	".cases[].expected.byBucket[].reviewsTotal",
	".cases[].expected.byBucket[].reworkDragRate",
	".cases[].expected.byBucket[].testGapRate",
	".cases[].expected.computedAt",
	".cases[].expected.daily[].bucket",
	".cases[].expected.daily[].changesRequestedPerPr",
	".cases[].expected.daily[].cycleTimeAvgHours",
	".cases[].expected.daily[].incidentRate",
	".cases[].expected.daily[].incidentsCount",
	".cases[].expected.daily[].pickupLatencyHours",
	".cases[].expected.daily[].postFirstReviewPushesCount",
	".cases[].expected.daily[].postFirstReviewPushesPerPr",
	".cases[].expected.daily[].prsMerged",
	".cases[].expected.daily[].prsTotal",
	".cases[].expected.daily[].revertPrs",
	".cases[].expected.daily[].revertRate",
	".cases[].expected.daily[].reviewAmplification",
	".cases[].expected.daily[].reviewCommentsPerLoc",
	".cases[].expected.daily[].reviewsPerPr",
	".cases[].expected.daily[].reviewsTotal",
	".cases[].expected.daily[].reworkPrs",
	".cases[].expected.daily[].reworkRate",
	".cases[].expected.daily[].testGapPrs",
	".cases[].expected.daily[].testGapRate",
	".cases[].expected.dataAvailable",
	".cases[].expected.delta.cycleTimeDeltaHours",
	".cases[].expected.delta.incidentRateDelta",
	".cases[].expected.delta.revertRateDelta",
	".cases[].expected.delta.reviewsPerPrDelta",
	".cases[].expected.delta.reworkRateDelta",
	".cases[].expected.delta.testGapRateDelta",
	".cases[].expected.endDate",
	".cases[].expected.humanPrs",
	".cases[].expected.missingStates[].guidance",
	".cases[].expected.missingStates[].key",
	".cases[].expected.missingStates[].title",
	".cases[].expected.orgId",
	".cases[].expected.repoBreakdown[].aiAssistedPrRatio",
	".cases[].expected.repoBreakdown[].aiPrsTotal",
	".cases[].expected.repoBreakdown[].reworkRateDelta",
	".cases[].expected.repoBreakdown[].scopeId",
	".cases[].expected.repoBreakdown[].scopeLabel",
	".cases[].expected.reviewerConcentration.dataAvailable",
	".cases[].expected.reviewerConcentration.reviewerCount",
	".cases[].expected.reviewerConcentration.reviewerGini",
	".cases[].expected.startDate",
	".cases[].expected.teamBreakdown[].aiAssistedPrRatio",
	".cases[].expected.teamBreakdown[].aiPrsTotal",
	".cases[].expected.teamBreakdown[].reworkRateDelta",
	".cases[].expected.teamBreakdown[].scopeId",
	".cases[].expected.teamBreakdown[].scopeLabel",
	".cases[].expected.totalPrs",
	".cases[].expected.unknownPrs",
	".cases[].fn",
	".cases[].loads[]",
	".cases[].repoLabels.11111111-1111-1111-1111-111111111111",
	".cases[].repoLabels.22222222-2222-2222-2222-222222222222",
	".cases[].repoRows",
	".cases[].repoRows[].full_name",
	".cases[].repoRows[].repo_id",
	".cases[].scope",
	".cases[].scope.buckets[]",
	".cases[].scope.repoId",
	".cases[].scope.teamId",
	".cases[].scope.workType",
	".cases[].slugRows[]",
	".cases[].teamLabels.t03",
	".cases[].teamLabels.team-a",
	".cases[].teamRepoIds",
	".cases[].teamRepoIds[]",
	".cases[].teamRows",
	".cases[].teamRows[].id",
	".cases[].teamRows[].repo_patterns[]",
	".datasets.ai.daily[].agent_created_prs",
	".datasets.ai.daily[].ai_assisted_pr_ratio",
	".datasets.ai.daily[].ai_assisted_prs",
	".datasets.ai.daily[].ai_cycle_time_delta_hours",
	".datasets.ai.daily[].ai_review_amplification",
	".datasets.ai.daily[].attribution_bucket",
	".datasets.ai.daily[].changes_requested_per_pr",
	".datasets.ai.daily[].computed_at",
	".datasets.ai.daily[].cycle_time_avg_hours",
	".datasets.ai.daily[].day",
	".datasets.ai.daily[].followup_commits_count",
	".datasets.ai.daily[].human_prs",
	".datasets.ai.daily[].incident_drag_rate",
	".datasets.ai.daily[].incidents_count",
	".datasets.ai.daily[].leverage_cycle_time_component",
	".datasets.ai.daily[].leverage_incident_component",
	".datasets.ai.daily[].leverage_prs_component",
	".datasets.ai.daily[].leverage_review_component",
	".datasets.ai.daily[].leverage_rework_component",
	".datasets.ai.daily[].leverage_test_component",
	".datasets.ai.daily[].prs_merged",
	".datasets.ai.daily[].prs_total",
	".datasets.ai.daily[].repo_id",
	".datasets.ai.daily[].revert_prs",
	".datasets.ai.daily[].revert_rate",
	".datasets.ai.daily[].reviews_per_pr",
	".datasets.ai.daily[].rework_drag_rate",
	".datasets.ai.daily[].rework_prs",
	".datasets.ai.daily[].team_id",
	".datasets.ai.daily[].test_gap_prs",
	".datasets.ai.daily[].test_gap_rate",
	".datasets.ai.daily[].unknown_prs",
	".datasets.human.daily[].agent_created_prs",
	".datasets.human.daily[].ai_assisted_pr_ratio",
	".datasets.human.daily[].ai_assisted_prs",
	".datasets.human.daily[].ai_cycle_time_delta_hours",
	".datasets.human.daily[].ai_review_amplification",
	".datasets.human.daily[].attribution_bucket",
	".datasets.human.daily[].changes_requested_per_pr",
	".datasets.human.daily[].computed_at",
	".datasets.human.daily[].cycle_time_avg_hours",
	".datasets.human.daily[].day",
	".datasets.human.daily[].followup_commits_count",
	".datasets.human.daily[].human_prs",
	".datasets.human.daily[].incident_drag_rate",
	".datasets.human.daily[].incidents_count",
	".datasets.human.daily[].leverage_cycle_time_component",
	".datasets.human.daily[].leverage_incident_component",
	".datasets.human.daily[].leverage_prs_component",
	".datasets.human.daily[].leverage_review_component",
	".datasets.human.daily[].leverage_rework_component",
	".datasets.human.daily[].leverage_test_component",
	".datasets.human.daily[].prs_merged",
	".datasets.human.daily[].prs_total",
	".datasets.human.daily[].revert_prs",
	".datasets.human.daily[].revert_rate",
	".datasets.human.daily[].reviews_per_pr",
	".datasets.human.daily[].rework_drag_rate",
	".datasets.human.daily[].rework_prs",
	".datasets.human.daily[].test_gap_prs",
	".datasets.human.daily[].test_gap_rate",
	".datasets.human.daily[].unknown_prs",
	".datasets.human.engagement[].bucket",
	".datasets.human.engagement[].day",
	".datasets.human.engagement[].loc_total",
	".datasets.human.engagement[].pickup_latency_hours",
	".datasets.human.engagement[].review_comments_total",
	".datasets.humanonly.daily[].agent_created_prs",
	".datasets.humanonly.daily[].ai_assisted_pr_ratio",
	".datasets.humanonly.daily[].ai_assisted_prs",
	".datasets.humanonly.daily[].ai_cycle_time_delta_hours",
	".datasets.humanonly.daily[].ai_review_amplification",
	".datasets.humanonly.daily[].attribution_bucket",
	".datasets.humanonly.daily[].changes_requested_per_pr",
	".datasets.humanonly.daily[].computed_at",
	".datasets.humanonly.daily[].cycle_time_avg_hours",
	".datasets.humanonly.daily[].day",
	".datasets.humanonly.daily[].human_prs",
	".datasets.humanonly.daily[].incident_drag_rate",
	".datasets.humanonly.daily[].incidents_count",
	".datasets.humanonly.daily[].leverage_cycle_time_component",
	".datasets.humanonly.daily[].leverage_incident_component",
	".datasets.humanonly.daily[].leverage_prs_component",
	".datasets.humanonly.daily[].leverage_review_component",
	".datasets.humanonly.daily[].leverage_rework_component",
	".datasets.humanonly.daily[].leverage_test_component",
	".datasets.humanonly.daily[].prs_merged",
	".datasets.humanonly.daily[].prs_total",
	".datasets.humanonly.daily[].revert_prs",
	".datasets.humanonly.daily[].revert_rate",
	".datasets.humanonly.daily[].reviews_per_pr",
	".datasets.humanonly.daily[].rework_drag_rate",
	".datasets.humanonly.daily[].rework_prs",
	".datasets.humanonly.daily[].test_gap_prs",
	".datasets.humanonly.daily[].test_gap_rate",
	".datasets.humanonly.daily[].unknown_prs",
	".datasets.many.daily[].agent_created_prs",
	".datasets.many.daily[].ai_assisted_pr_ratio",
	".datasets.many.daily[].ai_assisted_prs",
	".datasets.many.daily[].ai_cycle_time_delta_hours",
	".datasets.many.daily[].ai_review_amplification",
	".datasets.many.daily[].attribution_bucket",
	".datasets.many.daily[].changes_requested_per_pr",
	".datasets.many.daily[].computed_at",
	".datasets.many.daily[].cycle_time_avg_hours",
	".datasets.many.daily[].day",
	".datasets.many.daily[].human_prs",
	".datasets.many.daily[].incident_drag_rate",
	".datasets.many.daily[].incidents_count",
	".datasets.many.daily[].leverage_cycle_time_component",
	".datasets.many.daily[].leverage_incident_component",
	".datasets.many.daily[].leverage_prs_component",
	".datasets.many.daily[].leverage_review_component",
	".datasets.many.daily[].leverage_rework_component",
	".datasets.many.daily[].leverage_test_component",
	".datasets.many.daily[].prs_merged",
	".datasets.many.daily[].prs_total",
	".datasets.many.daily[].repo_id",
	".datasets.many.daily[].revert_prs",
	".datasets.many.daily[].revert_rate",
	".datasets.many.daily[].reviews_per_pr",
	".datasets.many.daily[].rework_drag_rate",
	".datasets.many.daily[].rework_prs",
	".datasets.many.daily[].team_id",
	".datasets.many.daily[].test_gap_prs",
	".datasets.many.daily[].test_gap_rate",
	".datasets.many.daily[].unknown_prs",
	".datasets.mixed.daily[].agent_created_prs",
	".datasets.mixed.daily[].ai_assisted_pr_ratio",
	".datasets.mixed.daily[].ai_assisted_prs",
	".datasets.mixed.daily[].ai_cycle_time_delta_hours",
	".datasets.mixed.daily[].ai_review_amplification",
	".datasets.mixed.daily[].attribution_bucket",
	".datasets.mixed.daily[].changes_requested_per_pr",
	".datasets.mixed.daily[].computed_at",
	".datasets.mixed.daily[].cycle_time_avg_hours",
	".datasets.mixed.daily[].day",
	".datasets.mixed.daily[].followup_commits_count",
	".datasets.mixed.daily[].human_prs",
	".datasets.mixed.daily[].incident_drag_rate",
	".datasets.mixed.daily[].incidents_count",
	".datasets.mixed.daily[].leverage_cycle_time_component",
	".datasets.mixed.daily[].leverage_incident_component",
	".datasets.mixed.daily[].leverage_prs_component",
	".datasets.mixed.daily[].leverage_review_component",
	".datasets.mixed.daily[].leverage_rework_component",
	".datasets.mixed.daily[].leverage_test_component",
	".datasets.mixed.daily[].prs_merged",
	".datasets.mixed.daily[].prs_total",
	".datasets.mixed.daily[].repo_id",
	".datasets.mixed.daily[].revert_prs",
	".datasets.mixed.daily[].revert_rate",
	".datasets.mixed.daily[].reviews_per_pr",
	".datasets.mixed.daily[].rework_drag_rate",
	".datasets.mixed.daily[].rework_prs",
	".datasets.mixed.daily[].team_id",
	".datasets.mixed.daily[].test_gap_prs",
	".datasets.mixed.daily[].test_gap_rate",
	".datasets.mixed.daily[].unknown_prs",
	".datasets.mixed.engagement[].bucket",
	".datasets.mixed.engagement[].day",
	".datasets.mixed.engagement[].loc_total",
	".datasets.mixed.engagement[].pickup_latency_hours",
	".datasets.mixed.engagement[].prs_with_first_review",
	".datasets.mixed.engagement[].review_comments_total",
	".datasets.noteam.daily[].agent_created_prs",
	".datasets.noteam.daily[].ai_assisted_pr_ratio",
	".datasets.noteam.daily[].ai_assisted_prs",
	".datasets.noteam.daily[].ai_cycle_time_delta_hours",
	".datasets.noteam.daily[].ai_review_amplification",
	".datasets.noteam.daily[].attribution_bucket",
	".datasets.noteam.daily[].changes_requested_per_pr",
	".datasets.noteam.daily[].computed_at",
	".datasets.noteam.daily[].cycle_time_avg_hours",
	".datasets.noteam.daily[].day",
	".datasets.noteam.daily[].human_prs",
	".datasets.noteam.daily[].incident_drag_rate",
	".datasets.noteam.daily[].incidents_count",
	".datasets.noteam.daily[].leverage_cycle_time_component",
	".datasets.noteam.daily[].leverage_incident_component",
	".datasets.noteam.daily[].leverage_prs_component",
	".datasets.noteam.daily[].leverage_review_component",
	".datasets.noteam.daily[].leverage_rework_component",
	".datasets.noteam.daily[].leverage_test_component",
	".datasets.noteam.daily[].prs_merged",
	".datasets.noteam.daily[].prs_total",
	".datasets.noteam.daily[].repo_id",
	".datasets.noteam.daily[].revert_prs",
	".datasets.noteam.daily[].revert_rate",
	".datasets.noteam.daily[].reviews_per_pr",
	".datasets.noteam.daily[].rework_drag_rate",
	".datasets.noteam.daily[].rework_prs",
	".datasets.noteam.daily[].team_id",
	".datasets.noteam.daily[].test_gap_prs",
	".datasets.noteam.daily[].test_gap_rate",
	".datasets.noteam.daily[].unknown_prs",
	".datasets.rounding.daily[].agent_created_prs",
	".datasets.rounding.daily[].ai_assisted_pr_ratio",
	".datasets.rounding.daily[].ai_assisted_prs",
	".datasets.rounding.daily[].ai_cycle_time_delta_hours",
	".datasets.rounding.daily[].ai_review_amplification",
	".datasets.rounding.daily[].attribution_bucket",
	".datasets.rounding.daily[].changes_requested_per_pr",
	".datasets.rounding.daily[].computed_at",
	".datasets.rounding.daily[].cycle_time_avg_hours",
	".datasets.rounding.daily[].day",
	".datasets.rounding.daily[].followup_commits_count",
	".datasets.rounding.daily[].human_prs",
	".datasets.rounding.daily[].incident_drag_rate",
	".datasets.rounding.daily[].incidents_count",
	".datasets.rounding.daily[].leverage_cycle_time_component",
	".datasets.rounding.daily[].leverage_incident_component",
	".datasets.rounding.daily[].leverage_prs_component",
	".datasets.rounding.daily[].leverage_review_component",
	".datasets.rounding.daily[].leverage_rework_component",
	".datasets.rounding.daily[].leverage_test_component",
	".datasets.rounding.daily[].prs_merged",
	".datasets.rounding.daily[].prs_total",
	".datasets.rounding.daily[].repo_id",
	".datasets.rounding.daily[].revert_prs",
	".datasets.rounding.daily[].revert_rate",
	".datasets.rounding.daily[].reviews_per_pr",
	".datasets.rounding.daily[].rework_drag_rate",
	".datasets.rounding.daily[].rework_prs",
	".datasets.rounding.daily[].team_id",
	".datasets.rounding.daily[].test_gap_prs",
	".datasets.rounding.daily[].test_gap_rate",
	".datasets.rounding.daily[].unknown_prs",
}

var oracleCasesNotClaims = map[string]string{
	".cases[].teamRows[].name":                           "Python reads the team name into the resolver tuple (providers/teams.py:255) but the AI resolvers use only the matched repository ids; TestRecordedFixtureFieldsNoResolverReadsDoNotMoveTheAnswer perturbs it. Go also reads it for the Go-only aiAttributedPrs teamName (CHAOS-7773); the oracle strips that field and attributed_names_test.go pins it",
	".datasets.human.engagement[].prs_with_first_review": "consumed input, the weight of the engagement average (ai.py:653 at 6121e851f4^): TestRecordedEngagementWeightIsRead zeroes it and requires the answer to change",
	".datasets.ai.daily[].org_id":                        "no resolver reads the row's org_id: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.ai.daily[].work_type":                     "no resolver reads the row's work_type: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.human.daily[].org_id":                     "no resolver reads the row's org_id: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.human.daily[].work_type":                  "no resolver reads the row's work_type: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.humanonly.daily[].org_id":                 "no resolver reads the row's org_id: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.humanonly.daily[].work_type":              "no resolver reads the row's work_type: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.many.daily[].org_id":                      "no resolver reads the row's org_id: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.many.daily[].work_type":                   "no resolver reads the row's work_type: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.mixed.daily[].org_id":                     "no resolver reads the row's org_id: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.mixed.daily[].work_type":                  "no resolver reads the row's work_type: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.noteam.daily[].org_id":                    "no resolver reads the row's org_id: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.noteam.daily[].work_type":                 "no resolver reads the row's work_type: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.rounding.daily[].org_id":                  "no resolver reads the row's org_id: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.rounding.daily[].work_type":               "no resolver reads the row's work_type: the organisation comes from the request context and the work type filters in SQL (ai.py:197-218 at 6121e851f4^); the probe test perturbs it",
	".datasets.humanonly.daily[].followup_commits_count": "read only by resolve_ai_review_load (ai.py:749,768 at 6121e851f4^) and no review_load case runs over this dataset (TestNoReviewLoadCaseRunsOverTheFollowupFreeDatasets)",
	".datasets.many.daily[].followup_commits_count":      "read only by resolve_ai_review_load (ai.py:749,768 at 6121e851f4^) and no review_load case runs over this dataset (TestNoReviewLoadCaseRunsOverTheFollowupFreeDatasets)",
	".datasets.noteam.daily[].followup_commits_count":    "read only by resolve_ai_review_load (ai.py:749,768 at 6121e851f4^) and no review_load case runs over this dataset (TestNoReviewLoadCaseRunsOverTheFollowupFreeDatasets)",
	".datasets.human.daily[].repo_id":                    "OPEN (CHAOS-7496, a separate ticket): Python reads it in _load_scope_breakdowns (ai.py:431-452 at 6121e851f4^) and no case moves; no recorder in the tree (the Python resolvers were deleted by #2794)",
	".datasets.human.daily[].team_id":                    "OPEN (CHAOS-7496, a separate ticket): Python reads it in _load_scope_breakdowns (ai.py:431-452 at 6121e851f4^) and no case moves; no recorder in the tree (the Python resolvers were deleted by #2794)",
	".datasets.humanonly.daily[].repo_id":                "OPEN (CHAOS-7496, a separate ticket): Python reads it in _load_scope_breakdowns (ai.py:431-452 at 6121e851f4^) and no case moves; no recorder in the tree (the Python resolvers were deleted by #2794)",
	".datasets.humanonly.daily[].team_id":                "OPEN (CHAOS-7496, a separate ticket): Python reads it in _load_scope_breakdowns (ai.py:431-452 at 6121e851f4^) and no case moves; no recorder in the tree (the Python resolvers were deleted by #2794)",
}

var oracleBClaims = []string{
	".cases[].name",
	".cases[].calls.evidence.limit",
	".cases[].calls.evidence.offset",
	".cases[].calls.hotspot.repo_id",
	".cases[].calls.hotspot.repo_ids",
	".cases[].calls.hotspot.repo_ids[]",
	".cases[].calls.mix.kinds",
	".cases[].calls.mix.kinds[]",
	".cases[].calls.mix.repo_ids",
	".cases[].calls.mix.repo_ids[]",
	".cases[].calls.prs.limit",
	".cases[].calls.prs.offset",
	".cases[].calls.prs.repo_id",
	".cases[].calls.prs.repo_ids",
	".cases[].calls.prs.repo_ids[]",
	".cases[].complexity[].bucket",
	".cases[].complexity[].prs_total",
	".cases[].complexity[].prs_touching_high_complexity",
	".cases[].daily[].ai_assisted_pr_ratio",
	".cases[].daily[].ai_cycle_time_delta_hours",
	".cases[].daily[].ai_review_amplification",
	".cases[].daily[].attribution_bucket",
	".cases[].daily[].changes_requested_per_pr",
	".cases[].daily[].computed_at",
	".cases[].daily[].cycle_time_avg_hours",
	".cases[].daily[].day",
	".cases[].daily[].incident_drag_rate",
	".cases[].daily[].incidents_count",
	".cases[].daily[].leverage_cycle_time_component",
	".cases[].daily[].leverage_incident_component",
	".cases[].daily[].leverage_review_component",
	".cases[].daily[].leverage_rework_component",
	".cases[].daily[].leverage_test_component",
	".cases[].daily[].prs_total",
	".cases[].daily[].revert_prs",
	".cases[].daily[].revert_rate",
	".cases[].daily[].reviews_per_pr",
	".cases[].daily[].rework_drag_rate",
	".cases[].daily[].rework_prs",
	".cases[].daily[].test_gap_prs",
	".cases[].daily[].test_gap_rate",
	".cases[].evidence[].actor",
	".cases[].evidence[].confidence",
	".cases[].evidence[].evidence",
	".cases[].evidence[].kind",
	".cases[].evidence[].observed_at",
	".cases[].evidence[].provider",
	".cases[].evidence[].repo_id",
	".cases[].evidence[].source",
	".cases[].evidence[].subject_id",
	".cases[].evidence[].subject_type",
	".cases[].expected.byBucket[].bucket",
	".cases[].expected.byBucket[].incidentRate",
	".cases[].expected.byBucket[].incidentsCount",
	".cases[].expected.byBucket[].prsTotal",
	".cases[].expected.byBucket[].revertPrs",
	".cases[].expected.byBucket[].revertRate",
	".cases[].expected.byBucket[].reworkPrs",
	".cases[].expected.byBucket[].reworkRate",
	".cases[].expected.byBucket[].testGapPrs",
	".cases[].expected.byBucket[].testGapRate",
	".cases[].expected.complexityOverlap[].bucket",
	".cases[].expected.complexityOverlap[].complexityOverlapRate",
	".cases[].expected.complexityOverlap[].prsTotal",
	".cases[].expected.complexityOverlap[].prsTouchingHighComplexity",
	".cases[].expected.dataAvailable",
	".cases[].expected.endDate",
	".cases[].expected.hasMore",
	".cases[].expected.hotspotOverlap[].avgHotspotRiskScore",
	".cases[].expected.hotspotOverlap[].bucket",
	".cases[].expected.hotspotOverlap[].hotspotOverlapRate",
	".cases[].expected.hotspotOverlap[].prsTotal",
	".cases[].expected.hotspotOverlap[].prsTouchingHotspots",
	".cases[].expected.missingStates[].guidance",
	".cases[].expected.missingStates[].key",
	".cases[].expected.missingStates[].title",
	".cases[].expected.mix[].count",
	".cases[].expected.mix[].kind",
	".cases[].expected.mix[].share",
	".cases[].expected.orgId",
	".cases[].expected.rows[].actor",
	".cases[].expected.rows[].confidence",
	".cases[].expected.rows[].evidence",
	".cases[].expected.rows[].kind",
	".cases[].expected.rows[].mergedAt",
	".cases[].expected.rows[].number",
	".cases[].expected.rows[].observedAt",
	".cases[].expected.rows[].provider",
	".cases[].expected.rows[].repoId",
	".cases[].expected.rows[].source",
	".cases[].expected.rows[].subjectId",
	".cases[].expected.rows[].subjectType",
	".cases[].expected.rows[].teamId",
	".cases[].expected.rows[].title",
	".cases[].expected.rows[].workType",
	".cases[].expected.startDate",
	".cases[].expected.total",
	".cases[].expected.totalAttributed",
	".cases[].fail",
	".cases[].fn",
	".cases[].hotspot[].avg_hotspot_risk_score",
	".cases[].hotspot[].bucket",
	".cases[].hotspot[].prs_total",
	".cases[].hotspot[].prs_touching_hotspots",
	".cases[].limit",
	".cases[].mix[].count",
	".cases[].mix[].kind",
	".cases[].nameQueryFails",
	".cases[].offset",
	".cases[].prs[].kind",
	".cases[].prs[].merged_at",
	".cases[].prs[].number",
	".cases[].prs[].repo_id",
	".cases[].prs[].title",
	".cases[].prs[].work_type",
	".cases[].repoRows",
	".cases[].repoRows[].full_name",
	".cases[].repoRows[].repo_id",
	".cases[].scope",
	".cases[].scope.buckets[]",
	".cases[].scope.repoId",
	".cases[].scope.teamId",
	".cases[].scope.workType",
	".cases[].slugRows[]",
	".cases[].teamRepoRows",
	".cases[].teamRows",
	".cases[].teamRows[].id",
	".cases[].teamRows[].repo_patterns[]",
}

var oracleBNotClaims = map[string]string{
	".cases[].teamRows[].name":                "Python reads the team name into the resolver tuple (providers/teams.py:255) but the AI resolvers use only the matched repository ids; the probe test perturbs it",
	".cases[].prs[].team_id":                  "Python overwrites it from the team map before use (ai.py:1333-1464 at 6121e851f4^: the SQL loader returns an empty team_id and the resolver assigns it from the team catalogue); the probe test perturbs it",
	".cases[].daily[].agent_created_prs":      "no risk/prs/overview resolver reads this daily-row field: risk reads only prs_total, rework, revert, test_gap, incidents and the bucket (ai.py:846-880 at 6121e851f4^), the other two read no daily rows; it is scripted because the reference SELECT returned it; the probe test perturbs it",
	".cases[].daily[].ai_assisted_prs":        "no risk/prs/overview resolver reads this daily-row field: risk reads only prs_total, rework, revert, test_gap, incidents and the bucket (ai.py:846-880 at 6121e851f4^), the other two read no daily rows; it is scripted because the reference SELECT returned it; the probe test perturbs it",
	".cases[].daily[].followup_commits_count": "no risk/prs/overview resolver reads this daily-row field: risk reads only prs_total, rework, revert, test_gap, incidents and the bucket (ai.py:846-880 at 6121e851f4^), the other two read no daily rows; it is scripted because the reference SELECT returned it; the probe test perturbs it",
	".cases[].daily[].human_prs":              "no risk/prs/overview resolver reads this daily-row field: risk reads only prs_total, rework, revert, test_gap, incidents and the bucket (ai.py:846-880 at 6121e851f4^), the other two read no daily rows; it is scripted because the reference SELECT returned it; the probe test perturbs it",
	".cases[].daily[].leverage_prs_component": "no risk/prs/overview resolver reads this daily-row field: risk reads only prs_total, rework, revert, test_gap, incidents and the bucket (ai.py:846-880 at 6121e851f4^), the other two read no daily rows; it is scripted because the reference SELECT returned it; the probe test perturbs it",
	".cases[].daily[].org_id":                 "no risk/prs/overview resolver reads this daily-row field: risk reads only prs_total, rework, revert, test_gap, incidents and the bucket (ai.py:846-880 at 6121e851f4^), the other two read no daily rows; it is scripted because the reference SELECT returned it; the probe test perturbs it",
	".cases[].daily[].prs_merged":             "no risk/prs/overview resolver reads this daily-row field: risk reads only prs_total, rework, revert, test_gap, incidents and the bucket (ai.py:846-880 at 6121e851f4^), the other two read no daily rows; it is scripted because the reference SELECT returned it; the probe test perturbs it",
	".cases[].daily[].repo_id":                "no risk/prs/overview resolver reads this daily-row field: risk reads only prs_total, rework, revert, test_gap, incidents and the bucket (ai.py:846-880 at 6121e851f4^), the other two read no daily rows; it is scripted because the reference SELECT returned it; the probe test perturbs it",
	".cases[].daily[].team_id":                "no risk/prs/overview resolver reads this daily-row field: risk reads only prs_total, rework, revert, test_gap, incidents and the bucket (ai.py:846-880 at 6121e851f4^), the other two read no daily rows; it is scripted because the reference SELECT returned it; the probe test perturbs it",
	".cases[].daily[].unknown_prs":            "no risk/prs/overview resolver reads this daily-row field: risk reads only prs_total, rework, revert, test_gap, incidents and the bucket (ai.py:846-880 at 6121e851f4^), the other two read no daily rows; it is scripted because the reference SELECT returned it; the probe test perturbs it",
	".cases[].daily[].work_type":              "no risk/prs/overview resolver reads this daily-row field: risk reads only prs_total, rework, revert, test_gap, incidents and the bucket (ai.py:846-880 at 6121e851f4^), the other two read no daily rows; it is scripted because the reference SELECT returned it; the probe test perturbs it",
}

func TestEveryRecordedPathIsDeclared(t *testing.T) {
	for name, entry := range map[string]struct {
		file      string
		claims    []string
		notClaims map[string]string
	}{
		"oracle_cases":   {"testdata/oracle_cases.json", oracleCasesClaims, oracleCasesNotClaims},
		"oracle_b_cases": {"testdata/oracle_b_cases.json", oracleBClaims, oracleBNotClaims},
	} {
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(entry.file)
			if err != nil {
				t.Fatal(err)
			}
			recordedpaths.Check(t, raw, entry.claims, entry.notClaims)
		})
	}
}

var oracleRange = model.AIDateRangeInput{StartDate: day("2026-08-01"), EndDate: day("2026-08-03")}

// answerA runs one oracle_cases case through the Go resolver, as TestResolvers_MatchPythonResolvers
// does, and returns the answer as a JSON map (dates kept).
func answerA(t *testing.T, c oracleCase, ds dataset) map[string]any {
	t.Helper()
	client := &fixtureClient{c: c, ds: ds}
	var (
		got any
		err error
	)
	switch c.Fn {
	case "resolve_ai_impact_summary":
		got, err = ImpactSummary(context.Background(), client, "org-1", oracleRange, scopeInput(c))
	case "resolve_ai_comparison":
		got, err = Comparison(context.Background(), client, "org-1", oracleRange, scopeInput(c))
	case "resolve_ai_review_load":
		got, err = ReviewLoad(context.Background(), client, "org-1", oracleRange, scopeInput(c))
	default:
		t.Fatalf("unknown fn %s", c.Fn)
	}
	if err != nil {
		t.Fatalf("%s: %v", c.Name, err)
	}
	answer := jsonMap(t, got)
	if c.Fn == "resolve_ai_impact_summary" {
		// The Go-only daily row field (CHAOS-7774) is not part of the recorded Python answer.
		stripGoOnlyImpactDailyRowFields(answer)
	}
	return answer
}

// jsonMap is the answer as a JSON map. The two date fields are graphqldate.Date values that do not
// marshal to their text, so they are read from the struct and set as "2006-01-02" strings.
func jsonMap(t *testing.T, v any) map[string]any {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out map[string]any
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	value := reflect.Indirect(reflect.ValueOf(v))
	for field, key := range map[string]string{"StartDate": "startDate", "EndDate": "endDate"} {
		date, ok := value.FieldByName(field).Interface().(graphqldate.Date)
		if !ok {
			t.Fatalf("the answer has no %s date", field)
		}
		out[key] = date.String()
	}
	return out
}

func withoutDates(m map[string]any) map[string]any {
	out := map[string]any{}
	for k, v := range m {
		if k != "startDate" && k != "endDate" {
			out[k] = v
		}
	}
	return out
}

// answerB runs one oracle_b_cases case, as TestGroupB_MatchPythonResolvers does, and returns the
// answer and the client that saw the statements.
func answerB(t *testing.T, c oracleBCase) (map[string]any, *fixtureClientB) {
	t.Helper()
	client := &fixtureClientB{
		fixtureClient: &fixtureClient{
			c:  oracleCase{SlugRows: c.SlugRows, TeamRows: c.TeamRows, RepoRows: c.RepoRows},
			ds: dataset{Daily: c.Daily},
		},
		c: c,
	}
	var scope *model.AIScopeInput
	var aScope *model.AIAttributionScopeInput
	if c.Scope != nil {
		scope = &model.AIScopeInput{RepoID: c.Scope.RepoID, TeamID: c.Scope.TeamID, WorkType: c.Scope.WorkType}
		aScope = &model.AIAttributionScopeInput{RepoID: c.Scope.RepoID, TeamID: c.Scope.TeamID}
		for _, b := range c.Scope.Buckets {
			scope.Buckets = append(scope.Buckets, model.AIAttributionBucketInput(b))
			aScope.Buckets = append(aScope.Buckets, model.AIAttributionBucketInput(b))
		}
	}
	limit, offset := 0, 0
	if c.Limit != nil {
		limit, offset = *c.Limit, *c.Offset
	}
	var (
		got any
		err error
	)
	switch c.Fn {
	case "resolve_ai_risk_breakdown":
		got, err = RiskBreakdown(context.Background(), client, "org-1", oracleRange, scope)
	case "resolve_ai_attributed_prs":
		got, err = AttributedPrs(context.Background(), client, "org-1", oracleRange, scope, limit, offset)
	case "resolve_ai_attribution_overview":
		got, err = AttributionOverview(context.Background(), client, "org-1", oracleRange, aScope, limit, offset)
	default:
		t.Fatalf("unknown fn %s", c.Fn)
	}
	if err != nil {
		t.Fatalf("%s: %v", c.Name, err)
	}
	answer := jsonMap(t, got)
	if c.Fn == "resolve_ai_attributed_prs" {
		// The two Go-only row fields (CHAOS-7773) are not part of the recorded Python answer.
		stripGoOnlyAttributedPrRowFields(answer)
	}
	if c.Fn == "resolve_ai_attribution_overview" {
		// The Go-only row name fields (CHAOS-8954) are not part of the recorded Python answer.
		stripGoOnlyAttributionRowFields(answer)
	}
	return answer, client
}

func loadOracleB(t *testing.T) []oracleBCase {
	t.Helper()
	raw, err := os.ReadFile("testdata/oracle_b_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []oracleBCase `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Cases) == 0 {
		t.Fatal("no captured cases")
	}
	return doc.Cases
}

// TestAnswersCarryTheRecordedDateRange pins expected.startDate and expected.endDate, which the
// two resolver tests delete from both sides before comparing: the Go answer must echo the
// requested range exactly as the reference recorded it.
func TestAnswersCarryTheRecordedDateRange(t *testing.T) {
	datasets, cases := loadOracle(t)
	for _, c := range cases {
		got := answerA(t, c, datasets[c.Dataset])
		for _, key := range []string{"startDate", "endDate"} {
			if got[key] != c.Expected[key] {
				t.Errorf("%s: %s = %v, recorded %v", c.Name, key, got[key], c.Expected[key])
			}
		}
	}
	for _, c := range loadOracleB(t) {
		got, _ := answerB(t, c)
		for _, key := range []string{"startDate", "endDate"} {
			if got[key] != c.Expected[key] {
				t.Errorf("%s: %s = %v, recorded %v", c.Name, key, got[key], c.Expected[key])
			}
		}
	}
}

func copyRows(rows []map[string]any, set func(row map[string]any)) []map[string]any {
	out := make([]map[string]any, len(rows))
	for i, row := range rows {
		cp := map[string]any{}
		for k, v := range row {
			cp[k] = v
		}
		set(cp)
		out[i] = cp
	}
	return out
}

func sameAnswer(t *testing.T, name string, got, want map[string]any, normalise func(any, string) any) {
	t.Helper()
	if !reflect.DeepEqual(normalise(withoutDates(got), ""), normalise(withoutDates(want), "")) {
		t.Errorf("%s: the answer moved when a field no resolver reads was perturbed", name)
	}
}

// TestRecordedFixtureFieldsNoResolverReadsDoNotMoveTheAnswer backs the not-a-claim fields named
// in the lists above: each is set to a different value in memory and every case must still give
// the recorded answer. (The four OPEN paths are not perturbed here: they are the ones Python
// reads.)
func TestRecordedFixtureFieldsNoResolverReadsDoNotMoveTheAnswer(t *testing.T) {
	datasets, cases := loadOracle(t)
	for _, c := range cases {
		ds := datasets[c.Dataset]
		ds.Daily = copyRows(ds.Daily, func(row map[string]any) {
			row["org_id"], row["work_type"] = "perturbed-org", "perturbed-work-type"
			switch c.Dataset {
			case "humanonly", "many", "noteam":
				row["followup_commits_count"] = float64(7)
			}
		})
		c.TeamRows = copyRows(c.TeamRows, func(row map[string]any) { row["name"] = "perturbed-name" })
		sameAnswer(t, c.Name, answerA(t, c, ds), c.Expected, normalize)
	}
	for _, c := range loadOracleB(t) {
		c.Daily = copyRows(c.Daily, func(row map[string]any) {
			for _, key := range []string{"agent_created_prs", "ai_assisted_prs", "followup_commits_count", "human_prs", "leverage_prs_component", "prs_merged", "unknown_prs"} {
				row[key] = float64(7)
			}
			for _, key := range []string{"org_id", "repo_id", "team_id", "work_type"} {
				row[key] = "perturbed-" + key
			}
		})
		c.Prs = copyRows(c.Prs, func(row map[string]any) { row["team_id"] = "perturbed-team" })
		c.TeamRows = copyRows(c.TeamRows, func(row map[string]any) { row["name"] = "perturbed-name" })
		got, _ := answerB(t, c)
		sameAnswer(t, c.Name, got, c.Expected, normalizeB)
	}
}

// TestNoReviewLoadCaseRunsOverTheFollowupFreeDatasets keeps the followup_commits_count
// not-a-claim honest: only review_load reads it, so the day a review_load case runs over one of
// these datasets the three paths become claims.
func TestNoReviewLoadCaseRunsOverTheFollowupFreeDatasets(t *testing.T) {
	_, cases := loadOracle(t)
	for _, c := range cases {
		switch c.Dataset {
		case "humanonly", "many", "noteam":
			if c.Fn == "resolve_ai_review_load" {
				t.Errorf("%s runs review_load over the %q dataset: its followup_commits_count is now read", c.Name, c.Dataset)
			}
		}
	}
}

// TestRecordedEngagementWeightIsRead backs the not-a-claim prs_with_first_review: it weights the
// engagement average, so a zero weight must change the answer of at least the cases that use it.
func TestRecordedEngagementWeightIsRead(t *testing.T) {
	datasets, cases := loadOracle(t)
	probed := 0
	for _, c := range cases {
		ds := datasets[c.Dataset]
		if len(ds.Engagement) == 0 || c.EngagementError {
			continue
		}
		weighted := false
		for _, row := range ds.Engagement {
			if v, _ := row["prs_with_first_review"].(float64); v != 0 {
				weighted = true
			}
		}
		if !weighted {
			continue
		}
		ds.Engagement = copyRows(ds.Engagement, func(row map[string]any) { row["prs_with_first_review"] = float64(0) })
		got := answerA(t, c, ds)
		if reflect.DeepEqual(normalize(withoutDates(got), ""), normalize(withoutDates(c.Expected), "")) {
			continue
		}
		probed++
	}
	if probed == 0 {
		t.Fatal("no case moved when the engagement weights were zeroed: the weight is not read, or no case uses it")
	}
}

// stringList reads a recorded call argument that must be absent (null) or a list of strings.
func stringList(t *testing.T, name string, v any) []string {
	t.Helper()
	if v == nil {
		return nil
	}
	list, ok := v.([]any)
	if !ok {
		t.Fatalf("%s: recorded argument %v is neither null nor a list", name, v)
	}
	out := make([]string, len(list))
	for i, e := range list {
		s, ok := e.(string)
		if !ok {
			t.Fatalf("%s: recorded list element %v is not a string", name, e)
		}
		out[i] = s
	}
	return out
}

func bindingString(bindings []clickhouse.Binding, name string) string {
	for _, b := range bindings {
		if b.Name == name {
			got, _ := b.Value.(string)
			return got
		}
	}
	return ""
}

func bindingStrings(bindings []clickhouse.Binding, name string) []string {
	for _, b := range bindings {
		if b.Name == name {
			got, _ := b.Value.([]string)
			return got
		}
	}
	return nil
}

// TestRecordedCallArgumentsAreComparedElementWise closes the gap in checkCalls, which compares
// only the lengths and presence of repo_ids and kinds: the recorded repo ids and bucket kinds the
// Python readers were called with must equal, element by element, what the Go statements bind.
// repo_id (a single id) is bound apart from repo_ids and compared apart. Also ties scope.buckets to the kinds
// the reference passed (the bucket name in lower case).
func TestRecordedCallArgumentsAreComparedElementWise(t *testing.T) {
	compared := 0
	for _, c := range loadOracleB(t) {
		_, client := answerB(t, c)
		find := func(marker string) []clickhouse.Binding {
			for i, st := range client.statements {
				if strings.Contains(st, marker) {
					return client.bindings[i]
				}
			}
			return nil
		}
		check := func(call, marker string) {
			want, ok := c.Calls[call].(map[string]any)
			if !ok {
				return
			}
			bindings := find(marker)
			if bindings == nil {
				t.Errorf("%s: the %s read did not run", c.Name, call)
				return
			}
			ids := stringList(t, c.Name+" "+call+".repo_ids", want["repo_ids"])
			if got := bindingStrings(bindings, "repo_ids"); !reflect.DeepEqual(got, ids) {
				t.Errorf("%s: %s repo ids bound %v, the reference was called with %v", c.Name, call, got, ids)
			}
			// The recorded mix call carries no repo_id key (the reference filters a single
			// repository inside its loader), so repo_id is compared only where it was recorded.
			if single, recorded := want["repo_id"]; recorded {
				wantID := ""
				if single != nil {
					s, ok := single.(string)
					if !ok {
						t.Fatalf("%s: %s.repo_id %v is not a string", c.Name, call, single)
					}
					wantID = s
				}
				if got := bindingString(bindings, "repo_id"); got != wantID {
					t.Errorf("%s: %s repo id bound %q, the reference was called with %q", c.Name, call, got, wantID)
				}
			}
			if kinds, present := want["kinds"]; present || call == "mix" {
				wantKinds := stringList(t, c.Name+" "+call+".kinds", kinds)
				if got := bindingStrings(bindings, "kinds"); !reflect.DeepEqual(got, wantKinds) {
					t.Errorf("%s: %s kinds bound %v, the reference was called with %v", c.Name, call, got, wantKinds)
				}
				if c.Scope != nil && len(c.Scope.Buckets) > 0 {
					var lower []string
					for _, b := range c.Scope.Buckets {
						lower = append(lower, strings.ToLower(b))
					}
					if !reflect.DeepEqual(lower, wantKinds) {
						t.Errorf("%s: scope buckets %v do not match the kinds %v the reference passed", c.Name, c.Scope.Buckets, wantKinds)
					}
				}
			}
			compared++
		}
		check("prs", "repo_id_str, number, kind, work_type")
		check("mix", "count() AS count")
		check("hotspot", "hotspots AS (")
	}
	if compared == 0 {
		t.Fatal("no recorded call compared: the test proved nothing")
	}
}

// TestRecordedTeamRepoRowsAreNullInEveryCase pins `teamRepoRows`: the field is not decoded by
// oracleBCase and no case scripts rows through it, so it must stay null.
func TestRecordedTeamRepoRowsAreNullInEveryCase(t *testing.T) {
	raw, err := os.ReadFile("testdata/oracle_b_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []map[string]any `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	for _, c := range doc.Cases {
		if v, present := c["teamRepoRows"]; !present || v != nil {
			t.Errorf("case %v: teamRepoRows = %v (present %v), want null: nothing reads rows from it", c["name"], v, present)
		}
	}
}

// TestScopeBreakdownFollowsTheDailyRowsRepoAndTeam is a SENSITIVITY probe for the four OPEN paths
// (datasets.human and datasets.humanonly daily[].repo_id and team_id), and it is consumption, not
// parity: no recorded Python answer covers them (the reference was deleted in #2794) and nothing
// here is an expected number from the old Python. In memory, each of those datasets gets an
// AI-attributed first row and label dictionaries that tell two repositories and two teams apart;
// the Go impact summary must then put the row under the repository and team it carries, and move
// it to the other one when the row's repo_id and team_id are changed. Which scope gains the row is
// decided by the Go code's own rule (the breakdown is keyed by the row's repo_id and team_id).
func TestScopeBreakdownFollowsTheDailyRowsRepoAndTeam(t *testing.T) {
	const alpha, beta = "11111111-1111-1111-1111-111111111111", "22222222-2222-2222-2222-222222222222"
	datasets, cases := loadOracle(t)
	scopeIDs := func(answer map[string]any, key string) []string {
		var ids []string
		rows, _ := answer[key].([]any)
		for _, row := range rows {
			if m, ok := row.(map[string]any); ok {
				if id, ok := m["scopeId"].(string); ok {
					ids = append(ids, id)
				}
			}
		}
		return ids
	}
	probed := 0
	for _, c := range cases {
		if c.Fn != "resolve_ai_impact_summary" || (c.Dataset != "human" && c.Dataset != "humanonly") {
			continue
		}
		c.RepoLabels = map[string]string{alpha: "acme/alpha", beta: "acme/beta"}
		c.TeamLabels = map[string]string{"team-a": "Team A", "team-b": "Team B"}
		run := func(repoID, teamID string) map[string]any {
			ds := datasets[c.Dataset]
			ds.Daily = copyRows(ds.Daily, func(row map[string]any) {
				row["repo_id"], row["team_id"] = repoID, teamID
				row["attribution_bucket"] = "ai_assisted"
				row["ai_assisted_prs"] = row["prs_total"]
			})
			return answerA(t, c, ds)
		}
		before, after := run(alpha, "team-a"), run(beta, "team-b")
		if got := scopeIDs(before, "repoBreakdown"); !reflect.DeepEqual(got, []string{alpha}) {
			t.Errorf("%s: repo breakdown scopes %v with every row in repository alpha, want [alpha]", c.Name, got)
		}
		if got := scopeIDs(after, "repoBreakdown"); !reflect.DeepEqual(got, []string{beta}) {
			t.Errorf("%s: repo breakdown scopes %v after the rows moved to repository beta, want [beta]", c.Name, got)
		}
		if got := scopeIDs(before, "teamBreakdown"); !reflect.DeepEqual(got, []string{"team-a"}) {
			t.Errorf("%s: team breakdown scopes %v with every row in team-a, want [team-a]", c.Name, got)
		}
		if got := scopeIDs(after, "teamBreakdown"); !reflect.DeepEqual(got, []string{"team-b"}) {
			t.Errorf("%s: team breakdown scopes %v after the rows moved to team-b, want [team-b]", c.Name, got)
		}
		probed++
	}
	if probed == 0 {
		t.Fatal("no impact_summary case over the human or humanonly dataset: the probe proved nothing")
	}
}

// TestOpenPathsAreExactlyTheNamedFour keeps the gap visible: a path whose reason starts with OPEN
// is a known hole in the frozen goldens (CHAOS-7496), so the list of them may only change on
// purpose, here, and never by someone adding an OPEN reason to quiet a new path.
func TestOpenPathsAreExactlyTheNamedFour(t *testing.T) {
	want := []string{
		".datasets.human.daily[].repo_id",
		".datasets.human.daily[].team_id",
		".datasets.humanonly.daily[].repo_id",
		".datasets.humanonly.daily[].team_id",
	}
	var got []string
	for _, notClaims := range []map[string]string{oracleCasesNotClaims, oracleBNotClaims} {
		for path, reason := range notClaims {
			if strings.HasPrefix(reason, "OPEN") {
				got = append(got, path)
			}
		}
	}
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("OPEN paths %v, want %v", got, want)
	}
}

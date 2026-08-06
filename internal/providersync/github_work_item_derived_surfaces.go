package providersync

import (
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
)

// This file ports three of the nine Python-derived work-item destinations:
// estimate_coverage_metrics_daily, work_item_team_attributions, and
// work_item_state_durations_daily. The remaining three derived destinations
// this lane owns (issue_type_metrics_daily and the two investment surfaces)
// are computed inline inside job_work_items.py and depend on two config-driven
// engines that have no Go port yet; they land separately.
//
// Per D16 these builders mirror Python bug-for-bug. Every divergence a reader
// might mistake for a defect is called out at its site with the Python line it
// mirrors, and every one of them is pinned by an oracle case rather than
// corrected here.

// githubWorkItemDerivedDay is a calendar day, not an instant. The daily
// destinations persist a ClickHouse Date, and the generic oracle comparator
// tags a value carrying oracleDate() as "date" so it can never compare equal
// to a same-looking plain string.
type githubWorkItemDerivedDay string

func newGitHubWorkItemDerivedDay(value time.Time) githubWorkItemDerivedDay {
	return githubWorkItemDerivedDay(value.UTC().Format("2006-01-02"))
}

func (day githubWorkItemDerivedDay) oracleDate() string { return string(day) }

// githubEstimateCoverageMetricsDailyRow mirrors
// EstimateCoverageMetricsDailyRecord (metrics/schemas.py).
type githubEstimateCoverageMetricsDailyRow struct {
	Day              githubWorkItemDerivedDay `json:"day"`
	Provider         string                   `json:"provider"`
	WorkScopeID      string                   `json:"work_scope_id"`
	TeamID           *string                  `json:"team_id"`
	TeamName         *string                  `json:"team_name"`
	EstimatedCount   int                      `json:"estimated_count"`
	UnestimatedCount int                      `json:"unestimated_count"`
	BacklogSize      int                      `json:"backlog_size"`
	Ratio            *float64                 `json:"ratio"`
	ComputedAt       time.Time                `json:"computed_at"`
	OrgID            string                   `json:"org_id"`
}

// githubWorkItemTeamAttributionRow mirrors WorkItemTeamAttributionRecord.
type githubWorkItemTeamAttributionRow struct {
	WorkItemID string     `json:"work_item_id"`
	Provider   string     `json:"provider"`
	Source     string     `json:"source"`
	IsPrimary  int        `json:"is_primary"`
	Confidence string     `json:"confidence"`
	Evidence   string     `json:"evidence"`
	ComputedAt time.Time  `json:"computed_at"`
	RepoID     *uuid.UUID `json:"repo_id"`
	TeamID     *string    `json:"team_id"`
	TeamName   *string    `json:"team_name"`
	OrgID      string     `json:"org_id"`
}

// githubWorkItemStateDurationDailyRow mirrors
// WorkItemStateDurationDailyRecord.
type githubWorkItemStateDurationDailyRow struct {
	Day           githubWorkItemDerivedDay `json:"day"`
	Provider      string                   `json:"provider"`
	WorkScopeID   string                   `json:"work_scope_id"`
	TeamID        string                   `json:"team_id"`
	TeamName      string                   `json:"team_name"`
	Status        string                   `json:"status"`
	DurationHours float64                  `json:"duration_hours"`
	ItemsTouched  int                      `json:"items_touched"`
	ComputedAt    time.Time                `json:"computed_at"`
	AvgWIP        float64                  `json:"avg_wip"`
	OrgID         string                   `json:"org_id"`
}

type githubWorkItemDerivedSurfaces struct {
	EstimateCoverage []githubEstimateCoverageMetricsDailyRow
	TeamAttributions []githubWorkItemTeamAttributionRow
	StateDurations   []githubWorkItemStateDurationDailyRow
}

const (
	githubEstimateCoverageDestination = "estimate_coverage_metrics_daily"
	githubTeamAttributionsDestination = "work_item_team_attributions"
	githubStateDurationsDestination   = "work_item_state_durations_daily"
)

// githubWorkItemDerivedSurfaceDestinations is the exact set this builder
// speaks for. It is a strict subset of githubWorkItemDerivedDestinations; the
// route's deriver seam requires every derived key to be present, so a caller
// assembling the full map must combine this with the other derived builders.
var githubWorkItemDerivedSurfaceDestinations = []string{
	githubEstimateCoverageDestination,
	githubStateDurationsDestination,
	githubTeamAttributionsDestination,
}

const (
	githubWorkItemUnassignedTeamID   = "unassigned"
	githubWorkItemUnassignedTeamName = "Unassigned"
)

// Stored-precision quantization for computed_at, applied HERE at the builder
// rather than only at the adapter boundary. The production feed is time.Now()
// (complete_route.go:118), which carries nanoseconds; no destination column can
// hold them. A stamp emitted at a precision the column cannot store is written,
// quantized by the server, read back different, and the replay verdict is
// Absent forever, so the committer rewrites on every recovery.
//
// The three destinations do NOT share a precision, so this cannot be one
// constant: estimate coverage and team attributions are DateTime64(3), while
// state durations is a plain DateTime (SECONDS). Each builder quantizes at its
// own stamping site, next to the destination whose column decides the rule.
//
// DIVERGENCE FROM PYTHON (deliberate, D16 does not apply -- this is a
// persistence contract, not a computed value): Python passes computed_at
// through raw and lets the ClickHouse sink quantize on insert, because nothing
// in Python ever compares a stamped value against a stored one. The Go
// committer does exactly that on replay, so it must stamp what will be stored.
// Every oracle case uses a whole-second ComputedAt, where the two are
// identical; the divergence is only reachable with sub-precision digits.
const (
	githubEstimateCoverageStampPrecision = time.Millisecond
	githubTeamAttributionStampPrecision  = time.Millisecond
	githubStateDurationStampPrecision    = time.Second
)

// githubWorkItemDerivedStamp quantizes a stamp to what its column can store.
// It is applied ONLY where computed_at is written into a row -- never to the
// value the compute reads. An open item's final state segment ends at
// computed_at, so truncating the arithmetic input would shorten duration_hours
// against Python by up to a second.
func githubWorkItemDerivedStamp(value time.Time, precision time.Duration) time.Time {
	return value.UTC().Truncate(precision)
}

// buildGitHubWorkItemDerivedSurfaces is deliberately pure: the caller loads
// the shared derivation context once and reuses it across backfill days. It
// registers and activates nothing.
func buildGitHubWorkItemDerivedSurfaces(
	claim Claim,
	rows githubWorkItemRows,
	day time.Time,
	computedAt time.Time,
	derived githubWorkItemDerivationContext,
) (githubWorkItemDerivedSurfaces, error) {
	if claim.Validate() != nil || claim.Provider != "github" ||
		!isWorkItemFamilyDataset(claim.Dataset) || day.IsZero() || computedAt.IsZero() {
		return githubWorkItemDerivedSurfaces{}, ErrInvalidConfiguration
	}
	dayUTC := time.Date(
		day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC,
	)
	end := dayUTC.AddDate(0, 0, 1)
	computedAt = computedAt.UTC()

	coverage, err := buildGitHubEstimateCoverageMetricsDaily(
		claim, rows, dayUTC, end, computedAt, derived,
	)
	if err != nil {
		return githubWorkItemDerivedSurfaces{}, err
	}
	attributions, err := buildGitHubWorkItemTeamAttributions(
		claim, rows, computedAt, derived,
	)
	if err != nil {
		return githubWorkItemDerivedSurfaces{}, err
	}
	durations, err := buildGitHubWorkItemStateDurationsDaily(
		claim, rows, dayUTC, end, computedAt, derived,
	)
	if err != nil {
		return githubWorkItemDerivedSurfaces{}, err
	}
	return githubWorkItemDerivedSurfaces{
		EstimateCoverage: coverage,
		TeamAttributions: attributions,
		StateDurations:   durations,
	}, nil
}

type githubEstimateCoverageKey struct {
	provider, workScopeID, teamID string
}

type githubEstimateCoverageBucket struct {
	teamName                         string
	estimatedCount, unestimatedCount int
}

// buildGitHubEstimateCoverageMetricsDaily mirrors
// compute_estimate_coverage_metrics_daily (compute_work_items.py:1116).
func buildGitHubEstimateCoverageMetricsDaily(
	claim Claim,
	rows githubWorkItemRows,
	dayUTC, end, computedAt time.Time,
	derived githubWorkItemDerivationContext,
) ([]githubEstimateCoverageMetricsDailyRow, error) {
	buckets := make(map[githubEstimateCoverageKey]*githubEstimateCoverageBucket)
	order := make([]githubEstimateCoverageKey, 0, len(rows.WorkItems))

	for _, item := range rows.WorkItems {
		if err := assertGitHubWorkItemDerivedTenancy(claim, item); err != nil {
			return nil, err
		}
		createdAt := item.CreatedAt.UTC()
		terminalAt := earliestGitHubWorkItemDerivedTime(item.CompletedAt, item.ClosedAt)
		if !createdAt.Before(end) {
			continue
		}
		subject := githubWorkItemDerivationSubjectFromRow(item)
		teamID, teamName, _ := derived.resolve(subject)
		key := githubEstimateCoverageKey{
			provider:    item.Provider,
			workScopeID: workItemDerivationScope(subject),
			teamID:      normalizeGitHubWorkItemDerivedTeamID(teamID),
		}
		bucket := buckets[key]
		if bucket == nil {
			// Python creates the bucket BEFORE the terminal-item skip below, so
			// an item that reached a terminal state before the window end still
			// materialises an all-zero group (backlog_size 0, ratio None). D16:
			// pinned by the terminal_only_group oracle case, not corrected. The
			// team_name recorded here is the FIRST contributing item's — later
			// items resolving a different name do not overwrite it, which is the
			// opposite of the state-duration builder's last-wins rule below.
			bucket = &githubEstimateCoverageBucket{
				teamName: normalizeGitHubWorkItemDerivedTeamName(teamName),
			}
			buckets[key] = bucket
			order = append(order, key)
		}
		if terminalAt != nil && terminalAt.Before(end) {
			continue
		}
		if item.StoryPoints == nil {
			bucket.unestimatedCount++
			continue
		}
		bucket.estimatedCount++
	}

	// Python sorts by (provider, work_scope_id, str(team_id or "")). team_id is
	// already normalised to a non-empty string by this point, so the `or ""`
	// arm is unreachable here; sorting on the normalised value matches.
	sort.SliceStable(order, func(left, right int) bool {
		return githubEstimateCoverageKeyLess(order[left], order[right])
	})

	result := make([]githubEstimateCoverageMetricsDailyRow, 0, len(order))
	for _, key := range order {
		bucket := buckets[key]
		backlogSize := bucket.estimatedCount + bucket.unestimatedCount
		var ratio *float64
		if backlogSize != 0 {
			value := float64(bucket.estimatedCount) / float64(backlogSize)
			ratio = &value
		}
		teamID := key.teamID
		teamName := bucket.teamName
		result = append(result, githubEstimateCoverageMetricsDailyRow{
			Day:              newGitHubWorkItemDerivedDay(dayUTC),
			Provider:         key.provider,
			WorkScopeID:      key.workScopeID,
			TeamID:           &teamID,
			TeamName:         &teamName,
			EstimatedCount:   bucket.estimatedCount,
			UnestimatedCount: bucket.unestimatedCount,
			BacklogSize:      backlogSize,
			Ratio:            ratio,
			ComputedAt: githubWorkItemDerivedStamp(
				computedAt, githubEstimateCoverageStampPrecision,
			),
			OrgID: claim.OrgID,
		})
	}
	return result, nil
}

func githubEstimateCoverageKeyLess(left, right githubEstimateCoverageKey) bool {
	if left.provider != right.provider {
		return left.provider < right.provider
	}
	if left.workScopeID != right.workScopeID {
		return left.workScopeID < right.workScopeID
	}
	return left.teamID < right.teamID
}

// buildGitHubWorkItemTeamAttributions mirrors
// compute_work_item_team_attributions (compute_work_items.py:1189).
//
// D16: the Python composite calls this inside its `for d in days` loop while
// the function itself takes no day, so a multi-day backfill recomputes and
// rewrites byte-identical rows once per day. This builder reproduces the
// per-call result exactly; the repetition is a property of the CALLER, not of
// this function, and is tracked as CHAOS-3494.
//
// It is NOT pinned here, and no oracle case covers it: proving it needs a
// multi-day driver, which arrives with the deriver that owns the day loop.
// Claiming a pin at a seam this PR does not contain would read as coverage
// that does not exist.
func buildGitHubWorkItemTeamAttributions(
	claim Claim,
	rows githubWorkItemRows,
	computedAt time.Time,
	derived githubWorkItemDerivationContext,
) ([]githubWorkItemTeamAttributionRow, error) {
	result := make([]githubWorkItemTeamAttributionRow, 0, len(rows.WorkItems))
	for _, item := range rows.WorkItems {
		if err := assertGitHubWorkItemDerivedTenancy(claim, item); err != nil {
			return nil, err
		}
		_, _, candidates := derived.resolve(githubWorkItemDerivationSubjectFromRow(item))
		for _, candidate := range candidates {
			// Python emits candidate.team_id / team_name UNNORMALISED here --
			// unlike every other derived surface, which routes them through
			// normalize_team_id. A null team_id therefore persists as NULL in
			// this table while the same resolution persists as "unassigned" in
			// estimate coverage. D16: mirrored, pinned by the
			// unassigned_candidate oracle case.
			result = append(result, githubWorkItemTeamAttributionRow{
				WorkItemID: item.WorkItemID,
				Provider:   item.Provider,
				Source:     candidate.Source,
				IsPrimary:  candidate.IsPrimary,
				Confidence: candidate.Confidence,
				Evidence:   candidate.Evidence,
				ComputedAt: githubWorkItemDerivedStamp(
					computedAt, githubTeamAttributionStampPrecision,
				),
				RepoID:   item.RepoID,
				TeamID:   candidate.TeamID,
				TeamName: candidate.TeamName,
				OrgID:    item.OrgID,
			})
		}
	}
	return result, nil
}

type githubStateDurationKey struct {
	provider, workScopeID, teamID, status string
}

type githubStateDurationTeamKey struct {
	provider, workScopeID, teamID string
}

type githubWorkItemStatusSegment struct {
	status     string
	start, end time.Time
}

// buildGitHubWorkItemStateDurationsDaily mirrors
// compute_work_item_state_durations_daily
// (compute_work_item_state_durations.py:108).
func buildGitHubWorkItemStateDurationsDaily(
	claim Claim,
	rows githubWorkItemRows,
	dayUTC, end, computedAt time.Time,
	derived githubWorkItemDerivationContext,
) ([]githubWorkItemStateDurationDailyRow, error) {
	transitionsByItem := make(map[string][]githubWorkItemTransitionRow)
	for _, transition := range rows.StatusTransitions {
		if transition.OrgID != claim.OrgID {
			return nil, ErrInvalidConfiguration
		}
		transitionsByItem[transition.WorkItemID] = append(
			transitionsByItem[transition.WorkItemID], transition,
		)
	}

	totals := make(map[githubStateDurationKey]float64)
	itemsSeen := make(map[githubStateDurationKey]map[string]struct{})
	teamNames := make(map[githubStateDurationTeamKey]string)
	order := make([]githubStateDurationKey, 0, len(rows.WorkItems))

	for _, item := range rows.WorkItems {
		if err := assertGitHubWorkItemDerivedTenancy(claim, item); err != nil {
			return nil, err
		}
		itemTransitions := transitionsByItem[item.WorkItemID]
		if len(itemTransitions) == 0 {
			// Python: an item with no transition history contributes no rows,
			// and -- because this `continue` precedes the team_name write below
			// -- also never registers its team name.
			continue
		}
		subject := githubWorkItemDerivationSubjectFromRow(item)
		teamIDValue, teamNameValue, _ := derived.resolve(subject)
		teamID := normalizeGitHubWorkItemDerivedTeamID(teamIDValue)
		teamName := normalizeGitHubWorkItemDerivedTeamName(teamNameValue)
		workScopeID := workItemDerivationScope(subject)
		// Last contributing item wins, in work-item iteration order. The
		// estimate-coverage builder above deliberately keeps the FIRST instead;
		// the two rules disagree in Python and must not be shared.
		teamNames[githubStateDurationTeamKey{item.Provider, workScopeID, teamID}] = teamName

		for _, segment := range githubWorkItemStatusSegments(item, itemTransitions, computedAt) {
			overlapStart := segment.start
			if dayUTC.After(overlapStart) {
				overlapStart = dayUTC
			}
			overlapEnd := segment.end
			if end.Before(overlapEnd) {
				overlapEnd = end
			}
			if !overlapEnd.After(overlapStart) {
				continue
			}
			hours := overlapEnd.Sub(overlapStart).Seconds() / 3600.0
			key := githubStateDurationKey{
				provider: item.Provider, workScopeID: workScopeID,
				teamID: teamID, status: segment.status,
			}
			if _, exists := totals[key]; !exists {
				order = append(order, key)
			}
			totals[key] += hours
			if itemsSeen[key] == nil {
				itemsSeen[key] = make(map[string]struct{})
			}
			itemsSeen[key][item.WorkItemID] = struct{}{}
		}
	}

	sort.SliceStable(order, func(left, right int) bool {
		return githubStateDurationKeyLess(order[left], order[right])
	})

	result := make([]githubWorkItemStateDurationDailyRow, 0, len(order))
	for _, key := range order {
		totalHours := totals[key]
		result = append(result, githubWorkItemStateDurationDailyRow{
			Day:           newGitHubWorkItemDerivedDay(dayUTC),
			Provider:      key.provider,
			WorkScopeID:   key.workScopeID,
			TeamID:        key.teamID,
			TeamName:      teamNames[githubStateDurationTeamKey{key.provider, key.workScopeID, key.teamID}],
			Status:        key.status,
			DurationHours: totalHours,
			ItemsTouched:  len(itemsSeen[key]),
			// The RAW computedAt above is what githubWorkItemStatusSegments
			// used as the open segment's end; only the stamp is quantized.
			ComputedAt: githubWorkItemDerivedStamp(
				computedAt, githubStateDurationStampPrecision,
			),
			// Python divides the whole-day total by a fixed 24, not by the
			// number of hours the window actually covers, so a partial-day
			// window understates avg_wip. Mirrored.
			AvgWIP: totalHours / 24.0,
			OrgID:  claim.OrgID,
		})
	}
	return result, nil
}

func githubStateDurationKeyLess(left, right githubStateDurationKey) bool {
	if left.provider != right.provider {
		return left.provider < right.provider
	}
	if left.workScopeID != right.workScopeID {
		return left.workScopeID < right.workScopeID
	}
	if left.teamID != right.teamID {
		return left.teamID < right.teamID
	}
	return left.status < right.status
}

// githubWorkItemStatusSegments mirrors _segment_statuses
// (compute_work_item_state_durations.py:56).
func githubWorkItemStatusSegments(
	item githubWorkItemRow,
	transitions []githubWorkItemTransitionRow,
	computedAt time.Time,
) []githubWorkItemStatusSegment {
	if len(transitions) == 0 {
		return nil
	}
	ordered := make([]githubWorkItemTransitionRow, len(transitions))
	copy(ordered, transitions)
	// Python's sorted() is stable, so transitions sharing an occurred_at keep
	// their input order. sort.Slice is NOT stable and would reorder them.
	sort.SliceStable(ordered, func(left, right int) bool {
		return ordered[left].OccurredAt.UTC().Before(ordered[right].OccurredAt.UTC())
	})

	endOfItem := computedAt.UTC()
	if item.CompletedAt != nil {
		endOfItem = item.CompletedAt.UTC()
	}
	currentStatus := ordered[0].FromStatus
	if currentStatus == "" {
		currentStatus = item.Status
	}
	currentStart := item.CreatedAt.UTC()

	segments := make([]githubWorkItemStatusSegment, 0, len(ordered)+1)
	for _, transition := range ordered {
		occurredAt := transition.OccurredAt.UTC()
		if !occurredAt.After(currentStart) {
			// A transition at or before the current segment start rewrites the
			// open segment rather than closing one, so a pre-creation
			// transition silently moves the item's start backwards.
			currentStatus = transition.ToStatus
			currentStart = occurredAt
			continue
		}
		segments = append(segments, githubWorkItemStatusSegment{
			status: currentStatus, start: currentStart, end: occurredAt,
		})
		currentStatus = transition.ToStatus
		currentStart = occurredAt
	}
	if endOfItem.After(currentStart) {
		segments = append(segments, githubWorkItemStatusSegment{
			status: currentStatus, start: currentStart, end: endOfItem,
		})
	}
	result := make([]githubWorkItemStatusSegment, 0, len(segments))
	for _, segment := range segments {
		if segment.end.After(segment.start) {
			result = append(result, segment)
		}
	}
	return result
}

// derivedRows encodes this builder's three destinations for the route's
// deriver seam. It speaks only for the destinations it owns: the caller
// merges this with the other derived builders' maps.
func (surfaces githubWorkItemDerivedSurfaces) derivedRows() (map[string][]json.RawMessage, error) {
	values := map[string]any{
		githubEstimateCoverageDestination: surfaces.EstimateCoverage,
		githubStateDurationsDestination:   surfaces.StateDurations,
		githubTeamAttributionsDestination: surfaces.TeamAttributions,
	}
	result := make(map[string][]json.RawMessage, len(values))
	for _, destination := range githubWorkItemDerivedSurfaceDestinations {
		rows, owned := values[destination]
		if !owned {
			return nil, ErrInvalidConfiguration
		}
		encoded, err := marshalGitHubWorkItemDerivedRows(rows)
		if err != nil {
			return nil, err
		}
		result[destination] = encoded
	}
	if len(result) != len(values) {
		return nil, ErrInvalidConfiguration
	}
	return result, nil
}

func marshalGitHubWorkItemDerivedRows(rows any) ([]json.RawMessage, error) {
	encoded, err := json.Marshal(rows)
	if err != nil {
		return nil, err
	}
	var result []json.RawMessage
	if err := json.Unmarshal(encoded, &result); err != nil {
		return nil, fmt.Errorf("%w: derived rows", ErrEffectRecoveryUnsafe)
	}
	if result == nil {
		// A nil Go slice marshals to `null`, which round-trips back to nil. The
		// deriver seam distinguishes "evaluated, produced nothing" from
		// "missing destination" by key presence, so the value must be an empty
		// slice, never a nil one.
		result = []json.RawMessage{}
	}
	return result, nil
}

// assertGitHubWorkItemDerivedTenancy refuses a row that does not belong to the
// claim, and a row whose created_at is missing.
//
// It is called BEFORE each builder's own window/terminal skips, deliberately.
// A foreign-tenant row that happens to fall outside the window would otherwise
// be silently skipped rather than refused, so whether a cross-tenant leak
// raises would depend on the row's dates -- the check would hold only for the
// rows that were going to be counted anyway, which is the opposite of a
// tenancy fence. Asserting first makes the guarantee unconditional.
//
// DIVERGENCE FROM PYTHON, at-site: the CreatedAt.IsZero() clause hard-fails,
// while Python accepts datetime(1,1,1) and carries it through. Go cannot
// distinguish "absent" from "zero instant" -- time.Time's zero value IS
// 0001-01-01 -- and every consumer here does date arithmetic against
// created_at, so a zero would silently place the item before every window
// rather than announce itself. Refusing is the safer half of the divergence
// and it is a REJECTION, never a wrong number. No oracle case covers it,
// because constructing one would require Python to accept an input this
// function rejects; it is asserted by the unit test instead.
func assertGitHubWorkItemDerivedTenancy(claim Claim, item githubWorkItemRow) error {
	if item.OrgID != claim.OrgID || item.Provider != claim.Provider ||
		item.CreatedAt.IsZero() {
		return ErrInvalidConfiguration
	}
	return nil
}

// earliestGitHubWorkItemDerivedTime mirrors _earliest_utc: the earliest
// non-null of the two, or nil when both are nil.
func earliestGitHubWorkItemDerivedTime(values ...*time.Time) *time.Time {
	var earliest *time.Time
	for _, value := range values {
		if value == nil {
			continue
		}
		candidate := value.UTC()
		if earliest == nil || candidate.Before(*earliest) {
			earliest = &candidate
		}
	}
	return earliest
}

// normalizeGitHubWorkItemDerivedTeamID mirrors normalize_team_id
// (providers/teams.py:36): None/blank becomes "unassigned", otherwise the
// value is stripped.
func normalizeGitHubWorkItemDerivedTeamID(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return githubWorkItemUnassignedTeamID
	}
	return strings.TrimSpace(*value)
}

// normalizeGitHubWorkItemDerivedTeamName mirrors normalize_team_name
// (providers/teams.py:43).
func normalizeGitHubWorkItemDerivedTeamName(value *string) string {
	if value == nil || strings.TrimSpace(*value) == "" {
		return githubWorkItemUnassignedTeamName
	}
	return strings.TrimSpace(*value)
}

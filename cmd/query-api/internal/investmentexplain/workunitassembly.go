package investmentexplain

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

// WorkUnitTimeRange ports api/models/schemas.py's WorkUnitTimeRange.
type WorkUnitTimeRange struct {
	Start time.Time
	End   time.Time
}

// WorkUnitEffort ports api/models/schemas.py's WorkUnitEffort.
type WorkUnitEffort struct {
	Metric string
	Value  float64
}

// EvidenceQualityOutput ports api/models/schemas.py's EvidenceQuality.
type EvidenceQualityOutput struct {
	Value *float64
	Band  *string
}

// WorkUnitEvidenceOutput ports api/models/schemas.py's WorkUnitEvidence.
// Each entry is a loosely-shaped map, matching Python's
// list[dict[str, Any]] -- the evidence entries are heterogeneous by
// design (a time_range entry has different keys than a repo_scope entry).
type WorkUnitEvidenceOutput struct {
	Textual    []map[string]any
	Structural []map[string]any
	Contextual []map[string]any
}

// InvestmentBreakdownOutput ports api/models/schemas.py's
// InvestmentBreakdown. Themes/Subcategories preserve JSON-decode order
// (see parseDistributionOrdered's doc comment) rather than using a plain
// Go map -- _dominant_subcategory's tie-break (investment_mix_explain.py:
// 124-132) depends on it.
type InvestmentBreakdownOutput struct {
	Themes        []keyValue
	Subcategories []keyValue
}

// WorkUnitInvestment ports api/models/schemas.py's WorkUnitInvestment.
type WorkUnitInvestment struct {
	WorkUnitID      string
	WorkUnitType    *string
	WorkUnitName    *string
	TimeRange       WorkUnitTimeRange
	Effort          WorkUnitEffort
	Investment      InvestmentBreakdownOutput
	EvidenceQuality EvidenceQualityOutput
	Evidence        WorkUnitEvidenceOutput
}

// BuildWorkUnitInvestmentsOptions ports build_work_unit_investments'
// parameters (work_units.py:234-241). ThemeFilters/SubcategoryFilters are
// the pre-split output of splitCategoryFilters -- callers that have a raw
// work_category list should call that first, matching how
// build_work_unit_investments itself calls _split_category_filters(filters)
// as its own first step.
type BuildWorkUnitInvestmentsOptions struct {
	OrgID              string
	StartTS            time.Time
	EndTS              time.Time
	RepoIDs            []string
	Limit              int
	IncludeText        bool
	WorkUnitID         string
	ThemeFilters       []string
	SubcategoryFilters []string
}

// pythonIsoFormat ports Python's datetime.isoformat() for a
// timezone-aware value: "YYYY-MM-DDTHH:MM:SS[.ffffff]+HH:MM" -- fractional
// seconds appear ONLY when non-zero (never zero-padded to a bare ".000000"),
// and the offset always uses a colon and never "Z", even for +00:00.
// Confirmed against CPython directly (both the zero- and
// non-zero-microsecond cases) -- Go's time.Format has no single layout
// that reproduces "omit fractional seconds entirely when zero", so this
// builds the string in two pieces.
func pythonIsoFormat(t time.Time) string {
	base := t.Format("2006-01-02T15:04:05")
	if t.Nanosecond() != 0 {
		base += fmt.Sprintf(".%06d", t.Nanosecond()/1000)
	}
	return base + t.Format("-07:00")
}

// BuildWorkUnitInvestments ports build_work_unit_investments
// (work_units.py:234-460) exactly, including its two attribution passes
// (a flat batch fetch across all rows, then a per-row recomputation for
// the actual team assigned to each unit -- see the Python source's own
// "arrayDistinct parity" comment at work_units.py:414-416 for why the
// per-row pass exists separately from the batch one).
//
// warn_once_for_mock_fixture_rows (work_units.py:286) is NOT ported --
// pure provenance telemetry with no effect on the returned rows, same
// treatment as record_stale_investment_membership_scope elsewhere in this
// package.
func (reader *Reader) BuildWorkUnitInvestments(ctx context.Context, opts BuildWorkUnitInvestmentsOptions) ([]WorkUnitInvestment, error) {
	if reader == nil || reader.client == nil {
		return nil, ErrUnavailable
	}

	limit := opts.Limit
	if limit < 1 {
		limit = 1
	}

	rows, err := reader.FetchWorkUnitInvestments(ctx, WorkUnitInvestmentsFilter{
		OrgID:      opts.OrgID,
		StartTS:    opts.StartTS,
		EndTS:      opts.EndTS,
		RepoIDs:    opts.RepoIDs,
		Limit:      limit,
		WorkUnitID: opts.WorkUnitID,
	})
	if err != nil {
		return nil, err
	}
	if len(rows) == 0 {
		return []WorkUnitInvestment{}, nil
	}

	if len(opts.ThemeFilters) > 0 || len(opts.SubcategoryFilters) > 0 {
		filtered := make([]WorkUnitInvestmentRow, 0, len(rows))
		for _, row := range rows {
			themeDist := parseDistribution(derefString(row.ThemeDistributionJSON))
			subcategoryDist := parseDistribution(derefString(row.SubcategoryDistributionJSON))
			if matchesCategoryFilter(themeDist, subcategoryDist, opts.ThemeFilters, opts.SubcategoryFilters) {
				filtered = append(filtered, row)
			}
		}
		rows = filtered
	}

	var quoteRows []WorkUnitInvestmentQuoteRow
	if opts.IncludeText {
		var unitRuns []WorkUnitRunPair
		for _, row := range rows {
			runID := derefString(row.CategorizationRunID)
			if row.WorkUnitID != "" && runID != "" {
				unitRuns = append(unitRuns, WorkUnitRunPair{WorkUnitID: row.WorkUnitID, RunID: runID})
			}
		}
		quoteRows, err = reader.FetchWorkUnitInvestmentQuotes(ctx, opts.OrgID, unitRuns)
		if err != nil {
			return nil, err
		}
	}

	repoIDValues := make([]string, 0, len(rows))
	for _, row := range rows {
		if repoID := derefString(row.RepoID); repoID != "" {
			repoIDValues = append(repoIDValues, repoID)
		}
	}
	repoScopes, err := reader.FetchRepoScopes(ctx, opts.OrgID, repoIDValues)
	if err != nil {
		return nil, err
	}

	var issueIDs, prRefs []string
	for _, row := range rows {
		payload := derefString(row.StructuralEvidenceJSON)
		issueIDs = append(issueIDs, extractIssueIDs(payload)...)
		prRefs = append(prRefs, extractPRRefs(payload)...)
	}
	var prRepoUUIDs []string
	for _, ref := range prRefs {
		if match := prEvidenceRefRE.FindStringSubmatch(ref); match != nil {
			prRepoUUIDs = append(prRepoUUIDs, match[1])
		}
	}
	repoIdentities, err := reader.FetchRepoIdentities(ctx, opts.OrgID, prRepoUUIDs)
	if err != nil {
		return nil, err
	}
	var prWorkItemIDs []string
	for _, ref := range prRefs {
		if id, ok := prRefWorkItemID(ref, repoIdentities); ok {
			prWorkItemIDs = append(prWorkItemIDs, id)
		}
	}
	teamAssignments, err := reader.FetchWorkItemTeamAssignments(ctx, opts.OrgID, append(append([]string{}, issueIDs...), prWorkItemIDs...))
	if err != nil {
		return nil, err
	}

	quotesByUnit := map[string][]WorkUnitInvestmentQuoteRow{}
	for _, quote := range quoteRows {
		if quote.WorkUnitID == "" {
			continue
		}
		quotesByUnit[quote.WorkUnitID] = append(quotesByUnit[quote.WorkUnitID], quote)
	}

	results := make([]WorkUnitInvestment, 0, len(rows))
	for _, row := range rows {
		unitID := row.WorkUnitID
		if unitID == "" {
			continue
		}
		fromTS := row.FromTS.UTC()
		if fromTS.IsZero() {
			fromTS = opts.StartTS.UTC()
		}
		toTS := row.ToTS.UTC()
		if toTS.IsZero() {
			toTS = opts.EndTS.UTC()
		}
		themeDistribution := parseDistributionOrdered(derefString(row.ThemeDistributionJSON))
		subcategoryDistribution := parseDistributionOrdered(derefString(row.SubcategoryDistributionJSON))
		metric := effortMetric(derefString(row.EffortMetric))
		var effortValue float64
		if row.EffortValue != nil {
			effortValue = *row.EffortValue
		}

		var structuralEvidence []map[string]any
		structuralPayload := derefString(row.StructuralEvidenceJSON)
		if structuralPayload != "" {
			var parsed map[string]any
			if err := json.Unmarshal([]byte(structuralPayload), &parsed); err == nil {
				entry := map[string]any{"type": "work_unit_nodes"}
				for k, v := range parsed {
					entry[k] = v
				}
				structuralEvidence = append(structuralEvidence, entry)
			}
		}

		var textualEvidence []map[string]any
		for _, quote := range quotesByUnit[unitID] {
			textualEvidence = append(textualEvidence, map[string]any{
				"type":   "evidence_quote",
				"quote":  quote.Quote,
				"source": quote.SourceType,
				"id":     quote.SourceID,
			})
		}

		spanDays := toTS.Sub(fromTS).Hours() / 24
		if spanDays < 0 {
			spanDays = 0
		}
		contextualEvidence := []map[string]any{
			{
				"type":      "time_range",
				"start":     pythonIsoFormat(fromTS),
				"end":       pythonIsoFormat(toTS),
				"span_days": spanDays,
			},
		}

		repoScope := "unassigned"
		if repoID := derefString(row.RepoID); repoID != "" {
			if scope, ok := repoScopes[repoID]; ok && scope != "" {
				repoScope = scope
			} else {
				repoScope = repoID
			}
		}
		contextualEvidence = append(contextualEvidence, map[string]any{
			"type":     "repo_scope",
			"repo_ids": []string{repoScope},
		})

		unitIssueIDs := extractIssueIDs(structuralPayload)
		var unitPRWorkItemIDs []string
		for _, ref := range extractPRRefs(structuralPayload) {
			if id, ok := prRefWorkItemID(ref, repoIdentities); ok {
				unitPRWorkItemIDs = append(unitPRWorkItemIDs, id)
			}
		}
		teamID, teamName := majorityTeamForIssues(
			dedupeStrings(append(append([]string{}, unitIssueIDs...), unitPRWorkItemIDs...)),
			teamAssignments,
		)
		contextualEvidence = append(contextualEvidence, map[string]any{
			"type":       "team_scope",
			"team_ids":   []string{teamID},
			"team_names": []string{teamName},
		})

		var evidenceQualityValue *float64
		if row.EvidenceQuality != nil {
			v := *row.EvidenceQuality
			evidenceQualityValue = &v
		}
		var bandInput string
		if rawBand := derefString(row.EvidenceQualityBand); rawBand != "" {
			bandInput = rawBand
		} else if row.EvidenceQuality == nil {
			bandInput = "unknown"
		} else {
			bandInput = "very_low"
		}
		evidenceBand := evidenceQualityBand(bandInput)

		results = append(results, WorkUnitInvestment{
			WorkUnitID:   unitID,
			WorkUnitType: cleanOptionalText(row.WorkUnitType),
			WorkUnitName: cleanOptionalText(row.WorkUnitName),
			TimeRange:    WorkUnitTimeRange{Start: fromTS, End: toTS},
			Effort:       WorkUnitEffort{Metric: metric, Value: effortValue},
			Investment: InvestmentBreakdownOutput{
				Themes:        themeDistribution,
				Subcategories: subcategoryDistribution,
			},
			EvidenceQuality: EvidenceQualityOutput{
				Value: evidenceQualityValue,
				Band:  &evidenceBand,
			},
			Evidence: WorkUnitEvidenceOutput{
				Textual:    textualEvidence,
				Structural: structuralEvidence,
				Contextual: contextualEvidence,
			},
		})
	}

	sort.SliceStable(results, func(i, j int) bool {
		if results[i].Effort.Value != results[j].Effort.Value {
			return results[i].Effort.Value > results[j].Effort.Value
		}
		return results[i].WorkUnitID < results[j].WorkUnitID
	})
	if len(results) > limit {
		results = results[:limit]
	}
	return results, nil
}

func derefString(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

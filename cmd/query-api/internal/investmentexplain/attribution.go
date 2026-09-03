package investmentexplain

import (
	"encoding/json"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// cleanOptionalText ports work_units.py's _clean_optional_text
// (work_units.py:46-50), specialized to string input: this port's only
// callers pass a ClickHouse String column (work_unit_type/work_unit_name),
// never Python's broader `object`, so there is no str()-of-arbitrary-value
// case to reproduce.
func cleanOptionalText(value *string) *string {
	if value == nil {
		return nil
	}
	trimmed := pythonparity.Strip(*value)
	if trimmed == "" {
		return nil
	}
	return &trimmed
}

// effortMetric ports work_units.py's _effort_metric (work_units.py:53-54).
func effortMetric(value string) string {
	if value == "active_hours" {
		return "active_hours"
	}
	return "churn_loc"
}

// evidenceQualityBand ports work_units.py's _evidence_quality_band
// (work_units.py:57-67).
func evidenceQualityBand(value string) string {
	switch value {
	case "high", "moderate", "low", "very_low":
		return value
	default:
		return "unknown"
	}
}

// splitCategoryFilters ports work_units.py's _split_category_filters
// (work_units.py:70-84), taking filters.why.work_category directly as a
// []string rather than a MetricFilter -- the exact same substance the
// Python function reads, since `for category in filters.why.work_category
// or []` is the only field it touches.
func splitCategoryFilters(workCategory []string) (themes, subcategories []string) {
	var themeList, subcategoryList []string
	for _, category := range workCategory {
		categoryStr := pythonparity.Strip(category)
		if categoryStr == "" {
			continue
		}
		if before, after, found := strings.Cut(categoryStr, "."); found {
			subcategoryList = append(subcategoryList, categoryStr)
			themeList = append(themeList, before)
			_ = after
		} else {
			themeList = append(themeList, categoryStr)
		}
	}
	return dedupeStrings(themeList), dedupeStrings(subcategoryList)
}

// parseDistribution ports work_units.py's _parse_distribution
// (work_units.py:87-97), specialized to string input: this port's callers
// always pass a ClickHouse String column
// (theme/subcategory_distribution_json), never Python's dict-or-string
// union -- the dict branch exists in Python only for other, non-ClickHouse
// callers this endpoint's call path never reaches.
//
// KNOWN DIVERGENCE, deliberately not closed: Python's dict comprehension
// (`float(v or 0.0)` per value) runs OUTSIDE the function's own
// try/except -- only json.loads itself is guarded -- so a distribution
// object that parses as valid JSON but holds a non-numeric string value
// (e.g. {"velocity": "oops"}) raises ValueError UNCAUGHT and crashes the
// whole request. This column is system-written (materialize.py), never
// user input, so that shape is not expected in practice. This port fails
// soft instead (returns an empty map for the whole distribution) rather
// than reproducing an unintentional 5xx-on-malformed-internal-data path.
func parseDistribution(raw string) map[string]float64 {
	if raw == "" {
		return map[string]float64{}
	}
	var decoded map[string]json.Number
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return map[string]float64{}
	}
	out := make(map[string]float64, len(decoded))
	for key, value := range decoded {
		numeric, err := value.Float64()
		if err != nil {
			numeric = 0
		}
		out[key] = numeric
	}
	return out
}

// parseDistributionOrdered is parseDistribution's order-preserving
// sibling: _dominant_subcategory (investment_mix_explain.py:124-132)
// iterates `subcategories.items()` and keeps the FIRST value that is
// STRICTLY greater than the running best (`v > best_value`, not `>=`),
// so on a tie the FIRST-encountered key wins -- and "first" means JSON
// object key order, which map[string]float64 (Go map iteration is
// randomized) cannot reproduce. json.Decoder's token stream is used
// directly rather than json.Unmarshal for exactly this reason: it is the
// one decode path in encoding/json that surfaces object keys in their
// original document order.
func parseDistributionOrdered(raw string) []keyValue {
	if raw == "" {
		return nil
	}
	decoder := json.NewDecoder(strings.NewReader(raw))
	decoder.UseNumber()

	token, err := decoder.Token()
	if err != nil {
		return nil
	}
	delim, ok := token.(json.Delim)
	if !ok || delim != '{' {
		return nil
	}

	var items []keyValue
	for decoder.More() {
		keyToken, err := decoder.Token()
		if err != nil {
			return nil
		}
		key, ok := keyToken.(string)
		if !ok {
			return nil
		}
		valueToken, err := decoder.Token()
		if err != nil {
			return nil
		}
		number, ok := valueToken.(json.Number)
		var numeric float64
		if ok {
			numeric, _ = number.Float64()
		}
		items = append(items, keyValue{Key: key, Value: numeric})
	}
	return items
}

// parseStructuralPayload ports work_units.py's _parse_structural_payload
// (work_units.py:100-111), specialized to string input for the same reason
// parseDistribution is.
func parseStructuralPayload(raw string) (map[string]any, bool) {
	if raw == "" {
		return nil, false
	}
	var decoded map[string]any
	if err := json.Unmarshal([]byte(raw), &decoded); err != nil {
		return nil, false
	}
	return decoded, true
}

// extractIssueIDs ports work_units.py's _extract_issue_ids
// (work_units.py:114-121).
func extractIssueIDs(structuralPayload string) []string {
	parsed, ok := parseStructuralPayload(structuralPayload)
	if !ok {
		return nil
	}
	issues, isList := parsed["issues"].([]any)
	if !isList {
		return nil
	}
	out := make([]string, 0, len(issues))
	for _, item := range issues {
		if s := stringifyTruthy(item); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// prEvidenceRefRE ports work_units.py's _PR_EVIDENCE_REF_RE
// (work_units.py:126): `{repo_uuid}#pr{number}` from
// work_graph/ids.py:generate_pr_id. Both capture groups are internally
// generated (a UUID this system minted, a PR number formatted from an
// int) rather than untrusted external/LLM text, so Go's ASCII-only \d is
// exact here -- unlike investment_mix_validation.py's NUMERIC_PATTERN,
// this is not validating arbitrary human/model-authored input.
var prEvidenceRefRE = regexp.MustCompile(`^([0-9a-fA-F-]{36})#pr(\d+)$`)

// extractPRRefs ports work_units.py's _extract_pr_refs
// (work_units.py:129-140).
func extractPRRefs(structuralPayload string) []string {
	parsed, ok := parseStructuralPayload(structuralPayload)
	if !ok {
		return nil
	}
	prs, isList := parsed["prs"].([]any)
	if !isList {
		return nil
	}
	out := make([]string, 0, len(prs))
	for _, item := range prs {
		if s := stringifyTruthy(item); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// repoIdentity is fetch_repo_identities' per-repo (slug, provider) pair.
type repoIdentity struct {
	Slug     string
	Provider string
}

// prRefWorkItemID ports work_units.py's _pr_ref_work_item_id
// (work_units.py:143-166).
func prRefWorkItemID(prRef string, repoIdentities map[string]repoIdentity) (string, bool) {
	match := prEvidenceRefRE.FindStringSubmatch(prRef)
	if match == nil {
		return "", false
	}
	identity, ok := repoIdentities[match[1]]
	if !ok || identity.Slug == "" {
		return "", false
	}
	system := "github"
	workItemType := "pr"
	if identity.Provider == "gitlab" {
		system = "gitlab"
		workItemType = "merge_request"
	}
	return deriveWorkItemID(system, identity.Slug, match[2], workItemType), true
}

// teamAssignment is fetch_work_item_team_assignments' per-work-item
// primary team attribution.
type teamAssignment struct {
	TeamID   string
	TeamName string
}

// majorityTeamForIssues ports work_units.py's _majority_team_for_issues
// (work_units.py:169-210) exactly -- see that function's own doc comment
// for the three rules it must share verbatim with build_unit_team_subquery
// (CHAOS-2416): vote per team ID not label, label is max(label) over the
// winning id's refs, ties break on (count, team ID).
func majorityTeamForIssues(issueIDs []string, teamMap map[string]teamAssignment) (teamID, teamLabel string) {
	counts := map[string]int{}
	labels := map[string]string{}
	for _, id := range dedupeStrings(issueIDs) {
		assignment := teamMap[id]
		trimmedTeamID := pythonparity.Strip(assignment.TeamID)
		trimmedTeamName := pythonparity.Strip(assignment.TeamName)
		if trimmedTeamID == "" {
			continue
		}
		counts[trimmedTeamID]++
		label := trimmedTeamName
		if label == "" {
			label = trimmedTeamID
		}
		if label > labels[trimmedTeamID] {
			labels[trimmedTeamID] = label
		}
	}
	if len(counts) == 0 {
		return "unassigned", "Unassigned"
	}

	ids := make([]string, 0, len(counts))
	for id := range counts {
		ids = append(ids, id)
	}
	// Python's max(counts.items(), key=lambda item: (item[1], item[0]))
	// picks the (count, id) pair that sorts HIGHEST -- ties broken by the
	// LARGER team id, not smaller. Sort ascending, take the last.
	sort.Slice(ids, func(i, j int) bool {
		if counts[ids[i]] != counts[ids[j]] {
			return counts[ids[i]] < counts[ids[j]]
		}
		return ids[i] < ids[j]
	})
	winner := ids[len(ids)-1]
	label := labels[winner]
	if label == "" {
		label = winner
	}
	return winner, label
}

// matchesCategoryFilter ports work_units.py's _matches_category_filter
// (work_units.py:213-231).
func matchesCategoryFilter(themeDistribution, subcategoryDistribution map[string]float64, themes, subcategories []string) bool {
	themeSet := toStringSet(themes)
	subcategorySet := toStringSet(subcategories)
	if len(themeSet) == 0 && len(subcategorySet) == 0 {
		return true
	}
	if len(subcategorySet) > 0 {
		for key, value := range subcategoryDistribution {
			if subcategorySet[key] && value > 0 {
				return true
			}
		}
	}
	if len(themeSet) > 0 {
		for key, value := range themeDistribution {
			if themeSet[key] && value > 0 {
				return true
			}
		}
	}
	return false
}

func toStringSet(values []string) map[string]bool {
	out := make(map[string]bool, len(values))
	for _, v := range values {
		out[v] = true
	}
	return out
}

// stringifyTruthy ports the `str(item) for item in ... if item` idiom used
// by _extract_issue_ids/_extract_pr_refs: skip Python-falsy entries
// (empty string, 0, None, False), else stringify. structural_evidence_json
// arrays hold only strings in practice (work-graph node ids), so this
// only needs the string and generic-JSON-number cases to be faithful to
// what could actually appear in a decoded JSON array element.
func stringifyTruthy(item any) string {
	switch v := item.(type) {
	case string:
		return v
	case json.Number:
		if v == "0" {
			return ""
		}
		return string(v)
	case float64:
		if v == 0 {
			return ""
		}
		return strconv.FormatFloat(v, 'g', -1, 64)
	case bool:
		if !v {
			return ""
		}
		return "True"
	case nil:
		return ""
	default:
		return ""
	}
}

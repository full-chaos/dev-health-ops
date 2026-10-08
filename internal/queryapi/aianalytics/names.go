package aianalytics

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-go/clickhouse"
)

// policyRuleNames are the display names of the policy rules the governance
// compute writes into ai_policy_events.rule_id (aigovernance.PolicyRule). A
// rule id outside this set has no name.
var policyRuleNames = map[string]string{
	"MISSING_AI_DECLARATION":         "Missing AI declaration",
	"MISSING_HUMAN_REVIEW":           "Missing human review",
	"SENSITIVE_REPO_DISALLOWED":      "AI use in a sensitive repository",
	"DISALLOWED_TOOL":                "Disallowed AI tool",
	"MISSING_SECURITY_SCAN":          "Missing security scan",
	"NEW_LICENSE_FINDING_FROM_AI_PR": "New license finding from an AI pull request",
}

func policyRuleName(ruleID string) *string {
	if n, ok := policyRuleNames[ruleID]; ok {
		return &n
	}
	return nil
}

type prKey struct {
	repoID string
	number uint32
}

// prSubjectKey reads the pull request a subject names: subject_id is the PR
// number alone, or "<repo uuid>#<number>" / "<repo uuid>:<number>". A prefix
// that is not the row's own repository is not trusted.
func prSubjectKey(subjectType, subjectID string, repoID *string) (prKey, bool) {
	if subjectType != "pull_request" || repoID == nil || *repoID == "" {
		return prKey{}, false
	}
	numText := subjectID
	if i := strings.LastIndexAny(subjectID, "#:"); i >= 0 {
		if !strings.EqualFold(subjectID[:i], *repoID) {
			return prKey{}, false
		}
		numText = subjectID[i+1:]
	}
	n, err := strconv.ParseUint(numText, 10, 32)
	if err != nil || n == 0 {
		return prKey{}, false
	}
	return prKey{repoID: *repoID, number: uint32(n)}, true
}

// loadPRTitles reads the stored titles of the given pull requests inside the
// org. A pull request with no non-empty title is absent from the result; a
// failed read degrades to no titles, never an error.
func loadPRTitles(ctx context.Context, client QueryClient, orgID string, keys []prKey, operation string) map[prKey]string {
	out := map[prKey]string{}
	if len(keys) == 0 || orgID == "" {
		return out
	}
	want := make(map[prKey]bool, len(keys))
	var repoIDs []string
	var numbers []uint32
	seenRepo := map[string]bool{}
	seenNum := map[uint32]bool{}
	for _, k := range keys {
		want[k] = true
		if !seenRepo[k.repoID] {
			seenRepo[k.repoID] = true
			repoIDs = append(repoIDs, k.repoID)
		}
		if !seenNum[k.number] {
			seenNum[k.number] = true
			numbers = append(numbers, k.number)
		}
	}
	rs, err := client.Query(ctx, `SELECT toString(repo_id) AS repo_id, number, coalesce(title, '') AS title
FROM git_pull_requests FINAL
WHERE org_id = {org_id:String}
  AND toString(repo_id) IN {repo_ids:Array(String)}
  AND number IN {numbers:Array(UInt32)}`, []clickhouse.Binding{
		{Name: "org_id", Value: orgID},
		{Name: "repo_ids", Value: repoIDs},
		{Name: "numbers", Value: numbers},
	})
	if err != nil {
		warnCatalogue(ctx, operation, fmt.Errorf("pull request titles query: %w", err))
		return out
	}
	defer rs.Close()
	for rs.Next() {
		var k prKey
		var title string
		if err := rs.Scan(&k.repoID, &k.number, &title); err != nil {
			warnCatalogue(ctx, operation, fmt.Errorf("pull request titles scan: %w", err))
			return map[prKey]string{}
		}
		if title != "" && want[k] {
			out[k] = title
		}
	}
	if err := rs.Err(); err != nil {
		warnCatalogue(ctx, operation, fmt.Errorf("pull request titles rows: %w", err))
		return map[prKey]string{}
	}
	return out
}

// subjectTitle is the stored title of the subject's pull request, or nil.
func subjectTitle(titles map[prKey]string, subjectType, subjectID string, repoID *string) *string {
	k, ok := prSubjectKey(subjectType, subjectID, repoID)
	if !ok {
		return nil
	}
	if t, ok := titles[k]; ok && t != "" {
		return &t
	}
	return nil
}

// loadTeamNamesOnly reads the team catalogue for the names alone: a page whose
// rows carry a team id but no repository still names its teams.
func loadTeamNamesOnly(ctx context.Context, client QueryClient, orgID, operation string) repoCatalogue {
	out := repoCatalogue{
		teamByRepo: map[string]*string{},
		repoNames:  map[string]string{},
		teamNames:  map[string]string{},
	}
	if orgID == "" {
		return out
	}
	teams, err := loadTeams(ctx, client, orgID)
	if err != nil {
		warnCatalogue(ctx, operation, err)
		return out
	}
	for _, t := range teams {
		if t.Name != "" {
			out.teamNames[t.ID] = t.Name
		}
	}
	return out
}

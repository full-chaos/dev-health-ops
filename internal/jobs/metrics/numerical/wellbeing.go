package numerical

import (
	"sort"
	"strings"
	"time"
)

// Commit is one deduplicated git commit -- one row per (repo_id, hash), the
// shape a caller reading git_commits (ReplacingMergeTree, one physical row
// per commit after FINAL/argMax collapse) already has. Python's
// compute_team_wellbeing_metrics_daily dedupes commit_stat_rows in-memory by
// (repo_id, commit_hash) ONLY because its input there is a per-file join
// product (git_commits LEFT JOIN git_commit_stats); ComputeTeamWellbeing has
// no file-level fields to join against and takes already-deduplicated
// commits, so it performs no dedup of its own.
type Commit struct {
	RepoID        string
	AuthorEmail   string
	AuthorName    string
	CommitterWhen time.Time
}

// TeamWellbeingMetric is one team's after-hours/weekend commit activity for a
// day -- the row shape team_metrics_daily persists.
type TeamWellbeingMetric struct {
	TeamID                 string
	TeamName               string
	CommitsCount           int
	AfterHoursCommitsCount int
	WeekendCommitsCount    int
	AfterHoursCommitRatio  float64
	WeekendCommitRatio     float64
}

// RepoTeamResolver resolves a team from a repo's full name (owner/repo). An
// empty repoName, or no match, must return ("", "").
type RepoTeamResolver interface {
	ResolveRepo(repoName string) (teamID, teamName string)
}

// MemberTeamResolver resolves a team from a normalized git identity. An
// empty identity, or no match, must return ("", "").
type MemberTeamResolver interface {
	ResolveMember(identity string) (teamID, teamName string)
}

// Unknown* mirror compute_team_wellbeing_metrics_daily's defaults
// (unknown_team_id="unassigned", unknown_team_name="Unassigned") --
// job_daily.py never overrides them, so they are constants here rather than
// parameters.
const (
	UnknownTeamID   = "unassigned"
	UnknownTeamName = "Unassigned"
)

// ComputeTeamWellbeing mirrors compute_team_wellbeing_metrics_daily
// (src/dev_health_ops/metrics/compute_wellbeing.py:39) over already
// tenant-scoped, deduplicated commit rows for one day.
//
// Two behaviours are reproduced deliberately because they are easy to
// "simplify" into a divergence:
//
//  1. TEAM RESOLUTION ORDER. A commit's repo is checked against the
//     repo-pattern resolver FIRST; only when that yields no team is the
//     author's identity checked against team membership. Swapping the order
//     would silently re-attribute commits in any org where a repo pattern
//     and a membership entry disagree.
//  2. NO IDENTITY-ALIAS RESOLUTION. job_daily.py calls the Python function
//     without an identity_resolver (it defaults to None), so identity
//     normalization here is the plain email > display name > "unknown"
//     fallback (normalizeGitIdentity), never alias-resolved. A caller that
//     wired an alias-resolving identity lookup in here would compute
//     different team_resolver.resolve(identity) inputs than production does.
//
// businessTZ, businessHoursStart and businessHoursEnd must be the same
// values job_daily.py resolves from BUSINESS_TIMEZONE / BUSINESS_HOURS_START
// / BUSINESS_HOURS_END (default "UTC", 9, 17) -- this function does not read
// the environment itself.
func ComputeTeamWellbeing(
	day time.Time,
	commits []Commit,
	repoNamesByID map[string]string,
	repoResolver RepoTeamResolver,
	memberResolver MemberTeamResolver,
	businessTZ *time.Location,
	businessHoursStart, businessHoursEnd int,
) []TeamWellbeingMetric {
	start := time.Date(day.UTC().Year(), day.UTC().Month(), day.UTC().Day(), 0, 0, 0, 0, time.UTC)
	end := start.Add(24 * time.Hour)

	type bucket struct {
		teamName   string
		commits    int
		afterHours int
		weekend    int
	}
	byTeam := make(map[string]*bucket)

	for _, commit := range commits {
		committedAt := commit.CommitterWhen.UTC()
		if committedAt.Before(start) || !committedAt.Before(end) {
			continue
		}
		identity := normalizeGitIdentity(commit.AuthorEmail, commit.AuthorName)

		var teamID, teamName string
		if repoResolver != nil {
			teamID, teamName = repoResolver.ResolveRepo(repoNamesByID[commit.RepoID])
		}
		if teamID == "" && memberResolver != nil {
			teamID, teamName = memberResolver.ResolveMember(identity)
		}
		if teamID == "" {
			teamID, teamName = UnknownTeamID, UnknownTeamName
		}

		entry := byTeam[teamID]
		if entry == nil {
			name := teamName
			if name == "" {
				name = teamID
			}
			entry = &bucket{teamName: name}
			byTeam[teamID] = entry
		}
		entry.commits++

		local := committedAt.In(businessTZ)
		if isWeekend(local) {
			entry.weekend++
		} else if isAfterHours(local, businessHoursStart, businessHoursEnd) {
			entry.afterHours++
		}
	}

	teamIDs := make([]string, 0, len(byTeam))
	for teamID := range byTeam {
		teamIDs = append(teamIDs, teamID)
	}
	sort.Strings(teamIDs)

	metrics := make([]TeamWellbeingMetric, 0, len(teamIDs))
	for _, teamID := range teamIDs {
		entry := byTeam[teamID]
		var afterHoursRatio, weekendRatio float64
		if entry.commits > 0 {
			afterHoursRatio = float64(entry.afterHours) / float64(entry.commits)
			weekendRatio = float64(entry.weekend) / float64(entry.commits)
		}
		metrics = append(metrics, TeamWellbeingMetric{
			TeamID:                 teamID,
			TeamName:               entry.teamName,
			CommitsCount:           entry.commits,
			AfterHoursCommitsCount: entry.afterHours,
			WeekendCommitsCount:    entry.weekend,
			AfterHoursCommitRatio:  afterHoursRatio,
			WeekendCommitRatio:     weekendRatio,
		})
	}
	return metrics
}

// isWeekend mirrors _is_weekend (compute_wellbeing.py): Python's
// datetime.weekday() is 0=Monday..6=Sunday and treats 5,6 (Sat, Sun) as the
// weekend -- the same two days time.Weekday's Saturday/Sunday name.
func isWeekend(t time.Time) bool {
	weekday := t.Weekday()
	return weekday == time.Saturday || weekday == time.Sunday
}

// isAfterHours mirrors _is_after_hours: weekend hours are never counted as
// after-hours (they are counted as weekend instead, and the two buckets are
// mutually exclusive in ComputeTeamWellbeing's if/else).
func isAfterHours(t time.Time, startHour, endHour int) bool {
	if isWeekend(t) {
		return false
	}
	hour := t.Hour()
	return hour < startHour || hour >= endHour
}

// normalizeGitIdentity mirrors normalize_git_identity's no-resolver fallback
// (src/dev_health_ops/providers/identity.py) -- the only branch
// job_daily.py's call to compute_team_wellbeing_metrics_daily ever exercises,
// since it never passes identity_resolver (default None). Python checks
// truthiness (`if email:`) before stripping; an all-whitespace email is
// truthy in Python but strips to "", so it falls through to display_name
// exactly as this does.
func normalizeGitIdentity(email, displayName string) string {
	if trimmed := strings.TrimSpace(email); trimmed != "" {
		return trimmed
	}
	if trimmed := strings.TrimSpace(displayName); trimmed != "" {
		return trimmed
	}
	return "unknown"
}

package aiimpact

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/google/uuid"
)

// D6040/D6041 (CHAOS-9915): an incident counts once, for one team, on one row.

type incidentRowKey struct {
	team     string
	workType string
	bucket   AttributionBucket
}

func incidentsByRow(records []Record) map[incidentRowKey]float64 {
	counts := map[incidentRowKey]float64{}
	for _, record := range records {
		team := ""
		if record.TeamID != nil {
			team = *record.TeamID
		}
		key := incidentRowKey{team: team, workType: record.WorkType, bucket: record.AttributionBucket}
		counts[key] += record.IncidentsCount
	}
	return counts
}

func nonZeroIncidents(records []Record) map[incidentRowKey]float64 {
	counts := incidentsByRow(records)
	for key, value := range counts {
		if value == 0 {
			delete(counts, key)
		}
	}
	return counts
}

func incidentFixtureParams(incidents ...IncidentRow) Params {
	return Params{
		Day: fixtureDay, OrgID: fixtureOrgID,
		RepoNamesByID: map[uuid.UUID]string{fixtureRepo: "acme/alpha"},
		Incidents:     incidents,
	}
}

func incidentAt(id string, after time.Duration) IncidentRow {
	return IncidentRow{ID: id, RepoID: fixtureRepo, StartedAt: fixtureBase.Add(after)}
}

func wantIncidents(t *testing.T, name string, records []Record, want map[incidentRowKey]float64) {
	t.Helper()
	got := nonZeroIncidents(records)
	if len(got) != len(want) {
		t.Errorf("%s: incidents by row %v, want %v", name, got, want)
		return
	}
	for key, value := range want {
		if got[key] != value {
			t.Errorf("%s: incidents by row %v, want %v", name, got, want)
			return
		}
	}
}

func mainOwners(owners ...string) func(uuid.UUID) []string {
	return func(uuid.UUID) []string { return append([]string(nil), owners...) }
}

// Branch 1: the team the incident source names wins over the triggering pull
// request's team and over the repository's main owner.
func TestIncidentCountsForTheTeamItsSourceNames(t *testing.T) {
	incident := incidentAt("i1", time.Hour)
	incident.SourceTeamID = strPtr("team-source")
	incident.Trigger = &IncidentTrigger{
		RepoID: fixtureRepo, Number: 7, TeamID: strPtr("team-pr"), Bucket: BucketAIAssisted, WorkType: "bug",
	}
	params := incidentFixtureParams(incident)
	params.MainOwners = mainOwners("team-owner")
	wantIncidents(t, "source team", Compute(params), map[incidentRowKey]float64{
		{team: "team-source", workType: "bug", bucket: BucketAIAssisted}: 1,
	})
}

// Branch 2: without a source team, the team that owned the triggering pull
// request. Its AI bucket and work type come only from that pull request.
func TestIncidentCountsForTheTeamThatOwnedTheTriggeringPullRequest(t *testing.T) {
	incident := incidentAt("i1", time.Hour)
	incident.Trigger = &IncidentTrigger{
		RepoID: fixtureRepo, Number: 7, TeamID: strPtr("team-pr"), Bucket: BucketAgentCreated, WorkType: "story",
	}
	params := incidentFixtureParams(incident)
	params.MainOwners = mainOwners("team-owner")
	wantIncidents(t, "pull request team", Compute(params), map[incidentRowKey]float64{
		{team: "team-pr", workType: "story", bucket: BucketAgentCreated}: 1,
	})
}

// Branch 3: no source team and no team on the triggering pull request (or no
// triggering pull request): the main owner of the repository takes it whole,
// whatever the lesser owners' shares are. With no triggering pull request there
// is no AI evidence: the human bucket row, work type "incident".
func TestIncidentCountsForTheRepositorysMainOwnerWhole(t *testing.T) {
	triggered := incidentAt("i1", time.Hour)
	triggered.Trigger = &IncidentTrigger{RepoID: fixtureRepo, Number: 7, Bucket: BucketAIAssisted, WorkType: "bug"}
	params := incidentFixtureParams(triggered, incidentAt("i2", 2*time.Hour))
	params.MainOwners = mainOwners("team-main")
	params.TeamResolver = func(uuid.UUID, string) []string { return []string{"team-main", "team-lesser"} }
	params.OwnerShare = func(_ uuid.UUID, team string) float64 {
		if team == "team-main" {
			return 0.7
		}
		return 0.3
	}
	wantIncidents(t, "main owner", Compute(params), map[incidentRowKey]float64{
		{team: "team-main", workType: "bug", bucket: BucketAIAssisted}:       1,
		{team: "team-main", workType: IncidentWorkType, bucket: BucketHuman}: 1,
	})
}

// A repository no team owns keeps its incidents whole in the no-team rows.
func TestIncidentOfARepositoryNoTeamOwnsCountsWithoutATeam(t *testing.T) {
	params := incidentFixtureParams(incidentAt("i1", time.Hour))
	wantIncidents(t, "no owner", Compute(params), map[incidentRowKey]float64{
		{team: "", workType: IncidentWorkType, bucket: BucketHuman}: 1,
	})
}

// D6041 (b), leg 1: of tied main owners, the one that owned the most recently
// merged pull request before the incident started. A pull request merged after
// the incident does not count, and a pull request of a team that is not tied
// does not count.
func TestTiedMainOwnersGoToTheOwnerOfTheMostRecentlyMergedPullRequest(t *testing.T) {
	incident := incidentAt("i1", 10*time.Hour)
	params := incidentFixtureParams(incident)
	params.MainOwners = mainOwners("team-a", "team-b")
	params.RepoMergedPRTeams = map[uuid.UUID][]MergedPRTeam{fixtureRepo: {
		{TeamID: "team-a", MergedAt: fixtureBase.Add(-48 * time.Hour)},
		{TeamID: "team-b", MergedAt: fixtureBase.Add(-24 * time.Hour)},
		{TeamID: "team-a", MergedAt: fixtureBase.Add(11 * time.Hour)},
		{TeamID: "team-c", MergedAt: fixtureBase.Add(9 * time.Hour)},
	}}
	wantIncidents(t, "most recent", Compute(params), map[incidentRowKey]float64{
		{team: "team-b", workType: IncidentWorkType, bucket: BucketHuman}: 1,
	})
}

// D6041 (b), leg 2: with no such pull request, the lowest team id, whatever the
// order the owners arrive in.
func TestTiedMainOwnersWithoutAPullRequestGoToTheLowestTeamID(t *testing.T) {
	for _, owners := range [][]string{{"team-b", "team-a", "team-c"}, {"team-c", "team-a", "team-b"}, {"team-a", "team-c", "team-b"}} {
		params := incidentFixtureParams(incidentAt("i1", time.Hour))
		params.MainOwners = mainOwners(owners...)
		params.RepoMergedPRTeams = map[uuid.UUID][]MergedPRTeam{fixtureRepo: {
			{TeamID: "team-b", MergedAt: fixtureBase.Add(2 * time.Hour)}, // after the incident
		}}
		wantIncidents(t, fmt.Sprint(owners), Compute(params), map[incidentRowKey]float64{
			{team: "team-a", workType: IncidentWorkType, bucket: BucketHuman}: 1,
		})
	}
}

// CHAOS-9915 as filed: one group mixing a pull request whose attribution row
// names the team (share 1) with one the ownership read gives to the same team at
// a lesser share. The incident count must not depend on the order of the facts,
// and it is never a fraction.
func TestIncidentCountDoesNotDependOnTheOrderOfAMixedGroup(t *testing.T) {
	merged := timePtr(fixtureBase.Add(time.Hour))
	pr := func(number int64) PullRequestRow {
		return PullRequestRow{RepoID: fixtureRepo, Number: number, CreatedAt: fixtureBase, MergedAt: merged}
	}
	named, resolved := pr(1), pr(2)
	for name, prs := range map[string][]PullRequestRow{"named first": {named, resolved}, "resolved first": {resolved, named}} {
		params := incidentFixtureParams(incidentAt("i1", 2*time.Hour))
		params.PullRequests = prs
		params.Attributions = []AttributionRow{
			{RepoID: fixtureRepo, Number: 1, Kind: strPtr("ai_assisted"), WorkType: strPtr("pull_request"), TeamID: strPtr("team-t")},
			{RepoID: fixtureRepo, Number: 2, Kind: strPtr("ai_assisted"), WorkType: strPtr("pull_request")},
		}
		params.TeamResolver = func(uuid.UUID, string) []string { return []string{"team-t", "team-u"} }
		params.OwnerShare = func(_ uuid.UUID, team string) float64 {
			if team == "team-t" {
				return 0.4
			}
			return 0.6
		}
		params.MainOwners = mainOwners("team-u")
		wantIncidents(t, name, Compute(params), map[incidentRowKey]float64{
			{team: "team-u", workType: IncidentWorkType, bucket: BucketHuman}: 1,
		})
	}
}

// An incident anchors on its own row only; the other rows of the same
// repository and day, pull request rows included, carry none of it. A team that
// attribution rows name but that neither owns the triggering pull request nor
// the repository takes none.
func TestIncidentSitsOnOneRowOnlyAndAnAttributionOnlyTeamTakesNone(t *testing.T) {
	merged := timePtr(fixtureBase.Add(time.Hour))
	params := incidentFixtureParams(incidentAt("i1", 2*time.Hour))
	params.PullRequests = []PullRequestRow{
		{RepoID: fixtureRepo, Number: 1, CreatedAt: fixtureBase, MergedAt: merged},
		{RepoID: fixtureRepo, Number: 2, CreatedAt: fixtureBase, MergedAt: merged},
		{RepoID: fixtureRepo, Number: 3, CreatedAt: fixtureBase, MergedAt: merged},
	}
	params.Attributions = []AttributionRow{
		{RepoID: fixtureRepo, Number: 1, Kind: strPtr("ai_assisted"), WorkType: strPtr("bug")},
		{RepoID: fixtureRepo, Number: 2, Kind: strPtr("agent_created"), WorkType: strPtr("story")},
		{RepoID: fixtureRepo, Number: 3, Kind: strPtr("ai_assisted"), WorkType: strPtr("bug"), TeamID: strPtr("team-named")},
	}
	params.TeamResolver = func(uuid.UUID, string) []string { return []string{"team-owner"} }
	params.MainOwners = mainOwners("team-owner")
	trigger := &IncidentTrigger{RepoID: fixtureRepo, Number: 1, Bucket: BucketAIAssisted, WorkType: "bug"}
	params.Incidents[0].Trigger = trigger
	records := Compute(params)
	wantIncidents(t, "one row", records, map[incidentRowKey]float64{
		{team: "team-owner", workType: "bug", bucket: BucketAIAssisted}: 1,
	})
	var rowsOfRepo int
	for _, record := range records {
		rowsOfRepo++
		if record.TeamID != nil && *record.TeamID == "team-named" && record.IncidentsCount != 0 {
			t.Errorf("attribution-only team has incidents_count %v, want 0", record.IncidentsCount)
		}
	}
	if rowsOfRepo < 3 {
		t.Fatalf("only %d rows; the case does not carry the pull request rows it is meant to leave at 0", rowsOfRepo)
	}
}

// incident_drag_rate is the row's own incidents over the row's merged pull
// requests: the incident the row carries, not the repository's count.
func TestIncidentDragRateIsTheRowsIncidentsOverItsMergedPullRequests(t *testing.T) {
	merged := timePtr(fixtureBase.Add(time.Hour))
	incident := incidentAt("i1", 2*time.Hour)
	incident.Trigger = &IncidentTrigger{RepoID: fixtureRepo, Number: 1, TeamID: strPtr("team-t"), Bucket: BucketAIAssisted, WorkType: "bug"}
	params := incidentFixtureParams(incident)
	params.PullRequests = []PullRequestRow{
		{RepoID: fixtureRepo, Number: 1, CreatedAt: fixtureBase, MergedAt: merged},
		{RepoID: fixtureRepo, Number: 2, CreatedAt: fixtureBase, MergedAt: merged},
	}
	params.Attributions = []AttributionRow{
		{RepoID: fixtureRepo, Number: 1, Kind: strPtr("ai_assisted"), WorkType: strPtr("bug"), TeamID: strPtr("team-t")},
		{RepoID: fixtureRepo, Number: 2, Kind: strPtr("ai_assisted"), WorkType: strPtr("bug"), TeamID: strPtr("team-t")},
	}
	var found bool
	for _, record := range Compute(params) {
		if record.AttributionBucket != BucketAIAssisted {
			continue
		}
		found = true
		if record.IncidentsCount != 1 || record.IncidentDragRate == nil || *record.IncidentDragRate != 0.5 {
			t.Errorf("ai_assisted row incidents_count %v rate %v, want 1 and 0.5 (1 incident / 2 merged)", record.IncidentsCount, record.IncidentDragRate)
		}
	}
	if !found {
		t.Fatal("no ai_assisted row")
	}
}

// The property: per repository and day, over every row, the incidents add up to
// the incidents that started that day, each value is a whole number, and the
// answer does not depend on input order. Co-owned repositories, several work
// types and buckets, tied owners, a pull request that is not in the day's facts,
// and incidents on other days are all in the case.
func TestIncidentsAddUpToTheIncidentsStartedAndAreWholeNumbers(t *testing.T) {
	merged := timePtr(fixtureBase.Add(time.Hour))
	build := func() Params {
		params := Params{
			Day: fixtureDay, OrgID: fixtureOrgID,
			RepoNamesByID: map[uuid.UUID]string{fixtureRepo: "acme/alpha", fixtureRepoB: "acme/beta"},
			PullRequests: []PullRequestRow{
				{RepoID: fixtureRepo, Number: 1, CreatedAt: fixtureBase, MergedAt: merged},
				{RepoID: fixtureRepo, Number: 2, CreatedAt: fixtureBase, MergedAt: merged},
				{RepoID: fixtureRepoB, Number: 3, CreatedAt: fixtureBase, MergedAt: merged},
			},
			Attributions: []AttributionRow{
				{RepoID: fixtureRepo, Number: 1, Kind: strPtr("ai_assisted"), WorkType: strPtr("bug")},
				{RepoID: fixtureRepo, Number: 2, Kind: strPtr("agent_created"), WorkType: strPtr("story")},
				{RepoID: fixtureRepoB, Number: 3, Kind: strPtr("ai_review"), WorkType: strPtr("bug"), TeamID: strPtr("team-named")},
			},
			TeamResolver: func(repo uuid.UUID, _ string) []string {
				if repo == fixtureRepo {
					return []string{"team-a", "team-b", "team-c"}
				}
				return []string{"team-d"}
			},
			OwnerShare: func(_ uuid.UUID, team string) float64 {
				return map[string]float64{"team-a": 0.45, "team-b": 0.45, "team-c": 0.1, "team-d": 1}[team]
			},
			MainOwners: func(repo uuid.UUID) []string {
				if repo == fixtureRepo {
					return []string{"team-b", "team-a"}
				}
				return []string{"team-d"}
			},
			RepoMergedPRTeams: map[uuid.UUID][]MergedPRTeam{fixtureRepo: {{TeamID: "team-b", MergedAt: fixtureBase.Add(-time.Hour)}}},
		}
		params.Incidents = []IncidentRow{
			{ID: "1", RepoID: fixtureRepo, StartedAt: fixtureBase.Add(1 * time.Hour), Trigger: &IncidentTrigger{RepoID: fixtureRepo, Number: 1, Bucket: BucketAIAssisted, WorkType: "bug"}},
			{ID: "2", RepoID: fixtureRepo, StartedAt: fixtureBase.Add(2 * time.Hour), Trigger: &IncidentTrigger{RepoID: fixtureRepo, Number: 2, Bucket: BucketAgentCreated, WorkType: "story"}},
			{ID: "3", RepoID: fixtureRepo, StartedAt: fixtureBase.Add(3 * time.Hour)},
			{ID: "4", RepoID: fixtureRepo, StartedAt: fixtureBase.Add(4 * time.Hour), Trigger: &IncidentTrigger{RepoID: fixtureRepo, Number: 99, TeamID: strPtr("team-c"), Bucket: BucketUnknown, WorkType: "pull_request"}},
			{ID: "5", RepoID: fixtureRepoB, StartedAt: fixtureBase.Add(5 * time.Hour), SourceTeamID: strPtr("team-d")},
			{ID: "6", RepoID: fixtureRepoB, StartedAt: fixtureBase.Add(6 * time.Hour), Trigger: &IncidentTrigger{RepoID: fixtureRepoB, Number: 3, Bucket: BucketAIReview, WorkType: "bug"}},
			{ID: "7", RepoID: fixtureRepoB, StartedAt: fixtureBase.Add(-time.Hour)},
			{ID: "8", RepoID: fixtureRepoB, StartedAt: fixtureBase.Add(30 * time.Hour)},
		}
		return params
	}
	const started = 6
	canonical := func(records []Record) string {
		counts := incidentsByRow(records)
		keys := make([]string, 0, len(counts))
		for key, value := range counts {
			keys = append(keys, fmt.Sprintf("%v=%v", key, value))
		}
		sort.Strings(keys)
		return fmt.Sprint(keys)
	}
	want := canonical(Compute(build()))
	random := rand.New(rand.NewSource(20261016))
	for attempt := 0; attempt < 30; attempt++ {
		params := build()
		random.Shuffle(len(params.Incidents), func(i, j int) { params.Incidents[i], params.Incidents[j] = params.Incidents[j], params.Incidents[i] })
		random.Shuffle(len(params.PullRequests), func(i, j int) {
			params.PullRequests[i], params.PullRequests[j] = params.PullRequests[j], params.PullRequests[i]
		})
		owners := params.MainOwners
		flip := attempt%2 == 0
		params.MainOwners = func(repo uuid.UUID) []string {
			list := owners(repo)
			if flip && len(list) > 1 {
				list[0], list[1] = list[1], list[0]
			}
			return list
		}
		records := Compute(params)
		var sum float64
		for _, record := range records {
			if record.IncidentsCount != math.Trunc(record.IncidentsCount) || record.IncidentsCount < 0 {
				t.Fatalf("attempt %d: incidents_count %v of %+v is not a whole number", attempt, record.IncidentsCount, record)
			}
			sum += record.IncidentsCount
		}
		if sum != started {
			t.Fatalf("attempt %d: incidents_count adds up to %v, want the %d incidents that started", attempt, sum, started)
		}
		if got := canonical(records); got != want {
			t.Fatalf("attempt %d: input order changed the answer:\n%s\n%s", attempt, got, want)
		}
	}
}

package providersync

import (
	"encoding/json"
	"strconv"
)

// githubTestsReportSlot is where one artifact's report rows belong in a run (CHAOS-13873): the artifact name, which a re-run job
// uploads again under a new artifact id, and the run attempt that made the artifact. Readers take a run's totals from the newest
// attempt of each slot and keep the case rows of every attempt for the flake measures.
type githubTestsReportSlot struct {
	Name    string
	Attempt int64
}

// scope is the value the natural keys hash in place of the artifact id. It is a digest, so a slot name holding "::" cannot meet
// another slot's keys through hashTestIdentifier's unescaped join.
func (slot githubTestsReportSlot) scope() string {
	return hashTestIdentifier("github-report-slot", slot.Name, strconv.FormatInt(slot.Attempt, 10))
}

const githubTestsExclusionReasonSuperseded = "superseded_by_newer_artifact"

// githubTestsReportSlotFor labels an artifact of run. GitHub's run_started_at is the start of the run's LATEST attempt: an artifact
// made at or after it belongs to that attempt; an older one was carried over from an earlier attempt and is labelled one attempt
// lower. A carried-over artifact read again in a later attempt can so get a higher label than before; its rows then repeat the same
// bytes at that label, and the newest label of the slot still holds the newest bytes. A missing or unreadable time labels the
// artifact with the run's attempt; the run's attempt defaults to 1.
func githubTestsReportSlotFor(run gitHubWorkflowRunPayload, artifact githubTestsArtifactPayload) githubTestsReportSlot {
	name := githubTestsArtifactSlotName(artifact)
	attempt, err := json.Number(stringValue(run.RunAttempt)).Int64()
	if err != nil || attempt < 1 {
		attempt = 1
	}
	created, started := parseGitHubWorkflowTime(artifact.CreatedAt), parseGitHubWorkflowTime(run.RunStartedAt)
	if created != nil && started != nil && created.Before(*started) && attempt > 1 {
		attempt--
	}
	return githubTestsReportSlot{Name: name, Attempt: attempt}
}

// githubTestsKeepNewestArtifactPerName keeps, of the artifacts that share a name, the one with the highest id (the newest upload)
// and returns the others as superseded. A name appears twice only across attempts of one run (an attempt cannot upload two
// artifacts of one name), so this keeps one report per slot in a pass and keeps the scope unique per download (CHAOS-4190).
func githubTestsKeepNewestArtifactPerName(items []githubTestsArtifactPayload) (kept, superseded []githubTestsArtifactPayload) {
	newest := make(map[string]int, len(items))
	for index, item := range items {
		name := githubTestsArtifactSlotName(item)
		if current, seen := newest[name]; !seen || githubTestsArtifactIDLess(items[current].ID, item.ID) {
			newest[name] = index
		}
	}
	kept = make([]githubTestsArtifactPayload, 0, len(items))
	for index, item := range items {
		if newest[githubTestsArtifactSlotName(item)] == index {
			kept = append(kept, item)
			continue
		}
		superseded = append(superseded, item)
	}
	return kept, superseded
}

// githubTestsArtifactSlotName is the artifact's name, or its id when the listing gives no name.
func githubTestsArtifactSlotName(artifact githubTestsArtifactPayload) string {
	if artifact.Name == "" {
		return string(artifact.ID)
	}
	return artifact.Name
}

package issueprlinks

import (
	"testing"
	"time"
)

// compareToGolden is the comparison TestDeriveMatchesFrozenPythonGolden
// Exhaustively performs, factored out so it can itself be falsified. It
// returns one description per divergence; an empty slice means parity.
//
// The comparison is by the table's OWN dedup identity
// (work_item_id, repo_id, pr_number) -- what ReplacingMergeTree collapses on --
// and then field-by-field, so a divergence in any column is reported rather
// than only a count mismatch.
func compareToGolden(got, want []Link) []string {
	var problems []string
	if len(got) != len(want) {
		problems = append(problems, "row count differs")
	}
	index := make(map[identity]Link, len(want))
	for _, link := range want {
		index[identity{workItemID: link.WorkItemID, repoID: link.RepoID, prNumber: link.PRNumber}] = link
	}
	for _, actual := range got {
		key := identity{workItemID: actual.WorkItemID, repoID: actual.RepoID, prNumber: actual.PRNumber}
		expected, ok := index[key]
		if !ok {
			problems = append(problems, "unexpected identity "+actual.WorkItemID)
			continue
		}
		switch {
		case actual.OrgID != expected.OrgID:
			problems = append(problems, "org_id differs for "+actual.WorkItemID)
		case actual.Confidence != expected.Confidence:
			problems = append(problems, "confidence differs for "+actual.WorkItemID)
		case actual.Provenance != expected.Provenance:
			problems = append(problems, "provenance differs for "+actual.WorkItemID)
		case actual.Evidence != expected.Evidence:
			problems = append(problems, "evidence differs for "+actual.WorkItemID)
		case !actual.LastSynced.Equal(expected.LastSynced):
			problems = append(problems, "last_synced differs for "+actual.WorkItemID)
		}
	}
	return problems
}

// TestComparatorIsFalsifiable plants a defect of each class this port could
// plausibly ship and proves the golden comparison CATCHES it.
//
// This is not ceremony. `go-api-epic.md:82` makes it mandatory practice: a
// parity result is worth nothing until the comparator has been shown to fail on
// a known-bad input, because a comparator that silently passes everything
// produces exactly the same green as a correct port. The golden went green on
// its first run; this is what makes that green mean something.
func TestComparatorIsFalsifiable(t *testing.T) {
	golden := loadGolden(t)
	inputs := golden.inputs(t)
	expected := golden.expectedLinks(t)

	// Sanity: the unperturbed comparison must be clean, or every case below
	// would "catch" a defect that was already there.
	if problems := compareToGolden(Derive(inputs).Links, expected); len(problems) != 0 {
		t.Fatalf("baseline comparison is not clean: %v", problems)
	}

	t.Run("dropped work-item existence gate is NOT falsifiable by this golden", func(t *testing.T) {
		// Recorded as a MEASURED LIMIT of the golden, not as a passing check.
		//
		// Dropping builder.py:754-755 -- declaring every dependency target to
		// exist -- changes nothing on org 70d529e0's data, because that gate
		// rejects zero rows there (see TestGoldenRejectionBreakdown: the whole
		// 6,365 splits into 2,493 written, 3,548 not_admissible, 298
		// unknown_repo, 26 pr_not_found, and nothing else). Every dependency
		// whose target is unsynced is already rejected earlier.
		//
		// So the golden cannot falsify this gate, and claiming it does would be
		// exactly the "test that cannot fail" this suite exists to avoid.
		// Coverage for it lives in TestDeriveRejectsUnknownWorkItem, on
		// synthetic input that actually exercises it.
		widened := inputs
		widened.WorkItems = append([]WorkItemRow(nil), inputs.WorkItems...)
		for _, dependency := range inputs.Dependencies {
			if dependency.TargetWorkItemID == "" {
				continue
			}
			widened.WorkItems = append(widened.WorkItems, WorkItemRow{
				OrgID:      dependency.OrgID,
				WorkItemID: dependency.TargetWorkItemID,
			})
		}
		if problems := compareToGolden(Derive(widened).Links, expected); len(problems) != 0 {
			t.Fatalf(
				"the golden's work-item set is no longer saturated -- this gate now DOES change the "+
					"output (%v), so promote this case to a real falsification check", problems,
			)
		}
	})

	t.Run("dropped pull-request existence gate", func(t *testing.T) {
		// builder.py:761-762. Emulated by declaring every parseable
		// (repo, number) pair to exist -- including PRs outside the build's
		// window, which is the subtler half of this gate.
		widened := inputs
		widened.PullRequests = append([]PullRequestRow(nil), inputs.PullRequests...)
		repos := make(map[[2]string]struct{}, len(inputs.Repos))
		byslug := make(map[[2]string]RepoRow, len(inputs.Repos))
		for _, repo := range inputs.Repos {
			key := [2]string{rowOrgID(repo.OrgID, inputs.OrgID), repo.Repo}
			repos[key] = struct{}{}
			byslug[key] = repo
		}
		for _, dependency := range inputs.Dependencies {
			source, ok := ParsePRSource(dependency.SourceWorkItemID)
			if !ok {
				continue
			}
			key := [2]string{rowOrgID(dependency.OrgID, inputs.OrgID), source.RepoSlug}
			repo, known := byslug[key]
			if !known {
				continue
			}
			widened.PullRequests = append(widened.PullRequests, PullRequestRow{
				OrgID:  rowOrgID(dependency.OrgID, inputs.OrgID),
				RepoID: repo.ID,
				Number: source.PRNumber,
			})
		}
		problems := compareToGolden(Derive(widened).Links, expected)
		if len(problems) == 0 {
			t.Fatal("dropping the pull-request gate went undetected")
		}
	})

	t.Run("off-by-one pr_number", func(t *testing.T) {
		mutated := append([]Link(nil), Derive(inputs).Links...)
		mutated[0].PRNumber++
		if problems := compareToGolden(mutated, expected); len(problems) == 0 {
			t.Fatal("an off-by-one pr_number went undetected")
		}
	})

	t.Run("evidence falls back to provider_attachment", func(t *testing.T) {
		// The unreachable branch of evidenceFor. If a refactor ever made it
		// reachable, every row's evidence would silently become
		// "github_attachment" instead of the provider's own raw kind.
		mutated := append([]Link(nil), Derive(inputs).Links...)
		for index := range mutated {
			mutated[index].Evidence = ProviderGitHub + "_attachment"
		}
		if problems := compareToGolden(mutated, expected); len(problems) == 0 {
			t.Fatal("an evidence-column regression went undetected")
		}
	})

	t.Run("last_synced stamped with now", func(t *testing.T) {
		// CHAOS-4769's hazard, planted deliberately: stamping build time rather
		// than the dependency row's own last_synced changes which row wins the
		// ReplacingMergeTree collapse against the Python fallback writers.
		mutated := append([]Link(nil), Derive(inputs).Links...)
		now := time.Date(2026, 9, 1, 12, 0, 0, 0, time.UTC)
		for index := range mutated {
			mutated[index].LastSynced = now
		}
		if problems := compareToGolden(mutated, expected); len(problems) == 0 {
			t.Fatal("a last_synced regression went undetected")
		}
	})

	t.Run("confidence widened", func(t *testing.T) {
		mutated := append([]Link(nil), Derive(inputs).Links...)
		mutated[0].Confidence = 0.9
		if problems := compareToGolden(mutated, expected); len(problems) == 0 {
			t.Fatal("a confidence regression went undetected")
		}
	})

	t.Run("a dropped row", func(t *testing.T) {
		full := Derive(inputs).Links
		if problems := compareToGolden(full[1:], expected); len(problems) == 0 {
			t.Fatal("a dropped row went undetected")
		}
	})
}

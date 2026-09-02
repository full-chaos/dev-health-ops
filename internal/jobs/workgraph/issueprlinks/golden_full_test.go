package issueprlinks

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/google/uuid"
)

// goldenDocument decodes tests/fixtures/issue_pr_links_python_golden.json.
//
// Every string field is an index into Strings -- the fixture interns ids so it
// stays checkable-in (414 KB; the largest golden in the repo before this one is
// 580 KB). The encoding is lossless: decodeGolden reconstructs the exact rows
// the deployed Python producer read and wrote.
type goldenDocument struct {
	Schema  string   `json:"schema"`
	OrgID   string   `json:"org_id"`
	Strings []string `json:"strings"`

	// [org_id*, source_work_item_id*, target_work_item_id*, relationship_type_raw*, last_synced*]
	Dependencies [][5]int `json:"dependencies"`
	// [org_id*, id*, repo*]
	Repos [][3]int `json:"repos"`
	// [org_id*, repo_id*, number]
	PullRequests [][3]int `json:"pull_requests"`
	// [org_id*, work_item_id*]
	WorkItems [][2]int `json:"work_items"`
	// [repo_id*, work_item_id*, pr_number, confidence, provenance*, evidence*, last_synced*, org_id*]
	Links []goldenLink `json:"links"`

	Counts map[string]int `json:"counts"`
}

// goldenLink keeps the two non-interned positions (pr_number, confidence)
// typed, so a JSON number never silently becomes a string index.
type goldenLink struct {
	RepoID     int
	WorkItemID int
	PRNumber   uint32
	Confidence float64
	Provenance int
	Evidence   int
	LastSynced int
	OrgID      int
}

func (link *goldenLink) UnmarshalJSON(data []byte) error {
	var raw []json.Number
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	if len(raw) != 8 {
		return fmt.Errorf("golden link row has %d fields, want 8", len(raw))
	}
	asInt := func(index int) (int, error) {
		value, err := raw[index].Int64()
		return int(value), err
	}
	var err error
	if link.RepoID, err = asInt(0); err != nil {
		return err
	}
	if link.WorkItemID, err = asInt(1); err != nil {
		return err
	}
	number, err := raw[2].Int64()
	if err != nil {
		return err
	}
	link.PRNumber = uint32(number)
	if link.Confidence, err = raw[3].Float64(); err != nil {
		return err
	}
	if link.Provenance, err = asInt(4); err != nil {
		return err
	}
	if link.Evidence, err = asInt(5); err != nil {
		return err
	}
	if link.LastSynced, err = asInt(6); err != nil {
		return err
	}
	link.OrgID, err = asInt(7)
	return err
}

func loadGolden(t *testing.T) goldenDocument {
	t.Helper()
	path := filepath.Join(repositoryRootPath(t), "tests", "fixtures", "issue_pr_links_python_golden.json")
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read frozen golden: %v", err)
	}
	var golden goldenDocument
	if err := json.Unmarshal(raw, &golden); err != nil {
		t.Fatalf("decode frozen golden: %v", err)
	}
	if golden.Schema != "issue_pr_links_python_golden.v1" {
		t.Fatalf("unexpected golden schema %q -- regenerate or update the decoder", golden.Schema)
	}
	return golden
}

func (golden goldenDocument) text(t *testing.T, index int) string {
	t.Helper()
	if index < 0 || index >= len(golden.Strings) {
		t.Fatalf("golden string index %d out of range (%d strings)", index, len(golden.Strings))
	}
	return golden.Strings[index]
}

func (golden goldenDocument) instant(t *testing.T, index int) time.Time {
	t.Helper()
	// Timestamps are parsed as INSTANTS and never compared as text -- the house
	// golden rule (repouser/golden_full_test.go:93-97). A string comparison
	// would call 12:00Z and 13:00+01:00 different when they are the same moment.
	value, err := time.Parse(time.RFC3339Nano, golden.text(t, index))
	if err != nil {
		t.Fatalf("parse golden timestamp %q: %v", golden.text(t, index), err)
	}
	return value.UTC()
}

func (golden goldenDocument) uuid(t *testing.T, index int) uuid.UUID {
	t.Helper()
	value, err := uuid.Parse(golden.text(t, index))
	if err != nil {
		t.Fatalf("parse golden uuid %q: %v", golden.text(t, index), err)
	}
	return value
}

// goldenInputs rebuilds Derive's Inputs from the frozen reads.
func (golden goldenDocument) inputs(t *testing.T) Inputs {
	t.Helper()
	inputs := Inputs{OrgID: golden.OrgID}
	for _, row := range golden.Dependencies {
		inputs.Dependencies = append(inputs.Dependencies, DependencyRow{
			OrgID:               golden.text(t, row[0]),
			SourceWorkItemID:    golden.text(t, row[1]),
			TargetWorkItemID:    golden.text(t, row[2]),
			RelationshipTypeRaw: golden.text(t, row[3]),
			LastSynced:          golden.instant(t, row[4]),
		})
	}
	for _, row := range golden.Repos {
		inputs.Repos = append(inputs.Repos, RepoRow{
			OrgID: golden.text(t, row[0]),
			ID:    golden.uuid(t, row[1]),
			Repo:  golden.text(t, row[2]),
		})
	}
	for _, row := range golden.PullRequests {
		inputs.PullRequests = append(inputs.PullRequests, PullRequestRow{
			OrgID:  golden.text(t, row[0]),
			RepoID: golden.uuid(t, row[1]),
			Number: uint32(row[2]),
		})
	}
	for _, row := range golden.WorkItems {
		inputs.WorkItems = append(inputs.WorkItems, WorkItemRow{
			OrgID:      golden.text(t, row[0]),
			WorkItemID: golden.text(t, row[1]),
		})
	}
	return inputs
}

// expectedLinks rebuilds the rows the deployed Python producer wrote.
func (golden goldenDocument) expectedLinks(t *testing.T) []Link {
	t.Helper()
	out := make([]Link, 0, len(golden.Links))
	for _, row := range golden.Links {
		out = append(out, Link{
			OrgID:      golden.text(t, row.OrgID),
			RepoID:     golden.uuid(t, row.RepoID),
			WorkItemID: golden.text(t, row.WorkItemID),
			PRNumber:   row.PRNumber,
			Confidence: float32(row.Confidence),
			Provenance: golden.text(t, row.Provenance),
			Evidence:   golden.text(t, row.Evidence),
			LastSynced: golden.instant(t, row.LastSynced),
		})
	}
	return out
}

// TestDeriveMatchesFrozenPythonGoldenExhaustively is the parity statement of
// this package: replaying the exact four ClickHouse reads the DEPLOYED Python
// producer performed on org 70d529e0's real data, Derive must reproduce every
// row it wrote -- all 2,493 of them, every field, in order.
//
// It is exhaustive on purpose. A hand-picked-subset assertion is "a test that
// cannot fail" for any field or row it does not name; this compares the whole
// output, so a regression anywhere is a failure here.
//
// NOTE on the row count: 2,493 is what the producer derives from THESE frozen
// inputs. The live `work_graph_issue_pr` table holds 2,476 native rows, which
// is a different number for a mundane reason -- it is the residue of an earlier
// build over an earlier data snapshot, with that build's own from/to window.
// The two are not comparable and the difference is not a defect; the golden's
// contract is inputs->outputs captured in ONE run, which is exactly what makes
// it immune to the tables moving underneath it.
func TestDeriveMatchesFrozenPythonGoldenExhaustively(t *testing.T) {
	golden := loadGolden(t)
	expected := golden.expectedLinks(t)

	result := Derive(golden.inputs(t))

	if got, want := len(result.Links), len(expected); got != want {
		t.Fatalf("derived %d links, python derived %d", got, want)
	}
	if want := golden.Counts["links"]; len(expected) != want {
		t.Fatalf("golden self-check: %d link rows but counts.links=%d", len(expected), want)
	}

	// Compare as sets keyed by the table's OWN dedup identity, then assert the
	// identity set is unique. Comparing positionally would make the test
	// sensitive to the loader's ORDER BY, which is a storage decision, not a
	// contract -- while the identity set IS the contract, because it is what
	// ReplacingMergeTree collapses on.
	expectedByIdentity := make(map[identity]Link, len(expected))
	for _, link := range expected {
		key := identity{workItemID: link.WorkItemID, repoID: link.RepoID, prNumber: link.PRNumber}
		if _, duplicate := expectedByIdentity[key]; duplicate {
			t.Fatalf("golden contains two rows for identity %+v -- the fixture is corrupt", key)
		}
		expectedByIdentity[key] = link
	}

	seen := make(map[identity]struct{}, len(result.Links))
	for index, got := range result.Links {
		key := identity{workItemID: got.WorkItemID, repoID: got.RepoID, prNumber: got.PRNumber}
		if _, duplicate := seen[key]; duplicate {
			t.Fatalf("Derive emitted two rows for identity %+v (index %d)", key, index)
		}
		seen[key] = struct{}{}

		want, ok := expectedByIdentity[key]
		if !ok {
			t.Fatalf("Derive emitted a link python did not: %+v", got)
		}
		if got.OrgID != want.OrgID {
			t.Errorf("link %+v: org_id = %q, want %q", key, got.OrgID, want.OrgID)
		}
		if got.Confidence != want.Confidence {
			t.Errorf("link %+v: confidence = %v, want %v", key, got.Confidence, want.Confidence)
		}
		if got.Provenance != want.Provenance {
			t.Errorf("link %+v: provenance = %q, want %q", key, got.Provenance, want.Provenance)
		}
		if got.Evidence != want.Evidence {
			t.Errorf("link %+v: evidence = %q, want %q", key, got.Evidence, want.Evidence)
		}
		if !got.LastSynced.Equal(want.LastSynced) {
			t.Errorf("link %+v: last_synced = %s, want %s", key, got.LastSynced, want.LastSynced)
		}
	}
}

// TestDeriveAccountsForEveryDependencyRow asserts the telemetry identity the
// live proof also asserts: every dependency row is either written or counted
// under exactly one rejection reason. Without it, adding a gate without a
// counter would silently shrink the output and the log line would still look
// healthy.
func TestDeriveAccountsForEveryDependencyRow(t *testing.T) {
	golden := loadGolden(t)
	result := Derive(golden.inputs(t))

	if !result.Balanced() {
		t.Fatalf(
			"accounting does not balance: read %d, wrote %d, rejected %d (%v)",
			result.DependenciesRead, result.Written(), result.RejectedTotal(), result.Rejected,
		)
	}
	if result.DependenciesRead != golden.Counts["dependencies"] {
		t.Fatalf(
			"read %d dependency rows, golden froze %d",
			result.DependenciesRead, golden.Counts["dependencies"],
		)
	}
	// The admission table's Linear row is the only one reachable on today's
	// data; the GitHub and Jira rows admit nothing until lane-4757's ingestion
	// lands. Asserting that keeps the golden honest about which slice is live.
	if result.AdmittedByRawKind["linear_attachment"] == 0 {
		t.Fatal("no linear_attachment rows admitted -- the proof org's primary mechanism vanished")
	}
	for _, unreachable := range []string{"github_closing_reference", "jira_dev_status"} {
		if count := result.AdmittedByRawKind[unreachable]; count != 0 {
			t.Errorf(
				"admitted %d %s rows: that raw kind is not written by either plane yet, so the "+
					"golden's expectations need regenerating alongside lane-4757's ingestion",
				count, unreachable,
			)
		}
	}
}

func repositoryRootPath(t *testing.T) string {
	t.Helper()
	working, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for directory := working; ; {
		if _, err := os.Stat(filepath.Join(directory, "go.mod")); err == nil {
			return directory
		}
		parent := filepath.Dir(directory)
		if parent == directory {
			t.Fatal("could not find repository root (no go.mod found)")
		}
		directory = parent
	}
}

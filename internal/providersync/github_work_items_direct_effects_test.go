package providersync

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
	"time"

	chdriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
)

// The evidence column is a String, so Go and Python agree only if they produce
// the same BYTES. Decoding both sides and comparing the decoded maps would pass
// while the stored strings differ in key order, separator spacing, or non-ASCII
// escaping -- which is exactly the class of divergence this encoder exists to
// prevent. Every case therefore executes the real CPython json.dumps and
// compares the raw string.
func TestPythonEvidenceJSONMatchesRealJSONDumpsByteForByte(t *testing.T) {
	python := pythonExecutable(t)
	cases := []struct {
		name     string
		evidence map[string]any
		// pythonLiteral is the dict literal in the same INSERTION ORDER the
		// Python detector builds it, because that order is what json.dumps
		// serialises and what the stored bytes therefore contain.
		pythonLiteral string
	}{
		{
			name:          "label",
			evidence:      map[string]any{"label": "ai-generated"},
			pythonLiteral: `{"label": "ai-generated"}`,
		},
		{
			// _ai_detection.detect_from_author builds login, user_type,
			// app_slug, known_ai_bot -- NOT alphabetical order. Marshalling the
			// Go map directly would sort these and store different bytes.
			name: "bot author disagrees with alphabetical order",
			evidence: map[string]any{
				"login": "copilot[bot]", "user_type": "Bot",
				"app_slug": "github-copilot", "known_ai_bot": true,
			},
			pythonLiteral: `{"login": "copilot[bot]", "user_type": "Bot", "app_slug": "github-copilot", "known_ai_bot": True}`,
		},
		{
			name: "commit trailer",
			evidence: map[string]any{
				"trailer_key": "Co-authored-by", "trailer_value": "Claude <noreply@anthropic.com>",
			},
			pythonLiteral: `{"trailer_key": "Co-authored-by", "trailer_value": "Claude <noreply@anthropic.com>"}`,
		},
		{
			name: "branch",
			evidence: map[string]any{
				"branch": "feat/ai-thing", "matched_pattern": `(?i)\bai\b`,
			},
			pythonLiteral: `{"branch": "feat/ai-thing", "matched_pattern": "(?i)\\bai\\b"}`,
		},
		{
			// detect_from_pr_body builds matched_text first; alphabetical order
			// would put matched_pattern first.
			name: "pr body disagrees with alphabetical order",
			evidence: map[string]any{
				"matched_text": "Generated with Claude", "matched_pattern": "Generated with",
			},
			pythonLiteral: `{"matched_text": "Generated with Claude", "matched_pattern": "Generated with"}`,
		},
		{
			// Python's json.dumps defaults to ensure_ascii=True and does NOT
			// escape <, > or &; Go's encoding/json does the opposite on both
			// counts. A PR title carrying either is ordinary, not exotic.
			name: "non-ascii and html characters",
			evidence: map[string]any{
				"label": "ai-généré <tag> & more … 🤖",
			},
			pythonLiteral: `{"label": "ai-g\u00e9n\u00e9r\u00e9 <tag> & more \u2026 \U0001F916"}`,
		},
		{
			name:          "control characters and quotes",
			evidence:      map[string]any{"label": "line\nbreak\t\"quoted\"\\slash\x01"},
			pythonLiteral: `{"label": "line\nbreak\t\"quoted\"\\slash\x01"}`,
		},
		{
			name: "null actor value",
			evidence: map[string]any{
				"login": "someone", "user_type": (*string)(nil),
				"app_slug": (*string)(nil), "known_ai_bot": false,
			},
			pythonLiteral: `{"login": "someone", "user_type": None, "app_slug": None, "known_ai_bot": False}`,
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			goEncoded, err := pythonEvidenceJSON(testCase.evidence)
			if err != nil {
				t.Fatalf("pythonEvidenceJSON: %v", err)
			}
			// Ask CPython what it stores for the same dict, in the same order.
			script := "import json,sys; sys.stdout.write(json.dumps(" +
				testCase.pythonLiteral + ", default=str))"
			output, err := exec.Command(python, "-c", script).CombinedOutput()
			if err != nil {
				t.Fatalf("execute python json.dumps: %v: %s", err, output)
			}
			if string(output) != goEncoded {
				t.Fatalf("stored evidence bytes diverge from Python\npython=%s\ngo    =%s",
					output, goEncoded)
			}
		})
	}
}

// A shape the encoder has not been shown must not be guessed at. Falling back
// to Go's sorted-key marshalling would store bytes nobody reviewed, and a test
// that decoded the evidence before comparing could never see it.
func TestPythonEvidenceJSONFailsClosedOnUnknownShape(t *testing.T) {
	t.Parallel()
	unknown := []map[string]any{
		{"surprise": "value"},
		{"label": "x", "extra": "y"},
		{},
		nil,
	}
	for _, evidence := range unknown {
		if encoded, err := pythonEvidenceJSON(evidence); err == nil {
			t.Fatalf("unknown evidence shape %v encoded as %q instead of failing", evidence, encoded)
		}
	}
}

// A value type the encoder cannot reproduce must be refused rather than passed
// to Python's `default=str`, whose output this code has not been shown.
func TestPythonEvidenceJSONFailsClosedOnUnreproducibleValueType(t *testing.T) {
	t.Parallel()
	if _, err := pythonEvidenceJSON(map[string]any{"label": 42}); err == nil {
		t.Fatal("integer evidence value encoded instead of failing closed")
	}
	if _, err := pythonEvidenceJSON(map[string]any{"label": []string{"a"}}); err == nil {
		t.Fatal("slice evidence value encoded instead of failing closed")
	}
}

// The committer replays Absent and hard-stops Conflict, so a duplicated key
// answering Absent would be rewritten forever. This pins that mapping.
//
// Note the honest scope: this arm is unreachable in all seven adapters today
// (the readbacks fence the full sorting key and collapse), so it is
// defense-in-depth against a future WHERE/collapse change, not the live
// duplicate protection. The live protection is dedupeBySortingKey, covered by
// TestDedupeBySortingKeyKeepsTheRowClickHouseRetains and the same-key
// integration cases.
func TestWorkItemReadbackVerdictResolvesDuplicatesBeforeAbsence(t *testing.T) {
	t.Parallel()
	if verdict, final := workItemReadbackVerdict(2); !final || verdict != EffectConflict {
		t.Fatalf("two rows: verdict=%s final=%v, want conflict/final", verdict, final)
	}
	if verdict, final := workItemReadbackVerdict(9); !final || verdict != EffectConflict {
		t.Fatalf("nine rows: verdict=%s final=%v, want conflict/final", verdict, final)
	}
	if verdict, final := workItemReadbackVerdict(0); !final || verdict != EffectAbsent {
		t.Fatalf("no rows: verdict=%s final=%v, want absent/final", verdict, final)
	}
	if _, final := workItemReadbackVerdict(1); final {
		t.Fatal("exactly one row must not be final -- the caller still has to compare it")
	}
}

func TestWorkItemVersionVerdictSeparatesStaleFromOverwritten(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, 8, 6, 12, 0, 0, 0, time.UTC)
	if verdict, final := workItemVersionVerdict(time.Time{}, now); !final || verdict != EffectAbsent {
		t.Fatalf("zero version: verdict=%s final=%v", verdict, final)
	}
	if verdict, final := workItemVersionVerdict(now.Add(-time.Hour), now); !final || verdict != EffectAbsent {
		t.Fatalf("older stored version must replay: verdict=%s final=%v", verdict, final)
	}
	if verdict, final := workItemVersionVerdict(now.Add(time.Hour), now); !final || verdict != EffectConflict {
		t.Fatalf("newer stored version must not replay: verdict=%s final=%v", verdict, final)
	}
	if _, final := workItemVersionVerdict(now, now); final {
		t.Fatal("equal versions must fall through to field comparison")
	}
	// A stored version in a different zone but the same instant is the same
	// version; comparing wall-clock fields rather than instants would call it
	// a conflict and hard-stop the unit.
	zoned := now.In(time.FixedZone("west", -7*60*60))
	if _, final := workItemVersionVerdict(zoned, now); final {
		t.Fatal("same instant in another zone must compare equal")
	}
}

// A partly-landed batch cannot be expressed to the committer: replaying it
// would rewrite the rows that already landed. Conflict is the only safe answer.
func TestFoldWorkItemInspectionsTreatsMixedBatchAsConflict(t *testing.T) {
	t.Parallel()
	verdicts := []EffectInspection{EffectExact, EffectAbsent}
	index := 0
	folded, err := foldWorkItemInspections([]int{0, 1}, func(int) (EffectInspection, error) {
		verdict := verdicts[index]
		index++
		return verdict, nil
	})
	if err != nil || folded != EffectConflict {
		t.Fatalf("mixed batch folded to %s (err=%v), want conflict", folded, err)
	}
}

// work_items omits description/priority_raw/service_class/due_at and
// work_item_transitions omits provider. Those omissions are the running Python
// system's behavior (D16), and a ReplacingMergeTree replaces whole rows, so
// writing them would diverge from what production stores. This pins the column
// lists against a well-meaning "the table has the column, fill it in" edit.
func TestDirectAdapterColumnListsMirrorThePythonSinkOmissions(t *testing.T) {
	t.Parallel()
	for _, absent := range []string{"description", "priority_raw", "service_class", "due_at"} {
		if containsColumn(gitHubWorkItemsInsert, absent) {
			t.Fatalf("work_items insert writes %q, which write_work_items does not", absent)
		}
		if containsColumn(gitHubWorkItemsSelect, absent) {
			t.Fatalf("work_items readback compares %q, a column this unit does not own", absent)
		}
	}
	if containsColumn(gitHubWorkItemTransitionsInsert, "provider") {
		t.Fatal("work_item_transitions insert writes provider, which write_work_item_transitions does not")
	}
}

// Every readback must fence the tenant. A missing org_id predicate on a table
// whose sorting key leads with org_id reads another tenant's row as ours.
func TestEveryDirectReadbackFencesOrgID(t *testing.T) {
	t.Parallel()
	selects := map[string]string{
		"work_items":              gitHubWorkItemsSelect,
		"work_item_transitions":   gitHubWorkItemTransitionsSelect,
		"work_item_dependencies":  gitHubWorkItemDependenciesSelect,
		"work_item_reopen_events": gitHubWorkItemReopenEventsSelect,
		"work_item_interactions":  gitHubWorkItemInteractionsSelect,
		"sprints":                 gitHubSprintsSelect,
		"ai_attribution":          gitHubAIAttributionSelectBase,
	}
	for destination, query := range selects {
		if !containsColumn(query, "org_id = ?") {
			t.Fatalf("%s readback does not fence org_id: %s", destination, query)
		}
		// Every readback must resolve ReplacingMergeTree versions somehow. The
		// six unpartitioned destinations use FINAL; ai_attribution resolves the
		// winner explicitly because FINAL's answer there depends on a server
		// knob (see below).
		resolves := containsColumn(query, "FINAL") ||
			containsColumn(gitHubAIAttributionSelectResolve, "LIMIT 1 BY") &&
				destination == "ai_attribution"
		if !resolves {
			t.Fatalf("%s readback does not resolve ReplacingMergeTree versions: %s", destination, query)
		}
	}
	// ai_attribution is the only PARTITIONED destination and its partition
	// expression is not in its sorting key. It deliberately does NOT fence the
	// partition: measured against a real ClickHouse, the default
	// do_not_merge_across_partitions_select_final=0 makes FINAL merge ACROSS
	// partitions, so the sorting key has exactly one current row and a partition
	// predicate would filter that winner out -- answering Absent for a correctly
	// superseded row, which replays forever. `found > 1` carries the duplicate
	// protection instead and is correct under both settings.
	//
	// Pinned here so the fence cannot be reintroduced as an "obvious fix" for
	// PR #1535 without confronting the measurement that rejected it.
	if containsColumn(gitHubAIAttributionSelectBase, "toYYYYMM") {
		t.Fatal("ai_attribution readback reintroduced a partition fence; see the " +
			"measurement recorded in github_work_items_ai_attribution_effects_clickhouse.go")
	}
	// It must also not use FINAL: FINAL's row count for a superseded row across
	// a month boundary depends on do_not_merge_across_partitions_select_final,
	// a server knob this code does not control, which would make the verdict
	// settings-dependent. The winner is resolved explicitly instead.
	if containsColumn(gitHubAIAttributionSelectBase, "FINAL") {
		t.Fatal("ai_attribution readback uses FINAL, making its verdict depend on " +
			"do_not_merge_across_partitions_select_final")
	}
	if !containsColumn(gitHubAIAttributionSelectResolve, "LIMIT 1 BY org_id, provider, subject_type, repo_id, subject_id, source") {
		t.Fatal("ai_attribution readback no longer resolves one row per sorting key")
	}
	if !containsColumn(gitHubAIAttributionSelectResolve, "ORDER BY computed_at DESC") {
		t.Fatal("ai_attribution readback no longer resolves by the RMT version column")
	}
}

func containsColumn(query, needle string) bool {
	return len(query) >= len(needle) && indexOf(query, needle) >= 0
}

func indexOf(haystack, needle string) int {
	for index := 0; index+len(needle) <= len(haystack); index++ {
		if haystack[index:index+len(needle)] == needle {
			return index
		}
	}
	return -1
}

// The dispatcher must refuse an effect aimed at a different destination even
// when the payload decodes cleanly, or a mis-wired slot would write one
// surface's rows into another's table.
func TestDirectAdaptersRejectForeignDestination(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "work-items")
	startedAt := time.Now().UTC()
	row := githubSprintRow{
		Provider: "github", SprintID: "1", Name: stringPointer("s"),
		State: stringPointer("open"), StartedAt: &startedAt,
		LastSynced: startedAt, OrgID: claim.OrgID,
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch("sprints", EffectReadbackRequired, []json.RawMessage{encoded})
	if err != nil {
		t.Fatal(err)
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: "github", Dataset: "work-items",
		Generation: claim.GenerationKey(), Destination: "work_items",
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	if err := (GitHubWorkItemsClickHouseAdapter{Conn: unreachableConn{t: t}}).WriteGitHubWorkItemEffect(
		t.Context(), identity, effect,
	); err == nil {
		t.Fatal("work_items adapter accepted a sprints effect")
	}
}

// body_length is UInt32. A negative Go int would wrap to an enormous unsigned
// value rather than error, so it must be refused before it reaches the driver.
func TestDirectAdaptersRejectNegativeBodyLength(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	row := githubWorkItemInteractionRow{
		WorkItemID: "gh:acme/api#42", Provider: "github",
		InteractionType: "comment", OccurredAt: now, BodyLength: -1,
		LastSynced: now, OrgID: nativeTestClaim("github", "work-items").OrgID,
	}
	identity, effect := directAdapterTestEffect(t, "work_item_interactions", row)
	adapter := GitHubWorkItemInteractionsClickHouseAdapter{Conn: unreachableConn{t: t}}
	if err := adapter.WriteGitHubWorkItemEffect(t.Context(), identity, effect); err == nil {
		t.Fatal("a negative body_length reached the write path")
	}
	if _, err := adapter.InspectGitHubWorkItemEffect(t.Context(), identity, effect); err == nil {
		t.Fatal("a negative body_length reached the readback path")
	}
}

// A row carrying another tenant's org_id must be refused before any I/O, not
// filtered later by the readback -- by then it has already been written.
func TestDirectAdaptersRejectCrossTenantRowsBeforeIO(t *testing.T) {
	t.Parallel()
	now := time.Now().UTC()
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	row := githubWorkItemRow{
		WorkItemID: "gh:acme/api#42", Provider: "github", Title: "t",
		Type: "issue", Status: "doing", RepoID: &repoID,
		Assignees: []string{}, Labels: []string{},
		CreatedAt: now, UpdatedAt: now, LastSynced: now,
		OrgID: "some-other-tenant",
	}
	identity, effect := directAdapterTestEffect(t, "work_items", row)
	// A non-nil connection that fails the test if reached: with a nil Conn the
	// guard under test could be deleted and this would still pass.
	adapter := GitHubWorkItemsClickHouseAdapter{Conn: unreachableConn{t: t}}
	if err := adapter.WriteGitHubWorkItemEffect(t.Context(), identity, effect); err == nil {
		t.Fatal("a cross-tenant row reached the write path")
	}
}

func directAdapterTestEffect(t *testing.T, destination string, row any) (GitHubWorkItemEffectIdentity, EffectBatch) {
	t.Helper()
	claim := nativeTestClaim("github", "work-items")
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	effect, err := BuildEffectBatch(destination, EffectReadbackRequired, []json.RawMessage{encoded})
	if err != nil {
		t.Fatal(err)
	}
	return GitHubWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: "github", Dataset: "work-items",
		Generation: claim.GenerationKey(), Destination: destination,
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}, effect
}

// unreachableConn is a driver.Conn that fails the test if the adapter reaches
// it. A nil Conn cannot serve this purpose: workItemAdapterGuard rejects a nil
// connection whenever there are rows, so a nil-Conn test passes even when the
// guard it claims to exercise has been deleted. (Found by mutations M13/M14/M15
// surviving against exactly that shape.)
type unreachableConn struct {
	t *testing.T
}

func (c unreachableConn) reached(method string) {
	c.t.Helper()
	c.t.Fatalf("adapter reached ClickHouse via %s -- the row should have been "+
		"refused before any I/O", method)
}

func (c unreachableConn) Contributors() []string { c.reached("Contributors"); return nil }
func (c unreachableConn) ServerVersion() (*chdriver.ServerVersion, error) {
	c.reached("ServerVersion")
	return nil, nil
}
func (c unreachableConn) Select(context.Context, any, string, ...any) error {
	c.reached("Select")
	return nil
}
func (c unreachableConn) Query(context.Context, string, ...any) (chdriver.Rows, error) {
	c.reached("Query")
	return nil, nil
}
func (c unreachableConn) QueryRow(context.Context, string, ...any) chdriver.Row {
	c.reached("QueryRow")
	return nil
}
func (c unreachableConn) PrepareBatch(context.Context, string, ...chdriver.PrepareBatchOption) (chdriver.Batch, error) {
	c.reached("PrepareBatch")
	return nil, nil
}
func (c unreachableConn) Exec(context.Context, string, ...any) error { c.reached("Exec"); return nil }
func (c unreachableConn) AsyncInsert(context.Context, string, bool, ...any) error {
	c.reached("AsyncInsert")
	return nil
}
func (c unreachableConn) Ping(context.Context) error { c.reached("Ping"); return nil }
func (c unreachableConn) Stats() chdriver.Stats      { c.reached("Stats"); return chdriver.Stats{} }
func (c unreachableConn) Close() error               { return nil }

var _ chdriver.Conn = unreachableConn{}

// The previous foreign-destination test could not see the destination guard at
// all: it sent a sprints ROW, which fails decodeEffectRows' DisallowUnknownFields
// before the guard is ever consulted, so deleting the guard left it green
// (mutation M14 survived against exactly that shape).
//
// This sends a payload that decodes cleanly as a work_items row but is labelled
// for another destination. Now only the guard can stop it, and if it does not,
// the adapter reaches the connection and unreachableConn fails the test.
func TestDirectAdaptersRejectMislabelledDestinationThatStillDecodes(t *testing.T) {
	t.Parallel()
	claim := nativeTestClaim("github", "work-items")
	now := time.Now().UTC()
	repoID := uuid.MustParse("c7198fbc-1945-3717-05d8-eb78866b4e79")
	row := githubWorkItemRow{
		WorkItemID: "gh:acme/api#42", Provider: "github", Title: "t",
		Type: "issue", Status: "doing", RepoID: &repoID,
		Assignees: []string{}, Labels: []string{},
		CreatedAt: now, UpdatedAt: now, LastSynced: now, OrgID: claim.OrgID,
	}
	encoded, err := json.Marshal(row)
	if err != nil {
		t.Fatal(err)
	}
	// Valid work_items rows, but the batch is labelled for another destination.
	effect, err := BuildEffectBatch("sprints", EffectReadbackRequired, []json.RawMessage{encoded})
	if err != nil {
		t.Fatal(err)
	}
	identity := GitHubWorkItemEffectIdentity{
		OrgID: claim.OrgID, Provider: "github", Dataset: "work-items",
		Generation: claim.GenerationKey(), Destination: "work_items",
		ContentDigest: effect.ContentDigest, RowCount: len(effect.Rows),
	}
	adapter := GitHubWorkItemsClickHouseAdapter{Conn: unreachableConn{t: t}}
	if err := adapter.WriteGitHubWorkItemEffect(t.Context(), identity, effect); err == nil {
		t.Fatal("adapter accepted an effect labelled for another destination")
	}
	if _, err := adapter.InspectGitHubWorkItemEffect(t.Context(), identity, effect); err == nil {
		t.Fatal("readback accepted an effect labelled for another destination")
	}
}

// dedupeBySortingKey must keep the row ClickHouse itself retains. Measured
// against a real ReplacingMergeTree with two rows at one key and EQUAL
// versions: one INSERT block (A,B) retains B; the reversed block (B,A) retains
// A -- so it is the LAST row in insertion order, not the greatest value. The
// reversed case is the control that rules out "max wins"; without it, keeping
// the last row would be indistinguishable from keeping the larger one.
func TestDedupeBySortingKeyKeepsTheRowClickHouseRetains(t *testing.T) {
	t.Parallel()
	type row struct{ key, payload string }
	identity := func(r row) string { return r.key }

	forward := dedupeBySortingKey([]row{{"k", "A"}, {"k", "B"}}, identity)
	if len(forward) != 1 || forward[0].payload != "B" {
		t.Fatalf("(A,B) deduped to %+v, want one row carrying B", forward)
	}
	reversed := dedupeBySortingKey([]row{{"k", "B"}, {"k", "A"}}, identity)
	if len(reversed) != 1 || reversed[0].payload != "A" {
		t.Fatalf("(B,A) deduped to %+v, want one row carrying A -- keeping the "+
			"greatest value instead of the last would pass the forward case alone", reversed)
	}
	// Distinct keys must survive untouched, and in order.
	distinct := dedupeBySortingKey([]row{{"a", "1"}, {"b", "2"}, {"a", "3"}}, identity)
	if len(distinct) != 2 || distinct[0].payload != "3" || distinct[1].payload != "2" {
		t.Fatalf("mixed batch deduped to %+v, want [a=3 b=2] preserving first-seen order", distinct)
	}
}

// The sorting keys must not collide across different field splits: two rows
// whose concatenated fields are equal but whose field boundaries differ are
// different rows and must not be deduped into one.
func TestSortingKeysDoNotCollideAcrossFieldBoundaries(t *testing.T) {
	t.Parallel()
	left := githubWorkItemDependencyRow{
		OrgID: "org", SourceWorkItemID: "a", TargetWorkItemID: "bc",
		RelationshipType: "blocks",
	}
	right := githubWorkItemDependencyRow{
		OrgID: "org", SourceWorkItemID: "ab", TargetWorkItemID: "c",
		RelationshipType: "blocks",
	}
	if workItemDependencySortingKey(left) == workItemDependencySortingKey(right) {
		t.Fatal("two distinct dependency rows share a sorting key -- the separator " +
			"is not doing its job and one row would be silently dropped")
	}
}

// The LIMIT 1 BY key is a hard-coded string, so nothing but this test stops it
// drifting from the table it is supposed to mirror. A key that lost a column
// would collapse rows that are genuinely distinct; one that gained a column
// would stop collapsing rows that must collapse. Both are silent.
//
// The expectation is PARSED from the migration that owns the table
// (044_ai_attribution_repo_id_dedup_key.sql, which added repo_id to the dedup
// key) rather than restated here, so a future migration that changes the key
// fails this test instead of quietly disagreeing with the readback.
func TestAIAttributionResolveKeyMatchesTheMigrationSortingKey(t *testing.T) {
	t.Parallel()
	_, currentFile, _, _ := runtime.Caller(0)
	migration := filepath.Join(filepath.Dir(currentFile), "..", "..",
		"src", "dev_health_ops", "migrations", "clickhouse",
		"044_ai_attribution_repo_id_dedup_key.sql")
	source, err := os.ReadFile(migration)
	if err != nil {
		t.Fatalf("read the migration that owns ai_attribution's sorting key: %v", err)
	}
	var orderBy string
	for _, line := range strings.Split(string(source), "\n") {
		// The effective statement only; the file also discusses the OLD key in
		// comments, and matching one of those would assert against the very
		// key this migration replaced.
		if strings.HasPrefix(line, "ORDER BY (") {
			orderBy = strings.TrimSuffix(strings.TrimPrefix(strings.TrimSpace(line), "ORDER BY ("), ")")
			break
		}
	}
	if orderBy == "" {
		t.Fatal("no effective ORDER BY found in the migration -- the parse is " +
			"stale, so this test would otherwise assert nothing")
	}
	want := " ORDER BY computed_at DESC, record_id DESC LIMIT 1 BY " + orderBy
	if gitHubAIAttributionSelectResolve != want {
		t.Fatalf("resolve clause disagrees with the migration's sorting key\n"+
			"migration=%s\nresolve  =%s", want, gitHubAIAttributionSelectResolve)
	}
}

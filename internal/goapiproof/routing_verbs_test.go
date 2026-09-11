package goapiproof

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeCatalog(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "go_api_operations.json")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A catalog this process cannot read must REFUSE, never read as an empty
// one: "the file is broken" and "nothing is Go-eligible" are the two
// states this whole surface exists to tell apart.
func TestLoadOperationCatalogRefusesEveryUnusableShape(t *testing.T) {
	cases := map[string]string{
		"not json":                   "{",
		"an object, not an array":    `{"operation":"a","digest":"b"}`,
		"an empty array":             `[]`,
		"an entry with no digest":    `[{"operation":"a","digest":""}]`,
		"an entry with no operation": `[{"operation":"","digest":"b"}]`,
		"a duplicate operation":      `[{"operation":"a","digest":"b"},{"operation":"a","digest":"c"}]`,
		"a duplicate digest":         `[{"operation":"a","digest":"b"},{"operation":"c","digest":"b"}]`,
		// A byte the Python edge's `Path.read_text()`
		// cannot decode anywhere in the file, including in a key this
		// program never reads -- the edge rejects the WHOLE file, so this
		// must too, or Go would write a row nothing can dispatch.
		"invalid UTF-8, lone 0xFF":                             "[{\"operation\":\"a\",\"digest\":\"b\",\"ignored\":\"\xff\"}]",
		"invalid UTF-8, lone continuation byte 0x80":           "[{\"operation\":\"a\",\"digest\":\"b\",\"ignored\":\"\x80\"}]",
		"invalid UTF-8, overlong encoding":                     "[{\"operation\":\"a\",\"digest\":\"b\",\"ignored\":\"\xc0\xaf\"}]",
		"invalid UTF-8, UTF-16 surrogate":                      "[{\"operation\":\"a\",\"digest\":\"b\",\"ignored\":\"\xed\xa0\x80\"}]",
		"invalid UTF-8, above the Unicode code point limit":    "[{\"operation\":\"a\",\"digest\":\"b\",\"ignored\":\"\xf4\x90\x80\x80\"}]",
		"invalid UTF-8 in the operation KEY, not just a value": "[{\"operation\":\"\xff\",\"digest\":\"b\"}]",
		// An unpaired UTF-16 surrogate escape is valid
		// ASCII, so it passes the UTF-8 gate above -- encoding/json
		// silently collapses it to U+FFFD one layer down instead.
		"unpaired UTF-16 surrogate escape": `[{"operation":"a","digest":"b","ignored":"\ud800x"}]`,
	}
	for name, body := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := LoadOperationCatalog(writeCatalog(t, body)); !errors.Is(err, ErrCatalogUnusable) {
				t.Fatalf("LoadOperationCatalog(%s) = %v, want ErrCatalogUnusable", name, err)
			}
		})
	}
	if _, err := LoadOperationCatalog(filepath.Join(t.TempDir(), "absent.json")); !errors.Is(err, ErrCatalogUnusable) {
		t.Fatalf("a MISSING catalog must refuse too, got %v", err)
	}
}

// The checked-in catalog the Python edge actually loads must satisfy this
// reader. Two loaders that disagree about which files are acceptable
// would let a Go `enable` write rows the edge's own loader refuses to use.
func TestLoadOperationCatalogAcceptsTheCheckedInEdgeCatalog(t *testing.T) {
	catalog, err := LoadOperationCatalog(filepath.Join("..", "..", DefaultCatalogPath))
	if err != nil {
		t.Fatalf("the checked-in catalog must load: %v", err)
	}
	if len(catalog) == 0 {
		t.Fatal("the checked-in catalog is empty")
	}
	for operation, digest := range catalog {
		if len(digest) != 64 {
			t.Fatalf("%s: document digest %q is not a 64-hex sha256", operation, digest)
		}
	}
}

// Executed evidence for the
// package comment's claim: "extra keys stay ACCEPTED, deliberately"
// -- Python's subscript ignores them, so this file must too.
func TestLoadOperationCatalogAcceptsAnUnknownKey(t *testing.T) {
	catalog, err := LoadOperationCatalog(writeCatalog(t, `[{"operation":"flowMatrix","digest":"b","unexpected_future_field":"anything"}]`))
	if err != nil {
		t.Fatalf("an unrecognised key must not refuse the catalog: %v", err)
	}
	if catalog["flowMatrix"] != "b" {
		t.Fatalf("catalog[flowMatrix] = %q", catalog["flowMatrix"])
	}
}

// Executed evidence: catalogEntryKey
// reads by EXACT key out of a raw map, which is why the catalog was
// already immune to the case-shadow class that /registry, /buildinfo and
// LoadDocuments needed a fix for -- proven here rather than assumed from
// the mechanism alone.
func TestLoadOperationCatalogFieldsAreNeverShadowedByADifferentlyCasedKey(t *testing.T) {
	catalog, err := LoadOperationCatalog(writeCatalog(t, `[{"operation":"flowMatrix","digest":"good","Digest":"tampered"}]`))
	if err != nil {
		t.Fatalf("LoadOperationCatalog: %v", err)
	}
	if got := catalog["flowMatrix"]; got != "good" {
		t.Fatalf("catalog[flowMatrix] = %q, want the exact \"digest\" key's value, not the shadow", got)
	}
}

func TestResolveOperationsTreatsAllRegisteredAsTheWholeCatalog(t *testing.T) {
	catalog := map[string]string{"b": "2", "a": "1"}
	for _, raw := range []string{"all-registered", "  all-registered  "} {
		got, err := ResolveOperations(raw, catalog)
		if err != nil {
			t.Fatalf("ResolveOperations(%q) = %v", raw, err)
		}
		if want := []string{"a", "b"}; !reflect.DeepEqual(got, want) {
			t.Fatalf("ResolveOperations(%q) = %v, want %v", raw, got, want)
		}
	}
}

// An operator who typos one operation must be TOLD, not handed a
// successful-looking run that touched everything except the one they
// cared about.
func TestResolveOperationsRefusesANameTheCatalogDoesNotCarry(t *testing.T) {
	_, err := ResolveOperations("a,nope", map[string]string{"a": "1"})
	if !errors.Is(err, ErrUnknownOperation) {
		t.Fatalf("ResolveOperations = %v, want ErrUnknownOperation", err)
	}
	if !strings.Contains(err.Error(), "nope") {
		t.Fatalf("the refusal must name the unknown operation, got %q", err)
	}
}

// Shared by all four verbs: a separators-only value
// must never widen to "everything".
// The r1 finding and its second half. `","` was the reported case; `""`
// was the one that survived the first fix and was found by a codex round
// probing the binary (`-operations ”` sailed past the filter into the
// HTTP preflight, proving it had selected everything).
func TestSplitOperationsRefusesAFilterThatNamesNothing(t *testing.T) {
	for _, raw := range []string{",", " , ", ",,,", " ,, , ", "", "   ", "\t\n"} {
		got, err := SplitOperations(raw)
		if !errors.Is(err, ErrEmptyOperationFilter) {
			t.Fatalf("SplitOperations(%q) = (%v, %v), want ErrEmptyOperationFilter", raw, got, err)
		}
	}
	for _, raw := range []string{",", "", "   "} {
		got, err := ResolveOperations(raw, map[string]string{"a": "1"})
		if !errors.Is(err, ErrEmptyOperationFilter) {
			t.Fatalf("ResolveOperations(%q) = (%v, %v), want ErrEmptyOperationFilter -- a write verb must never widen silently", raw, got, err)
		}
	}
}

// The SQL is the contract. `enable` may set reachability; `disable` may
// set mode and provenance and NOTHING else -- in particular it must never
// assign current_candidate_build, which go_api_cli.py documents as "Never
// written -- disable changes mode only".
func TestDisableSQLNeverAssignsTheCandidateBuild(t *testing.T) {
	// The SET clause is checked in isolation, not the whole statement:
	// `current_candidate_build` legitimately appears in the WHERE as the
	// optional guard, and a naive substring check over the whole SQL
	// cannot tell the guard from an assignment. Splitting at WHERE is what
	// makes this assertion mean what its name says.
	setClause, whereClause, split := strings.Cut(disableRoutingRowSQL, "\n WHERE ")
	if !split {
		t.Fatal("disableRoutingRowSQL must have a WHERE clause -- an unqualified UPDATE would rewrite every row in the table")
	}
	for _, forbidden := range []string{"current_candidate_build", "owner", "rollout_percentage", "eligible_orgs"} {
		if strings.Contains(setClause, forbidden+" =") {
			t.Fatalf("disableRoutingRowSQL must never assign %q -- disable changes mode only", forbidden)
		}
	}
	for _, required := range []string{"schema_digest = $1", "document_digest = $2", "selected_operation = $3"} {
		if !strings.Contains(whereClause, required) {
			t.Fatalf("disableRoutingRowSQL must key its WHERE on %q -- the row's full primary key, never a partial match", required)
		}
	}
	for _, required := range []string{"mode = $4", "review_evidence = $5", "recorded_by = $7", "updated_at = $8"} {
		if !strings.Contains(disableRoutingRowSQL, required) {
			t.Fatalf("disableRoutingRowSQL must assign %q", required)
		}
	}
	// The guard has to be part of the WRITE, not a separate earlier read:
	// a row repointed between the plan and the write must simply not
	// match (go_api_routing_admin.apply_disable).
	if !strings.Contains(disableRoutingRowSQL, "$6::text IS NULL OR current_candidate_build = $6") {
		t.Fatal("the candidate-build guard must be part of the UPDATE's WHERE, not a separate read")
	}
}

// The lock-order convention (CHAOS-5507): a write verb registers the
// candidate build BEFORE it touches a routing row. `enable`'s SQL must
// therefore contain no row lock of its own to take first.
func TestEnableSQLTakesNoRoutingRowLockBeforeRegisteringTheBuild(t *testing.T) {
	if strings.Contains(upsertRoutingStateSQL, "FOR UPDATE") {
		t.Fatal("enable must not lock routing rows: registering the candidate build comes first, always (CHAOS-5507)")
	}
	if !strings.Contains(upsertRoutingStateSQL, "ON CONFLICT (schema_digest, document_digest, selected_operation) DO UPDATE") {
		t.Fatal("enable must upsert on the 3-column primary key so a re-run is a no-op in effect, not an error")
	}
}

func TestEnableRequestRefusesAnUnreachableMode(t *testing.T) {
	base := EnableRequest{
		SchemaDigest:   "sha256:x",
		RunningBuild:   "b",
		Operations:     []string{"a"},
		DocumentDigest: map[string]string{"a": "d"},
		RecordedBy:     "who",
		ReviewEvidence: "why",
	}
	for _, mode := range []string{"", "shadow", "python", "disabled", "PRIMARY"} {
		request := base
		request.Mode = mode
		if err := request.validate(); err == nil {
			t.Fatalf("enable must refuse mode %q -- only canary/primary make an operation reachable", mode)
		}
	}
	for _, mode := range EnableModes {
		request := base
		request.Mode = mode
		if err := request.validate(); err != nil {
			t.Fatalf("enable must accept mode %q: %v", mode, err)
		}
	}
}

// Every write verb needs a durable who and why. A rollout decision whose
// only record was a chat message is what CHAOS-5486's UNPROVEN marker
// exists to make impossible.
func TestEnableRequestRefusesAWriteWithNoDurableRecord(t *testing.T) {
	base := EnableRequest{
		SchemaDigest:   "sha256:x",
		RunningBuild:   "b",
		Operations:     []string{"a"},
		DocumentDigest: map[string]string{"a": "d"},
		Mode:           "canary",
		RecordedBy:     "who",
		ReviewEvidence: "why",
	}
	for name, mutate := range map[string]func(*EnableRequest){
		"no recorded-by":     func(r *EnableRequest) { r.RecordedBy = "" },
		"no review-evidence": func(r *EnableRequest) { r.ReviewEvidence = "" },
		"no running build":   func(r *EnableRequest) { r.RunningBuild = "" },
		"no schema digest":   func(r *EnableRequest) { r.SchemaDigest = "" },
		"no operations":      func(r *EnableRequest) { r.Operations = nil },
		"no document digest": func(r *EnableRequest) { r.DocumentDigest = nil },
		"rollout above 100":  func(r *EnableRequest) { r.RolloutPercentage = 101 },
		"negative rollout":   func(r *EnableRequest) { r.RolloutPercentage = -1 },
	} {
		request := base
		mutate(&request)
		if err := request.validate(); err == nil {
			t.Fatalf("enable must refuse: %s", name)
		}
	}
}

// The off-ramp must never be able to turn something ON -- enforced at the
// request, not only at the flag, because an invariant checked by one
// caller is an invariant the next caller breaks.
func TestDisableRequestRefusesEveryReachableMode(t *testing.T) {
	base := DisableRequest{SchemaDigest: "sha256:x", Operations: []string{"a"}}
	for _, mode := range []string{"canary", "primary", "", "PYTHON"} {
		request := base
		request.NewMode = mode
		if err := request.validate(); !errors.Is(err, ErrDisableRefusesEnablingMode) {
			t.Fatalf("disable must refuse mode %q with ErrDisableRefusesEnablingMode, got %v", mode, err)
		}
	}
	for _, mode := range DisableModes {
		request := base
		request.NewMode = mode
		if err := request.validate(); err != nil {
			t.Fatalf("disable must accept mode %q: %v", mode, err)
		}
	}
}

// A dry run needs no who/why -- it writes nothing. An --apply does.
func TestDisableRequestRequiresProvenanceOnlyToApply(t *testing.T) {
	request := DisableRequest{SchemaDigest: "sha256:x", Operations: []string{"a"}, NewMode: "python"}
	if err := request.validate(); err != nil {
		t.Fatalf("a dry-run disable needs no provenance: %v", err)
	}
	request.Apply = true
	if err := request.validate(); err == nil {
		t.Fatal("-apply must require recorded-by and review-evidence")
	}
	request.RecordedBy = "who"
	if err := request.validate(); err == nil {
		t.Fatal("-apply must require review-evidence too")
	}
	request.ReviewEvidence = "why"
	if err := request.validate(); err != nil {
		t.Fatalf("a fully-provenanced apply must validate: %v", err)
	}
}

// r2 mutation ledger (M40, M42, M44, all SURVIVED): the test above always
// mutates the SAME `request` in sequence, so by the time RecordedBy is
// checked, ReviewEvidence has already been filled -- and by the time
// ReviewEvidence is checked, RecordedBy has already been filled. Neither
// case ever isolates ONE missing field with every other field present,
// so a guard covering only that one field can be deleted without failing
// anything. This is DisableRequest's sibling of
// TestEnableRequestRefusesAWriteWithNoDurableRecord: one base request,
// one field mutated to empty at a time.
func TestDisableRequestRefusesEachMissingFieldInIsolation(t *testing.T) {
	base := DisableRequest{
		SchemaDigest:   "sha256:x",
		Operations:     []string{"a"},
		NewMode:        "python",
		RecordedBy:     "who",
		ReviewEvidence: "why",
		Apply:          true,
	}
	if err := base.validate(); err != nil {
		t.Fatalf("a fully-provenanced apply must validate: %v", err)
	}
	for name, mutate := range map[string]func(*DisableRequest){
		"no schema digest":          func(r *DisableRequest) { r.SchemaDigest = "" },
		"no operations":             func(r *DisableRequest) { r.Operations = nil },
		"no recorded-by, apply":     func(r *DisableRequest) { r.RecordedBy = "" },
		"no review-evidence, apply": func(r *DisableRequest) { r.ReviewEvidence = "" },
	} {
		request := base
		mutate(&request)
		if err := request.validate(); err == nil {
			t.Fatalf("disable must refuse: %s", name)
		}
	}
	// SchemaDigest and Operations are required even on a DRY RUN (no
	// -apply): a plan with no schema digest or no operations to plan
	// against is not a smaller plan, it is not a plan at all.
	dryRun := base
	dryRun.Apply = false
	for name, mutate := range map[string]func(*DisableRequest){
		"no schema digest, dry run": func(r *DisableRequest) { r.SchemaDigest = "" },
		"no operations, dry run":    func(r *DisableRequest) { r.Operations = nil },
	} {
		request := dryRun
		mutate(&request)
		if err := request.validate(); err == nil {
			t.Fatalf("disable must refuse even on a dry run: %s", name)
		}
	}
}

func TestDisableChangeIsNoopCoversBothWaysARowCannotMove(t *testing.T) {
	if !(DisableChange{CurrentMode: "", NewMode: "python"}).IsNoop() {
		t.Fatal("no row at the live digest is a no-op -- disable never inserts")
	}
	if !(DisableChange{CurrentMode: "python", NewMode: "python"}).IsNoop() {
		t.Fatal("a row already in the requested mode is a no-op")
	}
	if (DisableChange{CurrentMode: "canary", NewMode: "python"}).IsNoop() {
		t.Fatal("canary -> python is a real change")
	}
}

// SummarizeDisable reports every counter including the zeros, so
// "nothing needed changing" and "nothing was looked at" cannot read alike.
func TestSummarizeDisableSeparatesNoRowFromAlreadyInThatMode(t *testing.T) {
	got := SummarizeDisable([]DisableChange{
		{Operation: "a", CurrentMode: "canary", NewMode: "python", Applied: true},
		{Operation: "b", CurrentMode: "python", NewMode: "python", Applied: true},
		{Operation: "c", CurrentMode: "", NewMode: "python"},
	})
	want := DisableSummary{Total: 3, Actionable: 1, Applied: 2, NoRow: 1}
	if got != want {
		t.Fatalf("SummarizeDisable = %+v, want %+v", got, want)
	}
	empty := SummarizeDisable(nil)
	if empty != (DisableSummary{}) {
		t.Fatalf("an empty plan must summarize to all zeros, got %+v", empty)
	}
}

// Reachability is a two-part claim, and both parts must hold. Mirrors
// go_api_dispatcher's _REACHABLE_MODES and routeswitch's reachableModes.
func TestOperationStatusReachableMatchesBothPlanesRuleExactly(t *testing.T) {
	for _, mode := range []string{"canary", "primary"} {
		if !(OperationStatus{DigestState: DigestMatch, Mode: mode}).Reachable() {
			t.Fatalf("mode %q at the live digest is reachable", mode)
		}
		if (OperationStatus{DigestState: DigestStale, Mode: mode}).Reachable() {
			t.Fatalf("mode %q at a STALE digest is NOT reachable -- no request ever looks that row up", mode)
		}
	}
	for _, mode := range []string{"shadow", "python", "disabled", ""} {
		if (OperationStatus{DigestState: DigestMatch, Mode: mode}).Reachable() {
			t.Fatalf("mode %q is NOT reachable -- the client still gets Python's response", mode)
		}
	}
}

// The prefix is written durably on the ROW and must stay byte-identical
// to go_api_cli.py's, so a row written by either plane reads the same.
func TestUnprovenEvidencePrefixMatchesThePythonVerbByte(t *testing.T) {
	if UnprovenEvidencePrefix != "ACKNOWLEDGED-UNPROVEN: " {
		t.Fatalf("UnprovenEvidencePrefix = %q; go_api_cli.py's _enable_review_evidence writes %q", UnprovenEvidencePrefix, "ACKNOWLEDGED-UNPROVEN: ")
	}
}

// A catalog that decodes to JSON `null` gets its OWN sentence.
//
// `null` and `[]` both unmarshal into a nil slice, so one message covered
// both and told an operator holding a `null` file to look for an empty
// array. Executed evidence, CHAOS-5486's input-domain sweep through the
// real binary:
//
//	-catalog json-null -> "...null.json is an empty array"
//
// These refusals exist so the operator can FIX the file; one that
// misdescribes the file cannot be acted on.
func TestTheCatalogRefusalNamesWhatTheFileActuallyIs(t *testing.T) {
	for name, content := range map[string]string{
		"json null":    "null",
		"empty array":  "[]",
		"empty file":   "",
		"json object":  `{"flowMatrix":"abc"}`,
		"json scalar":  `"flowMatrix"`,
		"json number":  "42",
		"json zero":    "0",
		"not json":     "flowMatrix,abc",
		"array of nul": "[null]",
	} {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "catalog.json")
			if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
				t.Fatal(err)
			}
			_, err := LoadOperationCatalog(path)
			if err == nil {
				t.Fatalf("%s was accepted as a catalog", name)
			}
			if !errors.Is(err, ErrCatalogUnusable) {
				t.Fatalf("%s: err = %v, want ErrCatalogUnusable -- one sentinel for every unusable catalog", name, err)
			}
		})
	}

	// The specific misdescription, pinned: `null` must NOT be reported as
	// an empty array, and `[]` must still be.
	nullPath := filepath.Join(t.TempDir(), "null.json")
	if err := os.WriteFile(nullPath, []byte("null"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err := LoadOperationCatalog(nullPath)
	if err == nil || !strings.Contains(err.Error(), "JSON null") {
		t.Fatalf("a JSON-null catalog must be named as such, got: %v", err)
	}
	if err != nil && strings.Contains(err.Error(), "empty array") {
		t.Fatalf("a JSON-null catalog is still described as an empty array: %v", err)
	}

	emptyPath := filepath.Join(t.TempDir(), "empty.json")
	if err := os.WriteFile(emptyPath, []byte("[]"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, err = LoadOperationCatalog(emptyPath)
	if err == nil || !strings.Contains(err.Error(), "empty array") {
		t.Fatalf("an empty-array catalog must still be named as such, got: %v", err)
	}
}

// Every EnableRequest refusal is a REFUSAL, so `enable` prints the same
// word `disable` does for the same class of mistake.
//
// Executed evidence: `enable -mode shadow` printed the message with no
// "refused:" prefix while `disable -mode canary` printed one. Both exited
// 2 -- the machine contract was right and only the human half was wrong,
// which is the half a reader uses.
func TestEveryEnableRequestRefusalCarriesTheRefusalSentinel(t *testing.T) {
	valid := EnableRequest{
		SchemaDigest:      "sha256:live",
		RunningBuild:      "build-1",
		Operations:        []string{"flowMatrix"},
		DocumentDigest:    map[string]string{"flowMatrix": "doc-1"},
		Mode:              "canary",
		RolloutPercentage: 100,
		RecordedBy:        "lane",
		ReviewEvidence:    "why",
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("the canonical request was refused: %v", err)
	}

	broken := map[string]func(*EnableRequest){
		"no schema digest":   func(r *EnableRequest) { r.SchemaDigest = "" },
		"no running build":   func(r *EnableRequest) { r.RunningBuild = "" },
		"no recorded-by":     func(r *EnableRequest) { r.RecordedBy = "" },
		"no review-evidence": func(r *EnableRequest) { r.ReviewEvidence = "" },
		"no operations":      func(r *EnableRequest) { r.Operations = nil },
		"rollout -1":         func(r *EnableRequest) { r.RolloutPercentage = -1 },
		"rollout 101":        func(r *EnableRequest) { r.RolloutPercentage = 101 },
		"mode shadow":        func(r *EnableRequest) { r.Mode = "shadow" },
		"mode python":        func(r *EnableRequest) { r.Mode = "python" },
		"mode disabled":      func(r *EnableRequest) { r.Mode = "disabled" },
		"mode empty":         func(r *EnableRequest) { r.Mode = "" },
		"mode wrong case":    func(r *EnableRequest) { r.Mode = "CANARY" },
		"no document digest": func(r *EnableRequest) { r.DocumentDigest = map[string]string{} },
	}
	for name, break_ := range broken {
		t.Run(name, func(t *testing.T) {
			request := valid
			break_(&request)
			err := request.validate()
			if err == nil {
				t.Fatalf("%s was accepted", name)
			}
			if !errors.Is(err, ErrEnableRequestRefused) {
				t.Fatalf("%s: err = %v, want it to carry ErrEnableRequestRefused so the CLI prints \"refused:\"", name, err)
			}
		})
	}

	// The boundaries either side of the rollout range still pass, so the
	// guard is pinned as a RANGE rather than as "not 100".
	for _, rollout := range []int{0, 1, 99, 100} {
		request := valid
		request.RolloutPercentage = rollout
		if err := request.validate(); err != nil {
			t.Fatalf("rollout %d was refused: %v", rollout, err)
		}
	}
}

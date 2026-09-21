package goapiproof

import (
	"errors"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/testsupport/registryschema"
)

const (
	carryLiveDigest   = "sha256:29d509cd0000000000000000000000000000000000000000000000000000aaaa"
	carryTargetDigest = "sha256:898250a90000000000000000000000000000000000000000000000000000bbbb"
	carryDocument     = "06ca28a0517a34c0f5a6cc25b193da7b5682bea5192ae93e5a79edc7e7742208"
	carryOtherDoc     = "1111111111111111111111111111111111111111111111111111111111111111"
	carryBuild        = "b18e56fa79cfe20ce0f75df148144b832d92be36"
)

// routingModeVocabulary is the mode axis, READ FROM the schema the tests
// run against rather than typed here.
//
// The decision this verb makes is a function of the mode, and the mode's
// vocabulary is a CHECK constraint in the migration. Typing the five
// values into the enumeration would mean a sixth mode added to the
// constraint gets no cell and no decision -- exactly the shape where a
// state exists in the database that nothing in the verb has an answer
// for. Parsed from the fixture DDL, a new mode fails the enumeration
// until someone decides what a carry does with it.
func routingModeVocabulary(t *testing.T) []string {
	t.Helper()
	pattern := regexp.MustCompile(`ck_go_api_routing_state_mode\s*\n?\s*CHECK \(mode IN \(([^)]*)\)\)`)
	match := pattern.FindStringSubmatch(registryschema.DDL)
	if match == nil {
		t.Fatal("could not read the mode vocabulary out of the fixture DDL -- the enumeration below would then be a hand-typed list, which is what it exists not to be")
	}
	var modes []string
	for _, raw := range strings.Split(match[1], ",") {
		modes = append(modes, strings.Trim(strings.TrimSpace(raw), "'"))
	}
	sort.Strings(modes)
	if len(modes) < 5 {
		t.Fatalf("parsed %v as the mode vocabulary, which cannot be right", modes)
	}
	return modes
}

// The live-registry axis: what the DEPLOYED process says about this
// operation right now.
const (
	liveSame    = "live:same-document"
	liveDrifted = "live:different-document"
	liveAbsent  = "live:not-registered"
)

// The target-image axis: what the image this binary was built from
// registers.
const (
	targetSame    = "target:same-document"
	targetChanged = "target:changed-document"
	targetAbsent  = "target:not-registered"
)

// The target-digest-row axis: what is already at the digest being
// carried TO.
const (
	rowNone      = "row:none"
	rowIdentical = "row:identical"
	rowDifferent = "row:different"
)

// TestDecideCarryOverItsWholeInputDomain enumerates every combination of
// the four axes the decision reads, plus whether the operation has a
// Python execution path left, and asserts the outcome against the rule
// order this verb declares.
//
// The invariant under test, stated as the table's own shape: an
// operation REACHABLE at the live digest is either carried or the run
// refuses -- there is no combination in which a reachable operation is
// quietly skipped, which is the only way a roll could un-route something
// while this verb reported success.
func TestDecideCarryOverItsWholeInputDomain(t *testing.T) {
	modes := routingModeVocabulary(t)
	liveStates := []string{liveSame, liveDrifted, liveAbsent}
	targetStates := []string{targetSame, targetChanged, targetAbsent}
	rowStates := []string{rowNone, rowIdentical, rowDifferent}
	goOnlyStates := []bool{false, true}

	goOnlyOperation := someGoOnlyOperation(t)
	twoPlaneOperation := someTwoPlaneOperation(t)
	seen := map[string]bool{}
	cells := 0
	for _, mode := range modes {
		for _, liveState := range liveStates {
			for _, targetState := range targetStates {
				for _, rowState := range rowStates {
					for _, goOnly := range goOnlyStates {
						cells++
						operation := twoPlaneOperation
						if goOnly {
							operation = goOnlyOperation
						}
						row := CarryRow{
							Operation:         operation,
							DocumentDigest:    carryDocument,
							Mode:              mode,
							Build:             carryBuild,
							Owner:             "go",
							RolloutPercentage: 100,
							ReviewEvidence:    "the original decision",
						}
						inputs := CarryInputs{
							LiveDocumentDigest:    map[string]string{},
							TargetDocumentDigest:  map[string]string{},
							CatalogDocumentDigest: map[string]string{},
							TargetRows:            map[string]CarryRow{},
						}
						switch liveState {
						case liveSame:
							inputs.LiveDocumentDigest[operation] = carryDocument
						case liveDrifted:
							inputs.LiveDocumentDigest[operation] = carryOtherDoc
						}
						switch targetState {
						case targetSame:
							inputs.TargetDocumentDigest[operation] = carryDocument
							inputs.CatalogDocumentDigest[operation] = carryDocument
						case targetChanged:
							inputs.TargetDocumentDigest[operation] = carryOtherDoc
							inputs.CatalogDocumentDigest[operation] = carryOtherDoc
						}
						switch rowState {
						case rowIdentical:
							inputs.TargetRows[operation] = row
						case rowDifferent:
							different := row
							different.Mode = "python"
							inputs.TargetRows[operation] = different
						}

						want, wantReason := expectedCarryOutcome(mode, liveState, targetState, rowState)
						got := DecideCarry(row, inputs)
						if got.Action != want {
							t.Fatalf("mode=%s %s %s %s go_only=%t: action %s (%s), want %s",
								mode, liveState, targetState, rowState, goOnly, got.Action, got.Reason, want)
						}
						// WHICH fact decided it, not merely that
						// something did: two cells can share an action
						// and need different things done about them --
						// an operation the new image does not register
						// at all is a different problem from one whose
						// document text moved, and an operator acts on
						// the sentence, not on the enum.
						if !strings.Contains(got.Reason, wantReason) {
							t.Fatalf("mode=%s %s %s %s: reason %q does not say %q, so the report names the wrong cause",
								mode, liveState, targetState, rowState, got.Reason, wantReason)
						}
						if got.Action == CarryActionSkip || got.Action == CarryActionRefuse {
							if got.Reason == "" {
								t.Fatalf("mode=%s %s %s %s: a %s with no reason tells an operator nothing",
									mode, liveState, targetState, rowState, got.Action)
							}
						}
						if got.Action == CarryActionRefuse && got.Refusal == nil {
							t.Fatalf("mode=%s %s %s %s: a refusal with no sentinel cannot be classified by a caller",
								mode, liveState, targetState, rowState)
						}
						// Every carried value is the ROW's, never a
						// default this verb chose.
						if got.Mode != row.Mode || got.RolloutPercentage != row.RolloutPercentage ||
							got.Build != row.Build || got.Owner != row.Owner || got.DocumentDigest != row.DocumentDigest {
							t.Fatalf("outcome %+v does not echo the row it describes (%+v)", got, row)
						}
						seen[got.Action] = true
					}
				}
			}
		}
	}
	if want := len(modes) * len(liveStates) * len(targetStates) * len(rowStates) * len(goOnlyStates); cells != want {
		t.Fatalf("enumerated %d cells, want %d", cells, want)
	}
	for _, action := range []string{CarryActionCarry, CarryActionUnchanged, CarryActionSkip, CarryActionRefuse} {
		if !seen[action] {
			t.Fatalf("the enumeration never produced %s, so that branch is not covered by it", action)
		}
	}
}

// expectedCarryOutcome is the DECLARED rule order, written as the
// specification rather than as a second copy of the implementation: what
// is reachable now, then what the new image serves, then what is already
// at the target digest. Each rule names the fact that decided it, so a
// cell reached by the wrong branch fails even when the action agrees.
func expectedCarryOutcome(mode, liveState, targetState, rowState string) (action, reason string) {
	if mode != TargetModeCanary && mode != TargetModePrimary {
		return CarryActionSkip, "is not served to a client by either plane"
	}
	switch liveState {
	case liveAbsent:
		return CarryActionSkip, "the deployed process does not register this operation"
	case liveDrifted:
		return CarryActionSkip, "is not the one the deployed process registers"
	}
	switch targetState {
	case targetAbsent:
		return CarryActionRefuse, "does not register this operation at all"
	case targetChanged:
		return CarryActionRefuse, "the registered document changed"
	}
	switch rowState {
	case rowNone:
		return CarryActionCarry, ""
	case rowIdentical:
		return CarryActionUnchanged, ""
	default:
		return CarryActionRefuse, "already holds a row for this operation"
	}
}

// someTwoPlaneOperation names an operation that still HAS a Python
// execution path, asserted against the embedded ledger rather than
// assumed.
//
// Assuming cost a green test: `hotspots` reads like an ordinary
// two-plane operation and is in fact go-only, so a "this one must NOT
// escalate" case built on it was asserting the opposite of what it said,
// and the go-only axis of the enumeration was comparing go-only against
// go-only. Which operations have had their Python path deleted changes
// as the cutover proceeds, so the only safe way to name one of either
// kind is to ask the ledger.
func someTwoPlaneOperation(t *testing.T) string {
	t.Helper()
	ledger, err := DefaultGoServedLedger()
	if err != nil {
		t.Fatalf("read the embedded go-served ledger: %v", err)
	}
	for _, candidate := range []string{"flowMatrix", "securityAlerts", "savedReports", "busFactor", "experiments"} {
		if _, goOnly := ledger.Entry(candidate); !goOnly {
			return candidate
		}
	}
	t.Skip("every candidate operation is go-only, so there is no two-plane one to contrast against")
	return ""
}

func someGoOnlyOperation(t *testing.T) string {
	t.Helper()
	ledger, err := DefaultGoServedLedger()
	if err != nil {
		t.Fatalf("read the embedded go-served ledger: %v", err)
	}
	operations := ledger.Operations()
	if len(operations) == 0 {
		t.Skip("the go-served ledger is empty, so there is no go-only operation to enumerate against")
	}
	return operations[0]
}

// The catalog is the OTHER plane's half of reachability after the roll,
// and a row whose two halves disagree cannot be served end to end by the
// image being rolled to -- whichever half a carry believed.
func TestDecideCarryRefusesWhenThisImagesOwnArtifactsDisagree(t *testing.T) {
	row := CarryRow{Operation: "hotspots", DocumentDigest: carryDocument, Mode: "canary", Build: carryBuild, Owner: "go", RolloutPercentage: 100}
	for name, testCase := range map[string]struct {
		catalog    map[string]string
		wantReason string
	}{
		"catalog does not carry the operation": {map[string]string{}, "does not carry this operation"},
		"catalog names another document":       {map[string]string{"hotspots": carryOtherDoc}, "regenerate the catalog"},
	} {
		catalog, wantReason := testCase.catalog, testCase.wantReason
		t.Run(name, func(t *testing.T) {
			got := DecideCarry(row, CarryInputs{
				LiveDocumentDigest:    map[string]string{"hotspots": carryDocument},
				TargetDocumentDigest:  map[string]string{"hotspots": carryDocument},
				CatalogDocumentDigest: catalog,
			})
			if got.Action != CarryActionRefuse || !errors.Is(got.Refusal, ErrCarryImageDisagrees) {
				t.Fatalf("got %s (%v: %s), want a refusal classified as an image disagreement", got.Action, got.Refusal, got.Reason)
			}
			if !strings.Contains(got.Reason, wantReason) {
				t.Fatalf("reason %q does not say %q, so it names the wrong artifact to fix", got.Reason, wantReason)
			}
		})
	}
}

// NULL and an empty JSON container are different recorded intents, and a
// verb whose whole claim is that it preserves the row must not collapse
// one into the other -- nor report a row as already-carried when the two
// differ only there.
func TestSameCarriedStateSeparatesNullFromAnEmptyEligibleOrgs(t *testing.T) {
	empty := "{}"
	other := `{"orgs":[]}`
	base := CarryRow{Operation: "hotspots", DocumentDigest: carryDocument, Mode: "canary", Build: carryBuild, Owner: "go", RolloutPercentage: 100}
	withNull := base
	withEmpty := base
	withEmpty.EligibleOrgs = &empty
	withOther := base
	withOther.EligibleOrgs = &other

	if !sameCarriedState(withNull, withNull) || !sameCarriedState(withEmpty, withEmpty) {
		t.Fatal("a row must equal itself")
	}
	if sameCarriedState(withNull, withEmpty) {
		t.Fatal("NULL eligible_orgs and an empty JSON object are different recorded intents")
	}
	if sameCarriedState(withEmpty, withOther) {
		t.Fatal("two different JSON values must not compare equal")
	}
	// Every other copied column is part of the comparison too: a rerun
	// must not report UNCHANGED over a row that differs in any of them.
	for name, mutate := range map[string]func(*CarryRow){
		"mode":            func(r *CarryRow) { r.Mode = "primary" },
		"rollout":         func(r *CarryRow) { r.RolloutPercentage = 50 },
		"build":           func(r *CarryRow) { r.Build = "0000000000000000000000000000000000000000" },
		"owner":           func(r *CarryRow) { r.Owner = "python" },
		"document digest": func(r *CarryRow) { r.DocumentDigest = carryOtherDoc },
	} {
		t.Run(name, func(t *testing.T) {
			changed := base
			mutate(&changed)
			if sameCarriedState(base, changed) {
				t.Fatalf("a row differing in %s must not compare equal", name)
			}
		})
	}
	// review_evidence is deliberately NOT compared: a carried row's
	// evidence carries the CARRIED-FROM prefix by construction, so
	// comparing it would make every rerun look like a conflict.
	evidenced := base
	evidenced.ReviewEvidence = CarriedEvidence(carryLiveDigest, carryBuild, time.Now(), "why")
	if !sameCarriedState(base, evidenced) {
		t.Fatal("review_evidence must not take part in the comparison, or no rerun can ever report UNCHANGED")
	}
}

// The prefix is the ONLY thing that tells a reader of go_api_routing_audits
// a carried row from an ordinary enable: alembic 0130's CHECK admits
// enable|disable|repoint for `action`, so a carry is recorded as an
// enable. Pinned here because that makes the prefix a contract, not a
// nicety.
func TestCarriedEvidenceKeepsTheSourceReasonAndItsOwnPrefixes(t *testing.T) {
	at := time.Date(2026, 9, 20, 5, 27, 24, 0, time.UTC)
	source := NamedLimitEvidence("the ledger reason", "chris ruled it")
	rendered := CarriedEvidence(carryLiveDigest, carryBuild, at, source)

	if !strings.HasPrefix(rendered, CarriedEvidencePrefix) {
		t.Fatalf("evidence %q does not open with %q", rendered, CarriedEvidencePrefix)
	}
	if !strings.Contains(rendered, carryLiveDigest) || !strings.Contains(rendered, carryBuild) {
		t.Fatalf("evidence %q must name the digest carried from and the build the row claims", rendered)
	}
	if !strings.Contains(rendered, "2026-09-20T05:27:24Z") {
		t.Fatalf("evidence %q must carry the instant it was written, in UTC", rendered)
	}
	// A named-limit enablement stays a named-limit enablement at the new
	// digest. Losing that prefix would make a row that nobody proved read
	// like one that was.
	if !strings.Contains(rendered, source) {
		t.Fatalf("evidence %q dropped the source row's own reason or its prefix", rendered)
	}
}

// The request refuses on any doubt, and every refusal carries the
// classification sentinel so a caller can tell a refusal from a crash.
func TestCarryRequestRefusesEveryMissingPrecondition(t *testing.T) {
	valid := CarryRequest{
		LiveSchemaDigest:   carryLiveDigest,
		TargetSchemaDigest: carryTargetDigest,
		RunningBuild:       carryBuild,
		RecordedBy:         "lane-gwc-api-digestcarry",
		ReviewEvidence:     "CHAOS-6107 carry before the roll",
		PrincipalID:        "b0a1c2d3-0000-4000-8000-000000000001",
		Inputs: CarryInputs{
			LiveDocumentDigest:    map[string]string{"hotspots": carryDocument},
			TargetDocumentDigest:  map[string]string{"hotspots": carryDocument},
			CatalogDocumentDigest: map[string]string{"hotspots": carryDocument},
		},
	}
	if err := valid.validate(); err != nil {
		t.Fatalf("the complete request must validate: %v", err)
	}

	for name, mutate := range map[string]func(*CarryRequest){
		"no live digest":              func(r *CarryRequest) { r.LiveSchemaDigest = "" },
		"no target digest":            func(r *CarryRequest) { r.TargetSchemaDigest = "" },
		"the digests are the same":    func(r *CarryRequest) { r.TargetSchemaDigest = r.LiveSchemaDigest },
		"no running build":            func(r *CarryRequest) { r.RunningBuild = "" },
		"cross-check build disagrees": func(r *CarryRequest) { r.ExpectBuild = "0000000000000000000000000000000000000000" },
		"no recorded-by":              func(r *CarryRequest) { r.RecordedBy = "" },
		"recorded-by over the column": func(r *CarryRequest) { r.RecordedBy = strings.Repeat("x", auditRecordedByMax+1) },
		"no review-evidence":          func(r *CarryRequest) { r.ReviewEvidence = "" },
		"evidence over the column":    func(r *CarryRequest) { r.ReviewEvidence = strings.Repeat("x", auditReviewEvidenceMax+1) },
		"no principal id":             func(r *CarryRequest) { r.PrincipalID = "" },
		"the live registry is empty":  func(r *CarryRequest) { r.Inputs.LiveDocumentDigest = nil },
		"this image registers none":   func(r *CarryRequest) { r.Inputs.TargetDocumentDigest = nil },
		"the catalog is empty":        func(r *CarryRequest) { r.Inputs.CatalogDocumentDigest = nil },
	} {
		t.Run(name, func(t *testing.T) {
			request := valid
			mutate(&request)
			err := request.validate()
			if err == nil {
				t.Fatal("this request must be refused")
			}
			if !errors.Is(err, ErrCarryRequestRefused) {
				t.Fatalf("refusal %v is not classified as one, so a caller reads it as a crash", err)
			}
		})
	}
	// The equal-digest case is the verb's own reason for existing
	// inverted, so it carries its own sentinel as well.
	same := valid
	same.TargetSchemaDigest = same.LiveSchemaDigest
	if err := same.validate(); !errors.Is(err, ErrCarryDigestUnchanged) {
		t.Fatalf("err = %v, want it classified as the digests being equal", err)
	}
}

// A carry writes rows at a new key and NEVER rewrites one that is there,
// because a row at the target digest is a decision somebody else took.
func TestCarrySQLOnlyEverInsertsAndTouchesNoOtherDigest(t *testing.T) {
	if !strings.Contains(carryRoutingRowSQL, "ON CONFLICT (schema_digest, document_digest, selected_operation) DO NOTHING") {
		t.Fatalf("the carry write must be a DO NOTHING insert:\n%s", carryRoutingRowSQL)
	}
	if strings.Contains(carryRoutingRowSQL, "DO UPDATE") || strings.Contains(carryRoutingRowSQL, "UPDATE public.go_api_routing_state") {
		t.Fatalf("the carry write must never update an existing row:\n%s", carryRoutingRowSQL)
	}
	if strings.Contains(carryRoutingRowSQL, "DELETE") {
		t.Fatalf("the carry write must never delete anything:\n%s", carryRoutingRowSQL)
	}
	// Every preserved column is written from a PARAMETER, so none of
	// them can be a constant this verb chose on the operator's behalf.
	for _, column := range []string{"owner", "mode", "eligible_orgs", "rollout_percentage"} {
		if !strings.Contains(carryRoutingRowSQL, column) {
			t.Fatalf("the carry write does not carry %s:\n%s", column, carryRoutingRowSQL)
		}
	}
	for _, constant := range []string{"'go'", "'canary'", "'primary'", "100"} {
		if strings.Contains(carryRoutingRowSQL, constant) {
			t.Fatalf("the carry write hardcodes %s, which is a recorded decision it must copy instead:\n%s", constant, carryRoutingRowSQL)
		}
	}
	// The wide read is the shared one, at whichever digest it is given,
	// in the package's one total order.
	if !strings.Contains(surveyCarryRowsSQL, "ORDER BY selected_operation, document_digest") {
		t.Fatalf("the carry survey must use the shared total order:\n%s", surveyCarryRowsSQL)
	}
	if strings.Contains(surveyCarryRowsSQL, "FOR UPDATE") {
		t.Fatalf("the carry survey must take no lock -- the candidate build is registered before any routing-row lock:\n%s", surveyCarryRowsSQL)
	}
}

// An operator deciding whether to roll must be told, in the refusal
// itself, when a refused operation has no Python path left to fall back
// to: that one answers real clients an error rather than a slower page.
func TestARefusedGoOnlyOperationSaysDoNotRoll(t *testing.T) {
	goOnly := someGoOnlyOperation(t)
	refusals := []CarryOutcome{{Operation: goOnly, Reason: "the registered document changed", Refusal: ErrCarryDocumentMoved}}
	err := carryRefusalError(refusals)
	if !strings.Contains(err.Error(), "DO NOT ROLL") || !strings.Contains(err.Error(), goOnly) {
		t.Fatalf("refusal %q must name %s and say not to roll", err, goOnly)
	}
	if !errors.Is(err, ErrCarryDocumentMoved) {
		t.Fatalf("refusal %v lost its sentinel", err)
	}

	ordinary := []CarryOutcome{{Operation: someTwoPlaneOperation(t), Reason: "the registered document changed", Refusal: ErrCarryDocumentMoved}}
	if strings.Contains(carryRefusalError(ordinary).Error(), "DO NOT ROLL") {
		t.Fatal("an operation that still has a Python path must not carry the go-only escalation")
	}
}

// The report's order is a function of the data, or two runs of the same
// command cannot be diffed and no test can pin it.
func TestCarryOutcomesSortTotally(t *testing.T) {
	outcomes := []CarryOutcome{
		{Operation: "hotspots", DocumentDigest: carryOtherDoc},
		{Operation: "featureFlags", DocumentDigest: carryDocument},
		{Operation: "hotspots", DocumentDigest: carryDocument},
	}
	sortCarryOutcomes(outcomes)
	got := fmt.Sprintf("%s/%s %s/%s %s/%s",
		outcomes[0].Operation, outcomes[0].DocumentDigest[:4],
		outcomes[1].Operation, outcomes[1].DocumentDigest[:4],
		outcomes[2].Operation, outcomes[2].DocumentDigest[:4])
	want := "featureFlags/06ca hotspots/06ca hotspots/1111"
	if got != want {
		t.Fatalf("order = %q, want %q", got, want)
	}
}

// Every counter is reported even at zero: "nothing needed carrying" and
// "nothing was looked at" must not read alike.
func TestSummarizeCarryCountsEveryAction(t *testing.T) {
	summary := SummarizeCarry([]CarryOutcome{
		{Action: CarryActionCarry}, {Action: CarryActionCarry},
		{Action: CarryActionUnchanged},
		{Action: CarryActionSkip}, {Action: CarryActionSkip}, {Action: CarryActionSkip},
		{Action: CarryActionRefuse},
	})
	want := CarrySummary{Total: 7, Carried: 2, Unchanged: 1, Skipped: 3, Refused: 1}
	if summary != want {
		t.Fatalf("summary = %+v, want %+v", summary, want)
	}
	if empty := SummarizeCarry(nil); empty != (CarrySummary{}) {
		t.Fatalf("an empty run summarises as %+v, want every counter at zero", empty)
	}
}

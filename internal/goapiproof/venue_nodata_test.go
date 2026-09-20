package goapiproof

import (
	"reflect"
	"sort"
	"strings"
	"testing"
)

const nodataOp = "savedReports"

// productionReport is a completed production run in which every variant is
// vacuous except the one named `proven` (empty-allowed cases still match).
func productionReport(t *testing.T, operation, mode string, proven ...string) *VenueReceipt {
	t.Helper()
	base := goodReceipt(t, operation, mode)
	base.Venue = nil
	base.Digest = "p1"
	keep := map[string]bool{}
	for _, name := range proven {
		keep[name] = true
	}
	for i := range base.Outcomes {
		if keep[base.Outcomes[i].Variant] {
			continue
		}
		o := &base.Outcomes[i]
		o.Executed, o.TerminalState, o.RefusalReason = false, "", RefusalVacuousEmptyLegs
	}
	return base
}

func TestNoProdDataAdmitDomain(t *testing.T) {
	ok := func(prod, venue *VenueReceipt) error {
		return NoProdDataAdmit(prod, venue, nodataOp, testSchema, testDoc, testBuild, "primary")
	}
	newVenue := func() *VenueReceipt { return goodReceipt(t, nodataOp, "primary") }
	spec, _ := SpecFor(nodataOp)
	firstVariant := spec.Variants[0].Name
	if err := ok(productionReport(t, nodataOp, "primary", ""), newVenue()); err != nil {
		t.Fatalf("canonical: %v", err)
	}
	// mixed: empty-allowed cases match in production, data cases vacuous.
	if err := ok(productionReport(t, nodataOp, "primary", "", "PAGE_ZERO", "OFFSET_PAST_END"), newVenue()); err != nil {
		t.Fatalf("mixed: %v", err)
	}

	type mut struct {
		name string
		prod func(*VenueReceipt)
		ven  func(*VenueReceipt)
	}
	nop := func(*VenueReceipt) {}
	mutations := []mut{
		{"prod is a venue run", func(r *VenueReceipt) { r.Venue = &VenueStamp{Name: "bigboy-compose", Role: "admin"} }, nop},
		{"prod stage", func(r *VenueReceipt) { r.Stage = "shadow" }, nop},
		{"prod exit", func(r *VenueReceipt) { r.ExitCause = "stopped_by_signal" }, nop},
		{"prod build", func(r *VenueReceipt) { r.CandidateBuild = strings.Repeat("a", 40) }, nop},
		{"prod schema", func(r *VenueReceipt) { r.SchemaDigest = "sha256:x" }, nop},
		{"prod doc", func(r *VenueReceipt) { r.Outcomes[0].DocumentDigest = "sha256:x" }, nop},
		{"prod vacuous case at another doc", func(r *VenueReceipt) { r.Outcomes[len(r.Outcomes)-1].DocumentDigest = "sha256:x" }, nop},
		{"prod root-empty refusal", func(r *VenueReceipt) { r.Outcomes[len(r.Outcomes)-1].RefusalReason = RefusalEmptyResponseRoot }, nop},
		{"prod errored refusal", func(r *VenueReceipt) { r.Outcomes[len(r.Outcomes)-1].RefusalReason = RefusalErroredResponse }, nop},
		{"prod scope refusal", func(r *VenueReceipt) { r.Outcomes[len(r.Outcomes)-1].RefusalReason = RefusalScopeNotReflected }, nop},
		{"prod vacuous but not admitted", func(r *VenueReceipt) { r.Outcomes[len(r.Outcomes)-1].Admitted = false }, nop},
		{"prod vacuous but executed", func(r *VenueReceipt) { r.Outcomes[len(r.Outcomes)-1].Executed = true }, nop},
		{"prod mismatch", func(r *VenueReceipt) {
			one := 1
			r.Outcomes[0].RefusalReason, r.Outcomes[0].Executed, r.Outcomes[0].TerminalState, r.Outcomes[0].Outside = "", true, TerminalStateMismatch, &one
		}, nop},
		{"prod unexplained empty outcome", func(r *VenueReceipt) { r.Outcomes[len(r.Outcomes)-1].RefusalReason = "" }, nop},
		{"prod proven case carrying a refusal", func(r *VenueReceipt) { r.Outcomes[0].RefusalReason = RefusalEmptyResponseRoot }, nop},
		{"prod variant missing", func(r *VenueReceipt) { r.Outcomes = r.Outcomes[:len(r.Outcomes)-1] }, nop},
		{"prod base missing", func(r *VenueReceipt) { r.Outcomes = r.Outcomes[1:] }, nop},
		{"prod undeclared variant", func(r *VenueReceipt) {
			r.Outcomes = append(r.Outcomes, VenueOutcome{Operation: nodataOp, Variant: "NOPE"})
		}, nop},
		{"prod no outcomes", func(r *VenueReceipt) { r.Outcomes = nil }, nop},
		{"venue absent stamp", nop, func(r *VenueReceipt) { r.Venue = nil }},
		{"venue production", nop, func(r *VenueReceipt) { r.Venue.Name = "production" }},
		{"venue other build", nop, func(r *VenueReceipt) { r.CandidateBuild = strings.Repeat("b", 40) }},
		{"venue other schema", nop, func(r *VenueReceipt) { r.SchemaDigest = "sha256:x" }},
		{"venue variant unproven", nop, func(r *VenueReceipt) {
			for i := range r.Outcomes {
				if r.Outcomes[i].Variant == firstVariant {
					r.Outcomes[i].Executed = false
				}
			}
		}},
		{"venue variant refused vacuous", nop, func(r *VenueReceipt) {
			r.Outcomes[0].Executed, r.Outcomes[0].RefusalReason = false, RefusalVacuousEmptyLegs
		}},
		{"venue variant missing", nop, func(r *VenueReceipt) { r.Outcomes = r.Outcomes[1:] }},
		{"venue doc", nop, func(r *VenueReceipt) { r.Outcomes[0].DocumentDigest = "sha256:x" }},
	}
	for _, m := range mutations {
		prod, venue := productionReport(t, nodataOp, "primary", ""), newVenue()
		m.prod(prod)
		m.ven(venue)
		if err := ok(prod, venue); err == nil {
			t.Errorf("%s: admitted", m.name)
		}
	}
	// No vacuous case at all: production has data, so the ordinary proof applies.
	if err := ok(productionReport(t, nodataOp, "primary", append([]string{""}, variantNames(spec)...)...), newVenue()); err == nil {
		t.Error("a production report with no vacuous case admitted")
	}
	// Absent inputs and unlisted operations.
	if ok(nil, newVenue()) == nil || ok(productionReport(t, nodataOp, "primary", ""), nil) == nil {
		t.Error("a missing report admitted")
	}
	for _, other := range []string{"metricLineage", "featureFlags", "releaseImpact", "savedReport", "reportRuns", "zzNoSuchOp", ""} {
		if NoProdDataAdmit(productionReport(t, nodataOp, "primary", ""), newVenue(), other, testSchema, testDoc, testBuild, "primary") == nil {
			t.Errorf("%q admitted by class 2", other)
		}
	}
	// A well-formed pair for an operation OFF the list is still refused: the
	// list narrows, whatever the reports say.
	for _, other := range []string{"metricLineage", "releaseImpact", "featureFlags", "savedReport", "reportRuns"} {
		if _, err := SpecFor(other); err != nil {
			continue
		}
		if NoProdDataAdmit(productionReport(t, other, "primary", ""), goodReceipt(t, other, "primary"), other, testSchema, testDoc, testBuild, "primary") == nil {
			t.Errorf("%s admitted from well-formed reports", other)
		}
	}
	// Live side.
	prod, venue := productionReport(t, nodataOp, "primary", ""), newVenue()
	for name, err := range map[string]error{
		"live build":  NoProdDataAdmit(prod, venue, nodataOp, testSchema, testDoc, strings.Repeat("c", 40), "primary"),
		"live build ": NoProdDataAdmit(prod, venue, nodataOp, testSchema, testDoc, "", "primary"),
		"live schema": NoProdDataAdmit(prod, venue, nodataOp, "sha256:x", testDoc, testBuild, "primary"),
		"live doc":    NoProdDataAdmit(prod, venue, nodataOp, testSchema, "", testBuild, "primary"),
		"live doc 2":  NoProdDataAdmit(prod, venue, nodataOp, testSchema, "sha256:x", testBuild, "primary"),
		"mode":        NoProdDataAdmit(prod, venue, nodataOp, testSchema, testDoc, testBuild, "shadow"),
	} {
		if err == nil {
			t.Errorf("%s admitted", name)
		}
	}
}

func variantNames(spec OperationSpec) []string {
	var names []string
	for _, v := range spec.Variants {
		names = append(names, v.Name)
	}
	return names
}

func TestNoProdDataListIsNarrowAndDisjointFromClassOne(t *testing.T) {
	var listed []string
	for operation := range noProdDataOperations {
		listed = append(listed, operation)
		if _, err := SpecFor(operation); err != nil {
			t.Errorf("%s: %v", operation, err)
		}
		if VenueEligible(operation) {
			t.Errorf("%s is eligible for both classes", operation)
		}
	}
	sort.Strings(listed)
	if want := []string{"savedReports"}; !reflect.DeepEqual(listed, want) {
		t.Fatalf("list = %v, want %v", listed, want)
	}
}

func TestApplyVenueReceiptClassTwo(t *testing.T) {
	docs := map[string]string{nodataOp: testDoc, "metricLineage": testDoc, "featureFlags": testDoc}
	request := EnableRequest{
		SchemaDigest: testSchema, RunningBuild: testBuild, Mode: "primary",
		VenueReceipt: goodReceipt(t, nodataOp, "primary"), ProductionReport: productionReport(t, nodataOp, "primary", ""),
	}
	admitted, refused, still := applyVenueReceipt(request, docs, []string{nodataOp, "featureFlags"})
	if a := admitted[nodataOp]; a.venue != "d1" || a.production != "p1" || len(admitted) != 1 || refused["featureFlags"] == "" || !reflect.DeepEqual(still, []string{"featureFlags"}) {
		t.Fatalf("admitted=%v refused=%v still=%v", admitted, refused, still)
	}
	request.ProductionReport = nil
	if admitted, refused, still = applyVenueReceipt(request, docs, []string{nodataOp}); len(admitted) != 0 || refused[nodataOp] == "" || len(still) != 1 {
		t.Fatalf("no production report must not admit: %v %v %v", admitted, refused, still)
	}
	// Class 1 is unaffected by a production report.
	request.VenueReceipt = goodReceipt(t, "metricLineage", "primary")
	request.ProductionReport = productionReport(t, nodataOp, "primary", "")
	admitted, _, _ = applyVenueReceipt(request, docs, []string{"metricLineage"})
	if a := admitted["metricLineage"]; a.venue != "d1" || a.production != "" {
		t.Fatalf("class 1 admission: %+v", a)
	}
	// Evidence written for class 2 reads back as class 2.
	if got := VenueEvidenceClass(VenueEvidence("1"+strings.Repeat("a", 63), "2"+strings.Repeat("b", 63), "note")); got != VenueClassNoData {
		t.Fatalf("class = %q", got)
	}
}

// A listed operation must be able to finish a production run with no
// data: a variant that needs a real instance id is refused by the prover as
// operation_needs_an_instance_identifier (production has no id to give), so
// a class-2 list entry carrying one could never be admitted. Generated over
// every operation the prover knows: listed <=> reachable (no Instance
// variant), and the two excluded report operations do carry one.
func TestNoProdDataListedOperationsNeedNoInstanceID(t *testing.T) {
	needsInstance := func(operation string) bool {
		spec, err := SpecFor(operation)
		if err != nil {
			t.Fatalf("%s: %v", operation, err)
		}
		for _, variant := range spec.Variants {
			if variant.Instance != nil {
				return true
			}
		}
		return false
	}
	for _, operation := range KnownOperations() {
		if (NoProdDataEligible(operation) || VenueEligible(operation)) && needsInstance(operation) {
			t.Errorf("%s is admitted by a venue class but a variant needs an instance id the proof run cannot supply", operation)
		}
	}
	for _, excluded := range []string{"savedReport", "reportRuns"} {
		if !needsInstance(excluded) {
			t.Errorf("%s is off the list because it needs an instance id, but no variant declares one", excluded)
		}
		if NoProdDataEligible(excluded) {
			t.Errorf("%s is listed", excluded)
		}
	}
}

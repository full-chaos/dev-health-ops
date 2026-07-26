package domaingrants

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// Tests for the three assertions codex found could not fail.
//
// Each had the same underlying shape: the test proved a value reached some data
// structure, while the code that ACTS on that value was never exercised. Proving
// a fact is available is not proving anything is done with it.

// TestGateFailsOnEveryEnforcedCategory drives GateFailures directly, so deleting
// any enforced category is caught. Previously each category was reported by its
// own loop inside the gate test, and a unit test could only show the value
// arrived in IncompleteRoleSurface -- deleting the loop that reported it left
// every test green.
func TestGateFailsOnEveryEnforcedCategory(t *testing.T) {
	var selectOnly PrivilegeSet
	selectOnly.add(PrivSelect)

	report := &RoleReport{
		Incomplete: []IncompleteRoleSurface{{
			Role: RoleCoordinator,
			UndeclaredByEvidence: []PostureGapWithoutEvidence{
				{Table: "nobody_touches_it", Privilege: PrivUpdate},
			},
			DynamicSQL: []DynamicSite{
				{File: "internal/brand/new.go", Line: 3, Reason: "not a constant"},
			},
			UnparsedLocks: []UnparsedLockSite{
				{File: "internal/x/y.go", Line: 9, Statement: "LOCK a, b"},
			},
			FuncValueConflicts: []FuncValueConflictSite{
				{File: "cmd/x/deps.go", Line: 7, Field: "sources.newThing", Reason: "two implementations"},
			},
		}},
	}

	failures := strings.Join(GateFailures(report), "\n")
	for _, want := range []string{
		"UNACKNOWLEDGED BLIND SPOT",
		"UNACKNOWLEDGED DYNAMIC SQL",
		"UNPARSED LOCK",
		"UNRESOLVED FUNCTION-VALUE CALL",
	} {
		if !strings.Contains(failures, want) {
			t.Errorf("GateFailures did not report %s -- that category is collected but no longer "+
				"enforced, which is the exact state where the gate looks green over unanalyzed "+
				"surface:\n%s", want, failures)
		}
	}

	// And a clean report must produce NO failures, or the gate is unconditional and
	// its green means nothing either. The acknowledgement lists are emptied for
	// this half: with the REAL lists, an empty report correctly reports every
	// acknowledgement as stale, which is the self-cleaning property working rather
	// than a failure.
	realBlind := acknowledgedBlindSpots
	realDynamic := acknowledgedDynamicSQL
	t.Cleanup(func() {
		acknowledgedBlindSpots = realBlind
		acknowledgedDynamicSQL = realDynamic
	})
	acknowledgedBlindSpots = nil
	acknowledgedDynamicSQL = nil
	if extra := GateFailures(&RoleReport{}); len(extra) != 0 {
		t.Errorf("an empty report with no acknowledgements must produce no gate failures, got %v", extra)
	}
}

// TestAcknowledgementsRequireAReason makes the mandatory-reason check
// falsifiable. It could not fail before, because every real acknowledgement
// already carries a reason -- so the check had no input that would exercise it.
func TestAcknowledgementsRequireAReason(t *testing.T) {
	realBlind := acknowledgedBlindSpots
	realDynamic := acknowledgedDynamicSQL
	t.Cleanup(func() {
		acknowledgedBlindSpots = realBlind
		acknowledgedDynamicSQL = realDynamic
	})

	acknowledgedBlindSpots = []AcknowledgedBlindSpot{
		{Role: RoleDomain, Table: "t", Privilege: PrivSelect, Why: ""},
	}
	acknowledgedDynamicSQL = []AcknowledgedDynamicSQL{
		{File: "internal/x/y.go", Why: "   "},
	}

	surface := IncompleteRoleSurface{
		Role:                 RoleDomain,
		UndeclaredByEvidence: []PostureGapWithoutEvidence{{Table: "t", Privilege: PrivSelect}},
		DynamicSQL:           []DynamicSite{{File: "internal/x/y.go", Line: 1, Reason: "dynamic"}},
	}

	if _, _, unreasoned := PartitionBlindSpots([]IncompleteRoleSurface{surface}); len(unreasoned) != 1 {
		t.Errorf("a blind-spot acknowledgement with no reason must be reported, got %d", len(unreasoned))
	}
	if _, _, unreasoned := PartitionDynamicSQL([]IncompleteRoleSurface{surface}); len(unreasoned) != 1 {
		t.Errorf("a dynamic-SQL acknowledgement with a whitespace-only reason must be reported, got %d",
			len(unreasoned))
	}

	// And it must reach the gate, not just the partition function.
	failures := strings.Join(GateFailures(&RoleReport{Incomplete: []IncompleteRoleSurface{surface}}), "\n")
	if !strings.Contains(failures, "has no reason recorded") ||
		!strings.Contains(failures, "has no reasoning recorded") {
		t.Errorf("reasonless acknowledgements must fail the gate:\n%s", failures)
	}
}

// TestBuildPoolParamRolesMarksPartialEvidenceIncomplete exercises
// buildPoolParamRoles through a REAL package load, on the exact shape codex
// found: a parameter spelled for one role, reached by one recognised call from
// the other role AND one call the pass cannot classify.
//
// The existing H1 tests injected poolParamRoles directly and never ran the
// collector, which is precisely why this defect was invisible to them -- they
// tested the consumer against a hand-built state the producer would not have
// produced. The fixture below is a self-contained module (its own local package
// named pgxpool, so isPgxPoolPtr matches) and needs no network.
func TestBuildPoolParamRolesMarksPartialEvidenceIncomplete(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, content string) {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}

	write("go.mod", "module fixture\n\ngo 1.25\n")
	write("pgxpool/pgxpool.go", "package pgxpool\n\ntype Pool struct{}\n")
	// `build` is spelled for the DOMAIN role. One call site passes a recognised
	// coordinator root; the other passes an expression this pass cannot classify
	// (a struct field with a non-seed name). The observed set is therefore a LOWER
	// BOUND -- and must not suppress the domain seed.
	write("main.go", `package main

import "fixture/pgxpool"

type pools struct {
	Coordinator *pgxpool.Pool
	Other       *pgxpool.Pool
}

func build(domainPool *pgxpool.Pool) {}

func wireOne(p *pools)   { build(p.Coordinator) }
func wireTwo(p *pools)   { build(p.Other) }

func main() { wireOne(nil); wireTwo(nil) }
`)

	derived, err := DeriveForRole(dir, RoleDomain)
	if err != nil {
		t.Fatalf("DeriveForRole: %v", err)
	}
	// The observable consequence: no override is emitted, because the evidence is
	// incomplete. If partial evidence were allowed to outrank spelling, the domain
	// seed would be suppressed here and any SQL beyond `build` would vanish from
	// the domain surface -- a false ABSENCE.
	if len(derived.NameSeedOverrides) != 0 {
		t.Errorf("partial call-site evidence must NOT override the parameter name; got overrides %+v. "+
			"One call site passes an unclassifiable argument, so the observed role set is a lower "+
			"bound, and suppressing the domain seed on it makes real SQL disappear",
			derived.NameSeedOverrides)
	}
}

// TestBuildPoolParamRolesOverridesOnCompleteContradictingEvidence is the control:
// with EVERY call site classified and none of them this role, the override must
// fire. Without this half, the test above would pass for a build that simply
// never overrides anything.
func TestBuildPoolParamRolesOverridesOnCompleteContradictingEvidence(t *testing.T) {
	dir := t.TempDir()
	write := func(rel, content string) {
		full := filepath.Join(dir, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module fixture\n\ngo 1.25\n")
	write("pgxpool/pgxpool.go", "package pgxpool\n\ntype Pool struct{}\n")
	// Both call sites pass the recognised coordinator root, and `build` executes
	// SQL through its domain-spelled parameter.
	write("main.go", `package main

import (
	"context"

	"fixture/pgxpool"
)

type pools struct {
	Coordinator *pgxpool.Pool
}

func (p *pgxpool.Pool) placeholder() {}

func build(ctx context.Context, domainPool *pgxpool.Pool) {}

func wireOne(ctx context.Context, p *pools) { build(ctx, p.Coordinator) }
func wireTwo(ctx context.Context, p *pools) { build(ctx, p.Coordinator) }

func main() { wireOne(nil, nil); wireTwo(nil, nil) }
`)

	// The method-on-imported-type line above is illegal Go; drop it.
	write("main.go", `package main

import (
	"context"

	"fixture/pgxpool"
)

type pools struct {
	Coordinator *pgxpool.Pool
}

func build(ctx context.Context, domainPool *pgxpool.Pool) {
	_ = domainPool
}

func wireOne(ctx context.Context, p *pools) { build(ctx, p.Coordinator) }
func wireTwo(ctx context.Context, p *pools) { build(ctx, p.Coordinator) }

func main() { wireOne(nil, nil); wireTwo(nil, nil) }
`)

	derived, err := DeriveForRole(dir, RoleDomain)
	if err != nil {
		t.Fatalf("DeriveForRole: %v", err)
	}
	if len(derived.NameSeedOverrides) == 0 {
		t.Error("with every call site classified and none of them domain, the domain-spelled parameter " +
			"must be overridden -- otherwise the collector never overrides anything and the companion " +
			"test proves nothing")
	}
}

package daily

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"sort"
)

//go:embed families.json
var rawFamilies []byte

// Registry is the parsed metrics.daily family inventory.
//
// # Why this file exists (CHAOS-4283 PR2)
//
// families.json has always been the drift-tested contract for this package,
// but until now nothing in the daily package READ it at runtime -- it was
// documentation plus a fixture for tests and the generated matrix, while the
// actual registration lived as hand-written map assignments in
// cmd/dev-health-worker/daily.go. That was fine while the only per-family
// facts were "is it native" and "which phase", both of which the registration
// site expresses directly.
//
// Family ORDERING cannot be expressed that way. SetNativeFamilies takes a MAP,
// and Go map iteration is randomised, so the pre-CHAOS-4283 implementation
// sorted names alphabetically to get determinism. Alphabetical order happens to
// put `work_item` BEFORE `work_item_attribution` -- so once the work-item
// families move to pre_bridge, the three READERS of work_item_team_attributions
// would run before the family that WRITES it, silently reintroducing the exact
// stale-read defect codex round 1 caught as a P1 on CHAOS-4278. Nothing in the
// suite would have failed: the ordering was incidental, so no assertion
// depended on it.
//
// Encoding the dependency in families.json (as `"after"`) and deriving the run
// order from it makes the constraint declarative, drift-tested alongside the
// rest of the contract, and visible to the same guards that already read this
// file -- rather than a comment asking the next editor to keep two hand-written
// lists in a particular order.
type Registry struct {
	SchemaVersion int      `json:"schema_version"`
	Owner         string   `json:"owner"`
	Source        string   `json:"source"`
	Families      []Family `json:"families"`
}

// Family is one metrics.daily family's contract row.
type Family struct {
	Name   string   `json:"name"`
	Python string   `json:"python"`
	Writes []string `json:"writes"`
	// Port is "go" when a native executor computes this family, "pending"
	// when it still goes to the Python compatibility bridge.
	Port   string `json:"port"`
	Golden string `json:"golden"`
	// Phase is "" (pre_bridge, the default) or "post_bridge".
	Phase     string `json:"phase"`
	PhaseNote string `json:"phase_note"`
	// After names families that MUST compute before this one within the same
	// phase. It is a hard ordering constraint, not a hint: a family listed
	// here writes a table this family reads in the same partition.
	After []string `json:"after"`
}

// LoadRegistry parses the embedded families.json.
func LoadRegistry() (Registry, error) {
	var registry Registry
	if err := json.Unmarshal(rawFamilies, &registry); err != nil {
		return Registry{}, fmt.Errorf("decode families.json: %w", err)
	}
	if len(registry.Families) == 0 {
		return Registry{}, fmt.Errorf("families.json declares no families")
	}
	return registry, nil
}

// ErrFamilyOrderCycle reports an `after` graph that cannot be linearised.
//
// This is a CONSTRUCTION failure, never a fallback to alphabetical order. A
// cycle means two families each claim to need the other's output first, which
// is not a preference the dispatcher may quietly resolve -- whichever way it
// broke the tie, one of the two would read a stale table, which is precisely
// the defect this ordering exists to prevent. Failing loudly at startup is the
// only honest response.
var ErrFamilyOrderCycle = fmt.Errorf("families.json: `after` graph contains a cycle")

// ErrFamilyOrderUnknown reports an `after` entry naming a family that does not
// exist.
//
// Also fatal, and for a subtler reason: an unknown name would otherwise impose
// NO constraint at all, so a typo would silently degrade to "unordered" while
// still looking, in the JSON, exactly like a declared dependency. That is the
// same fail-open shape as a gate that reports a value it never checks.
var ErrFamilyOrderUnknown = fmt.Errorf("families.json: `after` names an unknown family")

// FamilyRunOrder linearises `names` so that every declared `after` dependency
// computes first, breaking ties alphabetically so the result is deterministic
// and reproducible run to run.
//
// `names` is the set actually REGISTERED (which may be a subset of
// families.json -- a family whose executor failed to construct is absent).
// Dependencies pointing outside that set are satisfied vacuously: the ordering
// constraint exists to sequence two families that both run, and a family that
// is not running cannot be waited for. It is still validated against the full
// registry, so a typo is caught even when the named family is not registered.
//
// CHAOS-5078 codex r1 F3 fix: a CYCLE in the DECLARED graph (families.json's
// `after` edges among ALL families, regardless of which subset is registered
// this run) is checked FIRST, unconditionally -- this is a JSON-authoring
// defect and must be caught the same way every run, not only on the runs
// where every cycle member happens to be registered. Before this fix, a
// cycle whose members were not ALL registered was satisfied vacuously (each
// edge touching an unregistered endpoint was silently dropped before the
// registered-subset toposort ever saw it), so it produced NO error at all --
// indistinguishable from a families.json with no cycle. Since which
// families are registered can vary run to run (an executor construction
// failure removes one from `names`), a families.json cycle could sit
// undetected until the one run where every member happened to construct
// successfully, at which point it would BLOCK ordering only then.
func declaredGraphHasCycle(registry Registry) ([]string, bool) {
	full := make(map[string][]string, len(registry.Families))
	for _, family := range registry.Families {
		full[family.Name] = append([]string(nil), family.After...)
	}
	remaining := make([]string, 0, len(registry.Families))
	for _, family := range registry.Families {
		remaining = append(remaining, family.Name)
	}
	sort.Strings(remaining)
	done := make(map[string]struct{}, len(remaining))
	for len(remaining) > 0 {
		progressed := false
		for index, name := range remaining {
			ready := true
			for _, dependency := range full[name] {
				if _, satisfied := done[dependency]; !satisfied {
					ready = false
					break
				}
			}
			if !ready {
				continue
			}
			done[name] = struct{}{}
			remaining = append(remaining[:index], remaining[index+1:]...)
			progressed = true
			break
		}
		if !progressed {
			return remaining, true
		}
	}
	return nil, false
}

// registeredDependencies returns, for each name in `names`, its DIRECT
// `after` dependencies restricted to the registered subset (mirrors
// FamilyRunOrder's own vacuous-satisfaction rule: a dependency pointing
// outside `names` is not returned, since a family that is not running this
// pass cannot be waited for or blocked on).
//
// CHAOS-5078 codex r2 F3: exposed as its own function (previously inlined in
// FamilyRunOrder only) so computeNativeFamilies can gate a family's RUNTIME
// execution on whether its dependency already failed or was itself blocked
// THIS pass -- FamilyRunOrder only proves a safe static ORDER, it says
// nothing about what happens when a family earlier in that order fails at
// runtime.
func registeredDependencies(registry Registry, names []string) map[string][]string {
	registered := make(map[string]struct{}, len(names))
	for _, name := range names {
		registered[name] = struct{}{}
	}
	dependencies := make(map[string][]string, len(names))
	for _, family := range registry.Families {
		for _, after := range family.After {
			if _, ok := registered[family.Name]; !ok {
				continue
			}
			if _, ok := registered[after]; !ok {
				continue
			}
			dependencies[family.Name] = append(dependencies[family.Name], after)
		}
	}
	return dependencies
}

func FamilyRunOrder(registry Registry, names []string) ([]string, error) {
	known := make(map[string]struct{}, len(registry.Families))
	for _, family := range registry.Families {
		known[family.Name] = struct{}{}
	}
	// Unknown-name check runs FIRST and unconditionally, before the
	// declared-graph cycle check below -- an `after` entry naming a family
	// absent from the WHOLE registry would otherwise also look unresolvable
	// to declaredGraphHasCycle's toposort (its dependency's `done` flag can
	// never be set, since that name is not among the registry's own
	// families either), which would misreport it as ErrFamilyOrderCycle
	// instead of the more specific, more actionable ErrFamilyOrderUnknown.
	for _, family := range registry.Families {
		for _, after := range family.After {
			if _, ok := known[after]; !ok {
				return nil, fmt.Errorf("%w: %q lists %q", ErrFamilyOrderUnknown, family.Name, after)
			}
		}
	}
	if unresolved, cyclic := declaredGraphHasCycle(registry); cyclic {
		return nil, fmt.Errorf("%w: unresolvable among %v (declared graph, independent of "+
			"which families are registered this run)", ErrFamilyOrderCycle, unresolved)
	}
	dependencies := registeredDependencies(registry, names)

	remaining := make([]string, len(names))
	copy(remaining, names)
	sort.Strings(remaining)

	done := make(map[string]struct{}, len(remaining))
	ordered := make([]string, 0, len(remaining))
	for len(remaining) > 0 {
		progressed := false
		for index, name := range remaining {
			ready := true
			for _, dependency := range dependencies[name] {
				if _, satisfied := done[dependency]; !satisfied {
					ready = false
					break
				}
			}
			if !ready {
				continue
			}
			ordered = append(ordered, name)
			done[name] = struct{}{}
			remaining = append(remaining[:index], remaining[index+1:]...)
			progressed = true
			break
		}
		if !progressed {
			return nil, fmt.Errorf("%w: unresolvable among %v", ErrFamilyOrderCycle, remaining)
		}
	}
	return ordered, nil
}

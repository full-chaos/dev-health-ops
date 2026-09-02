package units

import "time"

// MembershipWeightThreshold is declared in constants.go, alongside the other
// ported constants, so the two ported jobs cannot drift on it. Note 0.2 has no
// exact binary representation (bits 3fc999999999999a); the corpus pins the bits
// so a future edit to a "nicer" spelling fails rather than shifting a boundary.

// Distribution is a category->weight mapping that preserves INSERTION ORDER.
//
// # WHY THIS IS NOT A map[string]float64
//
// membership_categories iterates `distribution.items()`. Python dicts have
// preserved insertion order since 3.7 and it is a language guarantee, not an
// implementation detail. Go randomises map iteration deliberately, so a port
// backed by a plain map emits the same rows in a different sequence on every
// single run -- not just differently from Python, but differently from itself.
//
// Sorting instead would be deterministic and still wrong: it would produce an
// order Python never produces. Only insertion order reproduces the reference.
//
// The weights are `any` rather than float64 because _float_value accepts
// arbitrary objects and coerces them, and its isinstance ladder is itself
// load-bearing -- see FloatValue.
type Distribution struct {
	categories []string
	weights    map[string]any
}

// NewDistribution builds a Distribution from ordered pairs.
//
// Re-assigning a key that is already present KEEPS ITS ORIGINAL POSITION and
// overwrites the value, matching Python's `d[k] = v` on an existing key. A
// reader might reasonably expect re-assignment to move the key to the end; it
// does not, in either plane, and the corpus would not catch the difference
// unless a case re-assigns.
func NewDistribution(pairs ...CategoryWeight) *Distribution {
	distribution := &Distribution{weights: make(map[string]any, len(pairs))}
	for _, pair := range pairs {
		if _, seen := distribution.weights[pair.Category]; !seen {
			distribution.categories = append(distribution.categories, pair.Category)
		}
		distribution.weights[pair.Category] = pair.Weight
	}
	return distribution
}

// CategoryWeight is one (category, raw weight) pair.
type CategoryWeight struct {
	Category string
	Weight   any
}

// Len reports the number of distinct categories.
func (d *Distribution) Len() int {
	if d == nil {
		return 0
	}
	return len(d.categories)
}

// Categories returns the categories in insertion order.
func (d *Distribution) Categories() []string {
	if d == nil {
		return nil
	}
	return d.categories
}

// Weight returns the raw weight for a category.
func (d *Distribution) Weight(category string) any {
	if d == nil {
		return nil
	}
	return d.weights[category]
}

// FloatValue ports membership._float_value.
//
// # THE ORDER OF THE TYPE CHECKS IS LOAD-BEARING
//
// Python:
//
//	if isinstance(value, bool):        return 0.0
//	if isinstance(value, int | float): return float(value)
//	if isinstance(value, str):         try float(value) except ValueError: 0.0
//	return 0.0
//
// bool is checked FIRST because `isinstance(True, int)` is True in Python --
// bool is a subclass of int. Without that branch, True would coerce to 1.0
// rather than 0.0. Go has no such subtyping, so the ordering looks arbitrary
// here and would be easy to "tidy" into a single case; it is not arbitrary, it
// is the whole reason the branch exists.
//
// The string branch uses ParsePythonFloat, not strconv.ParseFloat. See
// floatcoerce.go: ParseFloat accepts hex floats Python rejects and reports
// ErrRange where Python simply returns inf, so it is wrong in both directions.
//
// Everything else -- None, list, dict, bytes -- is 0.0. Note bytes: Python's
// float(b"1.5") raises TypeError, not ValueError, so it would NOT be caught by
// the except clause; but bytes is not a str, so it never reaches the try at all
// and falls through to the final return. The distinction does not change the
// answer here, and is noted so a future reader does not "fix" the ordering on
// the theory that bytes should parse.
func FloatValue(value any) float64 {
	switch typed := value.(type) {
	case bool:
		// isinstance(value, bool) -> 0.0, before the int branch. `typed` is
		// deliberately unused: the VALUE of the bool is irrelevant, both True
		// and False coerce to 0.0.
		_ = typed
		return 0.0
	case int:
		return float64(typed)
	case int8:
		return float64(typed)
	case int16:
		return float64(typed)
	case int32:
		return float64(typed)
	case int64:
		return float64(typed)
	case uint:
		return float64(typed)
	case uint8:
		return float64(typed)
	case uint16:
		return float64(typed)
	case uint32:
		return float64(typed)
	case uint64:
		return float64(typed)
	case float32:
		return float64(typed)
	case float64:
		return typed
	case string:
		parsed, ok := ParsePythonFloat(typed)
		if !ok {
			return 0.0
		}
		return parsed
	default:
		return 0.0
	}
}

// LexicalArgmax ports membership.lexical_argmax.
//
// Python:
//
//	return min(distribution, key=lambda k: (-_float_value(distribution[k]), k))
//
// # THE DOCSTRING'S GUARANTEE IS FALSE, AND THIS REPRODUCES THE TRUE BEHAVIOUR
//
// The reference claims the lexical tie-break "makes the dominant choice
// deterministic across runs regardless of dict ordering". That holds only while
// every weight is a real number.
//
// `min` replaces its running candidate only on STRICTLY LESS THAN. Tuple
// comparison reaches the second element only when the first elements compare
// equal, and every comparison involving NaN is False -- including equality. So
// with a NaN weight the lexical key is never consulted and the FIRST entry in
// iteration order survives. Measured:
//
//	{"a.one": nan, "b.two": nan}  -> "a.one"
//	{"b.two": nan, "a.one": nan}  -> "b.two"     (not lexical)
//	{"a.nan": nan, "b.real": 0.9} -> "a.nan"     (NaN beats 0.9)
//
// The third line is the damaging one: a NaN outranks a real weight purely by
// being inserted first, and is_dominant is persisted. Filed as CHAOS-4840. The
// port reproduces it because bit-exact parity including reproduced bugs is the
// contract -- diverging unilaterally would put the two planes in disagreement.
// When CHAOS-4840 is fixed in Python, the corpus here fails deliberately; that
// failure is the guard working, and the fixture is regenerated as part of the
// fix.
//
// This is written as an explicit fold rather than a sort, because a sort needs a
// total order and the comparison here is NOT one: with NaN present it is not
// even reflexive. Handing a non-total comparator to sort.Slice is undefined
// behaviour in Go and would not reproduce `min` in any case.
func LexicalArgmax(distribution *Distribution) string {
	if distribution.Len() == 0 {
		// Python: `if not distribution: return "unknown"`.
		return "unknown"
	}

	best := distribution.categories[0]
	bestWeight := -FloatValue(distribution.weights[best])

	for _, category := range distribution.categories[1:] {
		weight := -FloatValue(distribution.weights[category])
		// Python's min keeps the incumbent unless the candidate is strictly
		// less. Reproduced literally: `<` is false for every NaN comparison, so
		// a NaN candidate never displaces, and a NaN incumbent is never
		// displaced.
		if weight < bestWeight {
			best, bestWeight = category, weight
			continue
		}
		// The lexical tie-break, reached only when the weights compare EQUAL.
		// NaN never compares equal, so this line is unreachable whenever either
		// side is NaN -- which is precisely the defect above.
		if weight == bestWeight && category < best {
			best = category
		}
	}
	return best
}

// MembershipRow is one (category, weight, is_dominant) triple.
type MembershipRow struct {
	Category   string
	Weight     float64
	IsDominant int
}

// MembershipCategories ports membership.membership_categories.
//
// Every category at or above the threshold is emitted, plus the argmax category
// ALWAYS, even below threshold, flagged is_dominant=1. So a mixed unit is
// findable under each significant category and every unit is findable under at
// least its dominant one.
//
// The comparison is `>=`, not `>`. Exactly-threshold is INCLUDED. A corpus can
// only see that on a NON-dominant category, since a dominant one is emitted via
// the `or is_dominant` arm whatever its weight -- the first version of this
// lane's corpus tested the boundary only on singletons and therefore tested
// nothing.
//
// Note `nan >= threshold` is False, so a NaN-weighted category is emitted only
// when it is dominant. Combined with the LexicalArgmax defect above, that means
// the bug is what puts such a row in the table at all.
func MembershipCategories(distribution *Distribution) []MembershipRow {
	if distribution.Len() == 0 {
		// Python returns [] for an empty distribution, before computing an
		// argmax -- so the "unknown" sentinel never reaches a row.
		return nil
	}

	dominant := LexicalArgmax(distribution)
	rows := make([]MembershipRow, 0, distribution.Len())
	seen := make(map[string]struct{}, distribution.Len())

	for _, category := range distribution.categories {
		weight := FloatValue(distribution.weights[category])
		isDominant := 0
		if category == dominant {
			isDominant = 1
		}
		if weight >= MembershipWeightThreshold || isDominant == 1 {
			rows = append(rows, MembershipRow{
				Category:   category,
				Weight:     weight,
				IsDominant: isDominant,
			})
			seen[category] = struct{}{}
		}
	}

	// Python: "Defensive: ensure the dominant row is present even if it was
	// filtered."
	//
	// UNREACHABLE. LexicalArgmax returns a key OF the distribution (the empty
	// case returned above), the loop visits every key, and the dominant key
	// always takes the `|| isDominant == 1` arm -- so it is always in `seen`.
	// Probed across NaN, negative and -inf weights: the branch never fired.
	//
	// Reproduced rather than deleted, because removing it would leave a future
	// reader believing the guarantee was never intended, and because a change to
	// LexicalArgmax that CAN return a non-member -- CHAOS-4840's fix might --
	// would silently start needing it.
	if _, present := seen[dominant]; !present {
		rows = append(rows, MembershipRow{
			Category:   dominant,
			Weight:     FloatValue(distribution.weights[dominant]),
			IsDominant: 1,
		})
	}
	return rows
}

// MembershipRecord ports metrics.schemas.WorkUnitMembershipRecord: one row per
// (node, category) in work_unit_membership.
//
// RunID carries no default here. Python declares `run_id: str = ""`, but
// build_membership_records takes it as a required keyword and the reference's
// own docstring says it "must be stamped on every row" -- the dataclass default
// exists for other construction sites, not this one. A Go zero value would
// silently reproduce the default rather than the requirement, so the builder
// takes it explicitly.
type MembershipRecord struct {
	OrgID                string
	NodeType             string
	NodeID               string
	WorkUnitID           string
	CategoryKind         string
	Category             string
	Weight               float64
	IsDominant           int
	CategorizationStatus string
	ComputedAt           time.Time
	RunID                string
}

// MembershipInput carries the non-distribution arguments of
// build_membership_records. Grouped into a struct because Python takes them as
// keyword-only arguments (the `*` in the signature), so no caller can supply
// them positionally and no caller can transpose two same-typed fields by
// accident. Five of these are strings; positional Go parameters would give up
// that protection silently.
type MembershipInput struct {
	UnitNodes            []NodeKey
	WorkUnitID           string
	CategorizationStatus string
	ComputedAt           time.Time
	OrgID                string
	RunID                string
}

// BuildMembershipRecords ports membership.build_membership_records.
//
// # THE ORDER IS THE OUTPUT
//
// Python emits, for each node in unit_nodes: every THEME row, then every
// SUBCATEGORY row. Within each kind the rows are in the distribution's insertion
// order, via MembershipCategories. So the sequence is
//
//	node[0] themes..., node[0] subcategories...,
//	node[1] themes..., node[1] subcategories..., ...
//
// and NOT all themes for all nodes followed by all subcategories. Swapping the
// loop nesting produces the same multiset of rows in a different sequence, which
// a set-comparison test cannot see.
//
// Both category lists are computed ONCE, before the node loop, exactly as the
// reference does. Recomputing them per node would be a pure waste in Go and
// would also give NaN-driven argmax instability a second chance to differ
// between iterations if the distribution were ever mutated mid-loop.
//
// A node contributes rows even when it appears twice in unit_nodes: the
// reference does not deduplicate, so neither does this.
func BuildMembershipRecords(
	input MembershipInput,
	themeDistribution *Distribution,
	subcategoryDistribution *Distribution,
) []MembershipRecord {
	themeRows := MembershipCategories(themeDistribution)
	subcategoryRows := MembershipCategories(subcategoryDistribution)

	records := make([]MembershipRecord, 0,
		len(input.UnitNodes)*(len(themeRows)+len(subcategoryRows)))

	appendRows := func(node NodeKey, kind string, rows []MembershipRow) {
		for _, row := range rows {
			records = append(records, MembershipRecord{
				OrgID:                input.OrgID,
				NodeType:             node.Type,
				NodeID:               node.ID,
				WorkUnitID:           input.WorkUnitID,
				CategoryKind:         kind,
				Category:             row.Category,
				Weight:               row.Weight,
				IsDominant:           row.IsDominant,
				CategorizationStatus: input.CategorizationStatus,
				ComputedAt:           input.ComputedAt,
				RunID:                input.RunID,
			})
		}
	}

	for _, node := range input.UnitNodes {
		// "theme" before "subcategory", per node. Not a stylistic choice.
		appendRows(node, "theme", themeRows)
		appendRows(node, "subcategory", subcategoryRows)
	}
	return records
}

package goapiproof

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/vektah/gqlparser/v2/ast"
	"github.com/vektah/gqlparser/v2/parser"
)

// StochasticLeafClass declares the leaves of an operation whose values are
// DRAWN per request -- a Monte Carlo percentile -- so that two responses to
// one request disagree by construction, on either plane and across planes.
// Equality of such a leaf proves nothing: it holds only when both draws land
// on the same value.
//
// Mechanism. A leaf named here is not compared by value across planes. Every
// other check on it still runs:
//
//   - presence and null: the comparator still reports a leaf present on one
//     plane only, or null on one plane and a value on the other. Null means
//     "no forecast", which is not a random outcome, so it stays a finding.
//   - type, per plane: a date leaf must be a "YYYY-MM-DD" string and an
//     integer leaf an integer JSON number, or null.
//   - order, per plane: the non-null leaves of each Ordering must be
//     non-decreasing in the order listed.
//   - date offset, per plane: each DateOffset's date leaf must equal the UTC
//     calendar date of BaseTimestampPath plus its days leaf, and the two must
//     be null together.
//
// A failed check is a mismatch finding whose shape no citation can cover. A
// drawn-value difference that passes every check is recorded as a
// FindingStochasticLeaf, not a mismatch, and the verdict is recorded as proven
// under this class (see Result.StochasticLeafCitation) -- never as a match,
// whether or not the draws happened to agree.
//
// Scope. Only the exact paths listed in Orderings are covered: there is no
// prefix, subtree or wildcard form, and a path that crosses a list, names a
// container, names a field the registered document does not select, or is
// also named by another declaration is refused before any request is sent.
//
// Blind spot. A plane whose draws come from the wrong distribution but still
// keep the declared order and offsets passes this class. The distribution is
// pinned elsewhere, by seeded golden fixtures, not by this comparison.
type StochasticLeafClass struct {
	// Ticket is the issue that owns the class.
	Ticket string
	// Reason states the mechanism, scope and blind spot in words.
	Reason string
	// Orderings lists every covered leaf, once, grouped by type. Within one
	// plane the non-null values of a group must be non-decreasing in the
	// order given.
	Orderings []StochasticOrdering
	// DateOffsets pairs a date leaf with the integer leaf it is derived from.
	DateOffsets []StochasticDateOffset
	// BaseTimestampPath names the leaf whose UTC calendar date every
	// DateOffset is measured from. It is not itself a covered leaf.
	BaseTimestampPath string
}

// StochasticOrdering is one typed group of covered leaves.
type StochasticOrdering struct {
	// Type is StochasticTypeDate or StochasticTypeInteger.
	Type string
	// NonDecreasing lists full, dotted, index-free leaf paths.
	NonDecreasing []string
}

// StochasticDateOffset states date == UTC date of the base timestamp + days.
type StochasticDateOffset struct {
	DatePath string
	DaysPath string
}

// Covered leaf types.
const (
	StochasticTypeDate    = "date"
	StochasticTypeInteger = "integer"
)

// FindingStochasticLeaf records a drawn-value difference on a covered leaf.
// It is an observation, not a mismatch: it never sets the terminal state and
// is never counted outside a citation.
const FindingStochasticLeaf = "stochastic_leaf_value"

// Shapes of a failed class check. None is a leaf shape, so no citation covers
// them.
const (
	ShapeStochasticType       = "stochastic_type"
	ShapeStochasticOrder      = "stochastic_order"
	ShapeStochasticDateOffset = "stochastic_date_offset"
	// ShapeStochasticLeaf labels the drawn-value differences a class
	// covered, in Result.CoveredByShape.
	ShapeStochasticLeaf = "stochastic_leaf"
)

// ProvenUnderStochasticLeafClass is the word a report prints for an
// operation proven under a StochasticLeafClass.
const ProvenUnderStochasticLeafClass = "stochastic_leaf_class"

// StochasticLeafCitationPrefix prefixes the class ticket in a receipt's
// baseline_defect array, so the row says which citation is a class and which
// is a defect in the baseline.
const StochasticLeafCitationPrefix = ProvenUnderStochasticLeafClass + ":"

const stochasticDateLayout = "2006-01-02"

// stochasticTimestampLayouts are the two wire forms of a timestamp leaf: RFC
// 3339 with a "T" separator, and the space-separated form of a Python
// datetime's str().
var stochasticTimestampLayouts = []string{
	"2006-01-02T15:04:05.999999999Z07:00",
	"2006-01-02 15:04:05.999999999Z07:00",
}

// Leaves returns every covered leaf path, sorted.
func (c *StochasticLeafClass) Leaves() []string {
	var out []string
	for _, ordering := range c.Orderings {
		out = append(out, ordering.NonDecreasing...)
	}
	sort.Strings(out)
	return out
}

// Citation is the string a receipt carries for this class.
func (c *StochasticLeafClass) Citation() string {
	return StochasticLeafCitationPrefix + c.Ticket
}

// validateStochasticPath refuses every path form that could reach more than
// the one leaf it names.
func validateStochasticPath(path string) error {
	if path != strings.TrimSpace(path) || path == "" {
		return fmt.Errorf("goapiproof: stochastic leaf path %q is blank or padded", path)
	}
	if strings.ContainsAny(path, "*[]$ \t") {
		return fmt.Errorf("goapiproof: stochastic leaf path %q carries a wildcard, index or root marker -- only a full dotted leaf path is accepted", path)
	}
	segments := strings.Split(path, ".")
	if segments[0] != "data" || len(segments) < 3 {
		return fmt.Errorf("goapiproof: stochastic leaf path %q must be data.<root>.<leaf>", path)
	}
	for _, segment := range segments {
		if segment == "" {
			return fmt.Errorf("goapiproof: stochastic leaf path %q has an empty segment", path)
		}
	}
	return nil
}

// validateStochasticLeafClass checks a declaration on its own and against
// the other declarations of the same Options. It needs no response and no
// document.
func validateStochasticLeafClass(opts Options) error {
	class := opts.StochasticLeaves
	if class == nil {
		return nil
	}
	if NamesNothing(class.Ticket) || NamesNothing(class.Reason) {
		return errors.New("goapiproof: a stochastic leaf class needs a Ticket and a Reason")
	}
	if len(class.Orderings) == 0 {
		return errors.New("goapiproof: a stochastic leaf class names no leaves")
	}
	typeOf := map[string]string{}
	for _, ordering := range class.Orderings {
		if ordering.Type != StochasticTypeDate && ordering.Type != StochasticTypeInteger {
			return fmt.Errorf("goapiproof: stochastic ordering type %q is neither %q nor %q", ordering.Type, StochasticTypeDate, StochasticTypeInteger)
		}
		if len(ordering.NonDecreasing) < 2 {
			return fmt.Errorf("goapiproof: stochastic ordering %v names fewer than two leaves, so it orders nothing", ordering.NonDecreasing)
		}
		for _, path := range ordering.NonDecreasing {
			if err := validateStochasticPath(path); err != nil {
				return err
			}
			if _, dup := typeOf[path]; dup {
				return fmt.Errorf("goapiproof: stochastic leaf %q is named twice", path)
			}
			typeOf[path] = ordering.Type
		}
	}
	for path := range typeOf {
		for other := range typeOf {
			if strings.HasPrefix(other, path+".") {
				return fmt.Errorf("goapiproof: stochastic leaf %q is the parent of %q, so it is not a leaf", path, other)
			}
		}
		if _, ok := opts.VolatileFields[path]; ok {
			return fmt.Errorf("goapiproof: %q is both a stochastic leaf and a volatile field", path)
		}
		if _, ok := opts.FloatTierB[path]; ok {
			return fmt.Errorf("goapiproof: %q is both a stochastic leaf and a Tier-B float", path)
		}
		for _, defect := range opts.BaselineDefects {
			if defectCovers(defect, path) {
				return fmt.Errorf("goapiproof: stochastic leaf %q is also under baseline defect %s", path, defect.Ticket)
			}
		}
		for _, list := range opts.OrderInsensitiveLists {
			if strings.HasPrefix(path, list.Path+".") {
				return fmt.Errorf("goapiproof: stochastic leaf %q lies inside the list %q", path, list.Path)
			}
		}
	}
	if len(class.DateOffsets) > 0 {
		if err := validateStochasticPath(class.BaseTimestampPath); err != nil {
			return err
		}
		if _, covered := typeOf[class.BaseTimestampPath]; covered {
			return fmt.Errorf("goapiproof: base timestamp %q cannot also be a stochastic leaf", class.BaseTimestampPath)
		}
	} else if class.BaseTimestampPath != "" {
		return fmt.Errorf("goapiproof: base timestamp %q is declared with no date offset to use it", class.BaseTimestampPath)
	}
	for _, offset := range class.DateOffsets {
		if typeOf[offset.DatePath] != StochasticTypeDate {
			return fmt.Errorf("goapiproof: date offset names %q, which is not a declared date leaf", offset.DatePath)
		}
		if typeOf[offset.DaysPath] != StochasticTypeInteger {
			return fmt.Errorf("goapiproof: date offset names %q, which is not a declared integer leaf", offset.DaysPath)
		}
	}
	return nil
}

// ValidateStochasticLeafClassAgainstDocument checks that every covered leaf,
// and the base timestamp, is a field the registered document selects with no
// sub-selection. A name the document does not select, or a field that has
// its own selection set, is refused.
func ValidateStochasticLeafClassAgainstDocument(opts Options, document string) error {
	if err := validateStochasticLeafClass(opts); err != nil {
		return err
	}
	class := opts.StochasticLeaves
	if class == nil {
		return nil
	}
	parsed, gqlErr := parser.ParseQuery(&ast.Source{Input: document})
	if gqlErr != nil {
		return fmt.Errorf("goapiproof: parse registered document: %w", gqlErr)
	}
	if len(parsed.Operations) != 1 {
		return fmt.Errorf("goapiproof: registered document carries %d operations, want 1", len(parsed.Operations))
	}
	paths := class.Leaves()
	if class.BaseTimestampPath != "" {
		paths = append(paths, class.BaseTimestampPath)
	}
	for _, path := range paths {
		segments := strings.Split(path, ".")[1:]
		selection := parsed.Operations[0].SelectionSet
		for i, segment := range segments {
			field, found := selectedField(selection, segment, parsed.Fragments)
			if !found {
				return fmt.Errorf("goapiproof: stochastic leaf %q: the registered document does not select %q", path, strings.Join(segments[:i+1], "."))
			}
			last := i == len(segments)-1
			if last && len(field.SelectionSet) > 0 {
				return fmt.Errorf("goapiproof: stochastic leaf %q is a field with a sub-selection, not a leaf", path)
			}
			if !last && len(field.SelectionSet) == 0 {
				return fmt.Errorf("goapiproof: stochastic leaf %q: %q is a leaf with nothing under it", path, strings.Join(segments[:i+1], "."))
			}
			selection = field.SelectionSet
		}
	}
	return nil
}

// selectedField finds the field whose response key is name in a selection
// set, following inline fragments and fragment spreads.
func selectedField(selection ast.SelectionSet, name string, fragments ast.FragmentDefinitionList) (*ast.Field, bool) {
	for _, item := range selection {
		switch typed := item.(type) {
		case *ast.Field:
			key := typed.Alias
			if key == "" {
				key = typed.Name
			}
			if key == name {
				return typed, true
			}
		case *ast.InlineFragment:
			if field, ok := selectedField(typed.SelectionSet, name, fragments); ok {
				return field, true
			}
		case *ast.FragmentSpread:
			if definition := fragments.ForName(typed.Name); definition != nil {
				if field, ok := selectedField(definition.SelectionSet, name, fragments); ok {
					return field, true
				}
			}
		}
	}
	return nil, false
}

// stochasticLookup reads one dotted path from a decoded `data` value.
// present is false when a key on the way is absent or an intermediate value
// is null. err is set when the walk crosses a list or ends on a container:
// the declaration then does not describe the response.
func stochasticLookup(data any, path string) (value any, present bool, err error) {
	current := data
	segments := strings.Split(path, ".")[1:]
	for i, segment := range segments {
		switch typed := current.(type) {
		case nil:
			return nil, false, nil
		case map[string]any:
			next, ok := typed[segment]
			if !ok {
				return nil, false, nil
			}
			current = next
		case []any:
			return nil, false, fmt.Errorf("%q crosses a list at %q", path, strings.Join(segments[:i], "."))
		default:
			return nil, false, fmt.Errorf("%q passes through a scalar at %q", path, strings.Join(segments[:i], "."))
		}
	}
	if isContainerKind(jsonKind(current)) {
		return nil, false, fmt.Errorf("%q is a %s, not a leaf", path, jsonKind(current))
	}
	return current, true, nil
}

// stochasticPlane is one plane's typed view of the covered leaves.
type stochasticPlane struct {
	name     string
	dates    map[string]time.Time
	integers map[string]int64
}

// applyStochasticLeafClass runs the class over one comparison: it
// reclassifies drawn-value differences on covered leaves, appends the
// per-plane check findings, and returns refusals when the declaration does
// not describe the response. covered counts the reclassified differences.
func applyStochasticLeafClass(class *StochasticLeafClass, baselineData, candidateData any, findings []Finding) (out []Finding, covered int, refusals []string) {
	leaves := map[string]bool{}
	for _, path := range class.Leaves() {
		leaves[path] = true
	}
	out = make([]Finding, 0, len(findings))
	for _, finding := range findings {
		// Exact concrete path only: "$." + a covered leaf. A path with a
		// list index or any extra segment is never reclassified.
		if finding.Kind == FindingMismatch && finding.Shape == ShapeValue &&
			strings.HasPrefix(finding.Path, "$.") && leaves[strings.TrimPrefix(finding.Path, "$.")] {
			finding.Kind = FindingStochasticLeaf
			finding.Detail += " (drawn value, not compared under " + class.Citation() + ")"
			covered++
		}
		out = append(out, finding)
	}

	typeOf := map[string]string{}
	for _, ordering := range class.Orderings {
		for _, path := range ordering.NonDecreasing {
			typeOf[path] = ordering.Type
		}
	}
	reached := map[string]bool{}
	planes := make([]stochasticPlane, 0, 2)
	for _, side := range []struct {
		name string
		data any
	}{{"baseline", baselineData}, {"candidate", candidateData}} {
		plane := stochasticPlane{name: side.name, dates: map[string]time.Time{}, integers: map[string]int64{}}
		for _, path := range class.Leaves() {
			value, present, err := stochasticLookup(side.data, path)
			if err != nil {
				refusals = append(refusals, fmt.Sprintf("%s plane: stochastic leaf %s", side.name, err))
				continue
			}
			if !present {
				continue
			}
			reached[path] = true
			if value == nil {
				continue
			}
			switch typeOf[path] {
			case StochasticTypeDate:
				text, isString := value.(string)
				parsed, parseErr := time.Parse(stochasticDateLayout, text)
				if !isString || parseErr != nil || parsed.Format(stochasticDateLayout) != text {
					out = append(out, stochasticFinding(path, ShapeStochasticType, "%s plane: %v is not a YYYY-MM-DD date", side.name, value))
					continue
				}
				plane.dates[path] = parsed
			case StochasticTypeInteger:
				number, isInt := asInt64(value)
				if !isInt {
					out = append(out, stochasticFinding(path, ShapeStochasticType, "%s plane: %v is not an integer", side.name, value))
					continue
				}
				plane.integers[path] = number
			}
		}
		planes = append(planes, plane)

		if len(class.DateOffsets) == 0 {
			continue
		}
		base, basePresent, err := stochasticLookup(side.data, class.BaseTimestampPath)
		if err != nil {
			refusals = append(refusals, fmt.Sprintf("%s plane: base timestamp %s", side.name, err))
			continue
		}
		if basePresent {
			reached[class.BaseTimestampPath] = true
		}
		var baseDate time.Time
		baseKnown := false
		if text, isString := base.(string); isString {
			for _, layout := range stochasticTimestampLayouts {
				if moment, parseErr := time.Parse(layout, text); parseErr == nil {
					utc := moment.UTC()
					baseDate = time.Date(utc.Year(), utc.Month(), utc.Day(), 0, 0, 0, 0, time.UTC)
					baseKnown = true
					break
				}
			}
		}
		for _, offset := range class.DateOffsets {
			date, hasDate := plane.dates[offset.DatePath]
			days, hasDays := plane.integers[offset.DaysPath]
			if hasDate != hasDays {
				out = append(out, stochasticFinding(offset.DatePath, ShapeStochasticDateOffset,
					"%s plane: %s and %s are not null together", side.name, offset.DatePath, offset.DaysPath))
				continue
			}
			if !hasDate {
				continue
			}
			if !baseKnown {
				out = append(out, stochasticFinding(class.BaseTimestampPath, ShapeStochasticType,
					"%s plane: base timestamp %v is not a parseable timestamp, so %s cannot be checked against %s", side.name, base, offset.DatePath, offset.DaysPath))
				continue
			}
			if want := baseDate.AddDate(0, 0, int(days)); !want.Equal(date) {
				out = append(out, stochasticFinding(offset.DatePath, ShapeStochasticDateOffset,
					"%s plane: %s is %s but base date %s + %s (%d) is %s", side.name, offset.DatePath,
					date.Format(stochasticDateLayout), baseDate.Format(stochasticDateLayout), offset.DaysPath, days, want.Format(stochasticDateLayout)))
			}
		}
	}

	for _, plane := range planes {
		for _, ordering := range class.Orderings {
			for i := 0; i < len(ordering.NonDecreasing); i++ {
				for j := i + 1; j < len(ordering.NonDecreasing); j++ {
					lower, upper := ordering.NonDecreasing[i], ordering.NonDecreasing[j]
					if violated, detail := stochasticOrderViolated(plane, ordering.Type, lower, upper); violated {
						out = append(out, stochasticFinding(upper, ShapeStochasticOrder, "%s plane: %s", plane.name, detail))
					}
				}
			}
		}
	}

	var unreached []string
	for _, path := range class.Leaves() {
		if !reached[path] {
			unreached = append(unreached, path)
		}
	}
	if len(class.DateOffsets) > 0 && !reached[class.BaseTimestampPath] {
		unreached = append(unreached, class.BaseTimestampPath)
	}
	if len(unreached) > 0 {
		refusals = append(refusals, fmt.Sprintf("stochastic leaf class %s names paths neither plane carries: %v", class.Ticket, unreached))
	}
	sort.Strings(refusals)
	return out, covered, refusals
}

// stochasticOrderViolated compares two non-null leaves of one plane. Every
// pair in an ordering is checked, not only neighbours, so a null in the
// middle does not hide an inversion across it.
func stochasticOrderViolated(plane stochasticPlane, kind, lower, upper string) (bool, string) {
	switch kind {
	case StochasticTypeDate:
		low, hasLow := plane.dates[lower]
		high, hasHigh := plane.dates[upper]
		if hasLow && hasHigh && high.Before(low) {
			return true, fmt.Sprintf("%s %s is before %s %s", upper, high.Format(stochasticDateLayout), lower, low.Format(stochasticDateLayout))
		}
	case StochasticTypeInteger:
		low, hasLow := plane.integers[lower]
		high, hasHigh := plane.integers[upper]
		if hasLow && hasHigh && high < low {
			return true, fmt.Sprintf("%s %d is less than %s %d", upper, high, lower, low)
		}
	}
	return false, ""
}

func stochasticFinding(path, shape, format string, args ...any) Finding {
	return Finding{Kind: FindingMismatch, Path: "$." + path, Detail: fmt.Sprintf(format, args...), Shape: shape}
}

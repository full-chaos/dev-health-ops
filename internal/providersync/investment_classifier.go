package providersync

import (
	"fmt"
	"os"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// Go port of the legacy rule-based investment classifier
// (analytics/investment.py). It is a DEPRECATED path in Python -- the canonical
// Investment View is WorkUnit-based -- and this port exists only to serve the
// legacy daily investment_* destinations, exactly as the Python one does.
//
// The rules are read from the REAL config file rather than transcribed into Go.
// A transcribed copy is a second, unversioned source of truth: the same defect
// class that left the derived-destination integration tests asserting against a
// pre-053 enum while production emitted values the copy could not represent. The
// path is supplied by the caller, mirroring Python's `InvestmentClassifier(
// config_path)`, so both engines can be pointed at one file and compared.
//
// Per D16 this mirrors Python bug-for-bug. Four of the 44 rules in the checked-in
// config are UNREACHABLE from the work-item call site, and that is reproduced
// rather than corrected; see investment_classifier_reachability_test.go, which
// DERIVES that dead set by execution and fails if a future config edit revives
// one.
//
// # Why the config is decoded as yaml.Node and not into typed structs
//
// Python reads the file with `yaml.safe_load` into plain dicts, and every
// lookup is `d.get(key, default)`. That makes an EXPLICIT NULL and an ABSENT
// KEY two different things everywhere: `priority:` yields None (and blows up
// the sort), while an absent `priority` yields 100; `match:` yields None (and
// blows up `_matches`), while an absent `match` yields {} and matches
// everything. A typed decode -- including one using pointers per field --
// collapses those two into each other, because yaml.v3 decodes an explicit
// null into the same nil a missing key leaves behind. The first version of this
// port did exactly that, and the result was a classifier that returned a
// plausible product/general/0.0/legacy_default (or, worse, FIRED a rule whose
// `match:` was null) for eight config shapes on which Python raises. That is
// fail-open in the one direction D16 says is unacceptable: during Python/Go
// coexistence both engines read the SAME file, so a Go engine that invents an
// answer where Python refuses silently writes rows Python would never have
// written.
//
// So the document is walked as yaml.Nodes, where "absent", "present and null"
// and "present with a value" are three distinguishable states, and every shape
// on which Python raises returns an *InvestmentConfigError naming the exception
// class Python raises for it. Those classes are MEASURED, not inferred: the
// refusal oracle pair (testdata/oracle_pairs/analytics_investment_refusal.py)
// executes the real Python classifier against the same files and compares the
// exception type name against what this file produces.
//
// # Declared divergences (Go refuses where Python proceeds)
//
// Node-walking removed the two divergences an earlier typed decode had -- a
// duplicate mapping key (PyYAML keeps the LAST, yaml.v3's typed decoder
// rejects the document) and a bare-string `component:`/`label:` (Python does
// substring containment / character iteration) are both mirrored exactly now,
// each pinned by its own oracle case. What remains, and is NOT mirrored:
//
//  1. A non-string scalar where Python would yield a non-string VALUE:
//     `id: 7`, `investment_area: true`. Python puts the int/bool straight into
//     InvestmentClassification, whose Python annotation is `str` and is not
//     enforced. Go's *string cannot hold it, so this refuses rather than
//     silently coerce to "7".
//  2. Two or more rules whose `priority` values are all non-numeric but
//     mutually comparable (`priority: "a"` everywhere). Python's sort succeeds;
//     this refuses. A single rule with a non-numeric priority is MIRRORED --
//     sorted() never calls the comparator for one element, so Python does not
//     raise there either.
//  3. A document yaml.v3's parser rejects outright but PyYAML accepts.
//
// All three are Go-erroring-where-Python-proceeds, which is the loud/safe
// direction: the job fails visibly rather than emitting a row Python would not
// have. None of the three occurs in the checked-in config.

const (
	legacyDefaultInvestmentArea = "product"
	legacyDefaultProjectStream  = "general"
	// legacyDefaultRulePriority mirrors `x.get("priority", 100)`: a rule with no
	// priority sorts as 100, which is AFTER the 40-and-below rules and BEFORE
	// the 999 catch-all.
	legacyDefaultRulePriority = 100
	legacyFallbackRuleID      = "legacy_default"
	legacyUnnamedRuleID       = "legacy_rule"
)

// YAML resolved tags this file distinguishes. Compared as strings rather than
// re-derived per call site so a typo cannot silently make a branch dead.
const (
	yamlNullTag  = "!!null"
	yamlBoolTag  = "!!bool"
	yamlIntTag   = "!!int"
	yamlFloatTag = "!!float"
	yamlStrTag   = "!!str"
	yamlSeqTag   = "!!seq"
	yamlMapTag   = "!!map"
)

// InvestmentConfigError is returned for every config/artifact combination on
// which the Python classifier RAISES, plus the small declared set on which
// Python proceeds and this port refuses rather than emit a value it cannot
// represent (see the file comment).
//
// PythonException names the exception CLASS Python raises -- "AttributeError"
// or "TypeError" -- and is the value the refusal oracle compares, so a Go
// refusal for the wrong reason cannot pass as agreement. It is EMPTY for the
// declared divergences, which is the type-level marker that Python does not
// raise there at all.
type InvestmentConfigError struct {
	PythonException string
	Detail          string
}

func (err *InvestmentConfigError) Error() string {
	if err.PythonException == "" {
		return fmt.Sprintf(
			"investment config: declared divergence, Python proceeds but this port "+
				"refuses: %s", err.Detail)
	}
	return fmt.Sprintf(
		"investment config: Python raises %s here: %s", err.PythonException, err.Detail)
}

func investmentAttributeError(format string, args ...any) error {
	return &InvestmentConfigError{
		PythonException: "AttributeError",
		Detail:          fmt.Sprintf(format, args...),
	}
}

func investmentTypeError(format string, args ...any) error {
	return &InvestmentConfigError{
		PythonException: "TypeError",
		Detail:          fmt.Sprintf(format, args...),
	}
}

// investmentUnrepresentable is the DECLARED-divergence constructor: Python
// proceeds and produces a value this port's types cannot hold. It carries no
// exception class precisely so it can never be mistaken for a mirrored raise.
func investmentUnrepresentable(format string, args ...any) error {
	return &InvestmentConfigError{Detail: fmt.Sprintf(format, args...)}
}

// InvestmentClassification mirrors the Python dataclass of the same name.
//
// Three of its four fields are *string rather than string because Python can
// and does put None in each of them: `id:`, `investment_area:` and
// `project_stream:` are all read with `.get(key, default)`, so a key that is
// PRESENT AND NULL returns None rather than the default. The Python dataclass
// annotates investment_area and rule_id as `str`, but a dataclass does not
// enforce annotations, so None reaches the call site regardless. A plain Go
// string would have to invent "product"/"general"/"legacy_rule" there, which is
// a silent value divergence in the fail-open direction.
type InvestmentClassification struct {
	InvestmentArea *string `json:"investment_area"`
	ProjectStream  *string `json:"project_stream"`
	Confidence     float64 `json:"confidence"`
	RuleID         *string `json:"rule_id"`
}

// investmentRule is one entry of the config's `rules:` list, held as raw nodes.
// Nothing about a rule is interpreted at load time except its priority, because
// Python interprets nothing else at load time either: `_load_rules` only sorts,
// so a rule whose `match:` is null is inert until classify() actually REACHES
// it, and a rule whose `output:` is null is inert until it MATCHES. Interpreting
// eagerly here would move those raises earlier than Python's and turn a config
// that Python classifies fine into a Go-side load failure.
type investmentRule struct {
	id       *yaml.Node
	priority *yaml.Node
	match    *yaml.Node
	output   *yaml.Node
	// sortKey is `x.get("priority", 100)` as a float, valid only when
	// priorityKind is investmentPriorityNumeric.
	sortKey      float64
	priorityKind investmentPriorityKind
}

// InvestmentArtifact is the classifier's input. Python passes a plain dict and
// the call site (job_work_items.py:1377) supplies exactly four keys: labels,
// component, title and provider.
//
// Title and Provider are carried here even though NOTHING reads them, because
// the Python matcher does not read them either -- it inspects only labels,
// paths and component. Dropping them would make the Go signature quietly
// narrower than the contract it ports, and the docstring's claim that `title`
// and `epic` participate would then have no visible counter-evidence.
type InvestmentArtifact struct {
	Labels []string
	// Paths is what the matcher's path_prefix arm reads. The work-item call
	// site never populates it, which is precisely why every path_prefix rule is
	// dead on that path; it stays here because the field is what makes that
	// deadness a property of the CALLER rather than of this engine.
	Paths []string
	// Component is a POINTER because Python reads it with
	// `artifact.get("component")` -- no default -- so an absent key yields None,
	// which is a different value from "" for both the `in` membership test AND
	// the bare-string containment path (`None in "analytics"` raises where
	// `"" in "analytics"` is True). The work-item call site always supplies the
	// key, and always as "" (WorkItem has no `component` attribute, so
	// `getattr(item, "component", "")` cannot return anything else), so nil is
	// unreachable from production -- but the contract this ports can express it
	// and so must this.
	Component *string
	// Read by neither engine. See the type comment.
	Title    string
	Provider string
}

// InvestmentClassifier holds the priority-ordered rules.
type InvestmentClassifier struct {
	rules []investmentRule
}

// NewInvestmentClassifier mirrors `InvestmentClassifier.__init__` +
// `_load_rules`, including its MISSING-FILE behaviour: Python logs a warning
// and returns an empty rule list rather than raising, so a classifier built
// against an absent path still answers, always with the legacy default. That
// is reproduced instead of failing closed, because failing closed here would
// change which rows the legacy destinations emit.
func NewInvestmentClassifier(configPath string) (*InvestmentClassifier, error) {
	contents, err := os.ReadFile(configPath)
	if err != nil {
		if os.IsNotExist(err) {
			// Python: logger.warning(...); return []
			return &InvestmentClassifier{rules: nil}, nil
		}
		return nil, err
	}
	var root yaml.Node
	if err := yaml.Unmarshal(contents, &root); err != nil {
		// Declared divergence 3: a document yaml.v3's PARSER rejects. Note that
		// duplicate mapping keys do NOT land here -- yaml.v3 only rejects those
		// when decoding into a typed value, and this decodes into a Node.
		return nil, fmt.Errorf("investment config %s: %w", configPath, err)
	}
	investmentApplyPyYAMLBooleans(&root)
	document := investmentDocumentBody(&root)
	if document == nil || document.Tag == yamlNullTag {
		// An empty or comment-only file is `yaml.safe_load(...) is None`, and
		// `data.get("rules", [])` is then an attribute lookup on None.
		return nil, investmentAttributeError(
			"'NoneType' object has no attribute 'get': %s parsed to nothing", configPath)
	}
	if document.Kind != yaml.MappingNode {
		return nil, investmentAttributeError(
			"'%s' object has no attribute 'get': %s is not a mapping",
			investmentPythonTypeName(document), configPath)
	}
	rules, err := investmentDecodeRules(investmentMappingValue(document, "rules"))
	if err != nil {
		return nil, err
	}
	if err := investmentResolvePriorities(rules); err != nil {
		return nil, err
	}
	// Python sorts with sorted(), which is STABLE, and the checked-in config
	// relies on that: 37 of its 44 rules share priority 10 and a further 2
	// share priority 30. Among equal priorities the FILE ORDER decides which
	// rule classify() reaches first, and classify() returns on first match --
	// so an unstable sort would silently change the answer for any artifact
	// matching more than one priority-10 rule. sort.Slice is NOT stable.
	sort.SliceStable(rules, func(left, right int) bool {
		return rules[left].sortKey < rules[right].sortKey
	})
	return &InvestmentClassifier{rules: rules}, nil
}

// investmentDecodeRules mirrors `sorted(data.get("rules", []), key=...)`'s
// treatment of whatever sits under `rules:`. sorted() ITERATES its argument and
// calls `.get` on every element, so the shape of `rules` decides between a
// TypeError (not iterable at all) and an AttributeError (iterable, but its
// elements are not dicts) -- both measured against the real Python.
func investmentDecodeRules(node *yaml.Node) ([]investmentRule, error) {
	if node == nil {
		// `data.get("rules", [])` -- an absent key is an empty list, not an error.
		return nil, nil
	}
	elements, err := investmentIterate(node)
	if err != nil {
		return nil, err
	}
	rules := make([]investmentRule, 0, len(elements))
	for _, element := range elements {
		element = investmentResolveAlias(element)
		if element == nil || element.Kind != yaml.MappingNode {
			// sorted()'s key function runs on every element BEFORE any
			// comparison, so a non-dict rule raises at load even for a
			// single-element list.
			return nil, investmentAttributeError(
				"'%s' object has no attribute 'get': a rules entry is not a mapping",
				investmentPythonTypeName(element))
		}
		rules = append(rules, investmentRule{
			id:       investmentMappingValue(element, "id"),
			priority: investmentMappingValue(element, "priority"),
			match:    investmentMappingValue(element, "match"),
			output:   investmentMappingValue(element, "output"),
		})
	}
	return rules, nil
}

// investmentResolvePriorities computes `x.get("priority", 100)` for every rule
// and reproduces WHEN sorted() blows up on the result.
//
// The subtlety that makes this worth its own function: sorted() computes every
// key first and only then compares, so a single rule with `priority:` null does
// NOT raise -- there is nothing to compare it against. Two rules do. Measured
// on the real Python both ways; an "any null priority is fatal" shortcut would
// refuse a config Python classifies fine.
func investmentResolvePriorities(rules []investmentRule) error {
	for index := range rules {
		key, kind := investmentPrioritySortKey(rules[index].priority)
		rules[index].sortKey = key
		rules[index].priorityKind = kind
	}
	// With fewer than two rules sorted() never calls the comparator, so
	// nothing about the key's type or spelling can matter yet.
	if len(rules) < 2 {
		return nil
	}
	for _, rule := range rules {
		switch rule.priorityKind {
		case investmentPriorityNotComparable:
			return investmentTypeError(
				"'<' not supported between instances of 'int' and '%s': a rule's "+
					"priority is not a number and there is more than one rule to order",
				investmentPythonTypeName(rule.priority))
		case investmentPriorityAmbiguous:
			return investmentUnrepresentable(
				"priority %q is octal to PyYAML's YAML 1.1 resolver and decimal to "+
					"yaml.v3's 1.2 one, so no ordering here is the mirror",
				investmentResolveAlias(rule.priority).Value)
		}
	}
	return nil
}

// investmentPriorityKind is how `x.get("priority", 100)` behaves as a sort key.
type investmentPriorityKind int

const (
	// Python compares it fine and both resolvers agree on its value.
	investmentPriorityNumeric investmentPriorityKind = iota
	// Python raises on the first comparison (null, a string, a container).
	investmentPriorityNotComparable
	// Both engines sort, but they would sort DIFFERENTLY -- a declared
	// divergence rather than a mirrored raise, and so refused separately.
	investmentPriorityAmbiguous
)

// investmentPrioritySortKey returns the numeric sort key and how Python would
// treat it. bool counts as numeric because Python's bool IS an int subclass, so
// `priority: true` sorts as 1 rather than raising.
func investmentPrioritySortKey(node *yaml.Node) (float64, investmentPriorityKind) {
	if node == nil {
		return legacyDefaultRulePriority, investmentPriorityNumeric
	}
	node = investmentResolveAlias(node)
	if node == nil || node.Kind != yaml.ScalarNode {
		return 0, investmentPriorityNotComparable
	}
	switch node.Tag {
	case yamlIntTag:
		// A leading zero is the one integer literal the two resolvers read
		// differently: PyYAML's YAML 1.1 makes `010` OCTAL (8), yaml.v3's 1.2
		// makes it decimal 10. Sorting by either is a guess, and the wrong
		// guess reorders rules silently. No rule in the checked-in config has
		// one.
		digits := strings.TrimLeft(node.Value, "-+")
		if len(digits) > 1 && digits[0] == '0' {
			return 0, investmentPriorityAmbiguous
		}
	case yamlFloatTag, yamlBoolTag:
	default:
		return 0, investmentPriorityNotComparable
	}
	var number float64
	if err := node.Decode(&number); err != nil {
		return 0, investmentPriorityNotComparable
	}
	return number, investmentPriorityNumeric
}

// Classify mirrors InvestmentClassifier.classify: first matching rule wins, and
// an unmatched artifact falls back to the legacy product/general bucket with
// confidence 0.0 rather than to anything unknown-like.
//
// It returns an error because Python RAISES here for several config shapes, and
// which rule the artifact reaches decides whether it does: a rule with a null
// `match:` is harmless until classify() gets to it, and a rule with a null
// `output:` is harmless until it MATCHES. Both are pinned by cases that
// classify the same file with and without reaching the offending rule.
func (classifier *InvestmentClassifier) Classify(
	artifact InvestmentArtifact,
) (InvestmentClassification, error) {
	for _, rule := range classifier.rules {
		matched, err := investmentRuleMatches(rule.match, artifact)
		if err != nil {
			return InvestmentClassification{}, err
		}
		if !matched {
			continue
		}
		// Python evaluates `output = rule.get("output", {})` before the match
		// test but only DEREFERENCES it after, and `.get` on a dict cannot
		// raise -- so the null-output AttributeError is a property of matching,
		// not of iterating.
		area, stream, err := investmentRuleOutput(rule.output)
		if err != nil {
			return InvestmentClassification{}, err
		}
		id, err := investmentRuleID(rule.id)
		if err != nil {
			return InvestmentClassification{}, err
		}
		return InvestmentClassification{
			InvestmentArea: area,
			ProjectStream:  stream,
			Confidence:     1.0,
			RuleID:         id,
		}, nil
	}
	return InvestmentClassification{
		InvestmentArea: investmentString(legacyDefaultInvestmentArea),
		ProjectStream:  investmentString(legacyDefaultProjectStream),
		Confidence:     0.0,
		RuleID:         investmentString(legacyFallbackRuleID),
	}, nil
}

// investmentRuleID mirrors `rule.get("id", "legacy_rule")`.
//
// Absent, present-and-null and present-and-empty are THREE different answers --
// "legacy_rule", None and "" -- and each has its own oracle case. The previous
// version of this port collapsed the first two into "legacy_rule" and claimed a
// synthetic case pinned it; no such case existed, and the behaviour diverged.
func investmentRuleID(node *yaml.Node) (*string, error) {
	return investmentOptionalString(node, legacyUnnamedRuleID, "id")
}

// investmentRuleOutput mirrors the two `output.get(...)` reads.
func investmentRuleOutput(node *yaml.Node) (*string, *string, error) {
	if node == nil {
		// `rule.get("output", {})` -- an absent block is {}, so both reads take
		// their defaults.
		return investmentString(legacyDefaultInvestmentArea),
			investmentString(legacyDefaultProjectStream), nil
	}
	node = investmentResolveAlias(node)
	if node == nil || node.Kind != yaml.MappingNode {
		return nil, nil, investmentAttributeError(
			"'%s' object has no attribute 'get': a matched rule's output block is "+
				"not a mapping", investmentPythonTypeName(node))
	}
	area, err := investmentOptionalString(
		investmentMappingValue(node, "investment_area"),
		legacyDefaultInvestmentArea, "investment_area")
	if err != nil {
		return nil, nil, err
	}
	stream, err := investmentOptionalString(
		investmentMappingValue(node, "project_stream"),
		legacyDefaultProjectStream, "project_stream")
	if err != nil {
		return nil, nil, err
	}
	return area, stream, nil
}

// investmentOptionalString is the shared `.get(key, default)` for the three
// fields that reach InvestmentClassification as strings: absent yields the
// default, explicit null yields None, a string yields itself.
func investmentOptionalString(
	node *yaml.Node, fallback string, key string,
) (*string, error) {
	if node == nil {
		return investmentString(fallback), nil
	}
	node = investmentResolveAlias(node)
	if node == nil || node.Tag == yamlNullTag {
		return nil, nil
	}
	if node.Kind == yaml.ScalarNode && node.Tag == yamlStrTag {
		return investmentString(node.Value), nil
	}
	// Declared divergence 1: Python would put this non-string straight into the
	// dataclass. Coercing it to text here would be a silent value change.
	return nil, investmentUnrepresentable(
		"%s is a %s; Python would carry that value into InvestmentClassification "+
			"unchanged and this port's *string cannot hold it",
		key, investmentPythonTypeName(node))
}

// investmentRuleMatches mirrors `_matches`.
//
// The structure matters as much as the conditions: each arm is a REJECTION
// test, and a criterion that is absent is not tested at all. So an EMPTY
// `match: {}` reaches the final `return true` and matches every artifact --
// including one with no labels at all. The real config expresses its catch-all
// as `always: true` instead, so only a synthetic config reaches the empty-map
// form; it is pinned there.
func investmentRuleMatches(match *yaml.Node, artifact InvestmentArtifact) (bool, error) {
	if match == nil {
		// `rule.get("match", {})` yields {} for an absent key, and {} matches
		// everything by the rule above.
		return true, nil
	}
	match = investmentResolveAlias(match)
	if match == nil || match.Kind != yaml.MappingNode {
		// A null (or otherwise non-dict) `match:` is inert until classify()
		// reaches this rule, and then `match_criteria.get("always")` raises.
		return false, investmentAttributeError(
			"'%s' object has no attribute 'get': a rule's match block is not a mapping",
			investmentPythonTypeName(match))
	}
	// Python tests `match_criteria.get("always")` for TRUTHINESS, so
	// `always: false` does not short-circuit -- it falls through to the
	// remaining criteria exactly as an absent key would. A non-empty string,
	// including the `always: 'no'` that YAML would otherwise fold to false,
	// IS truthy.
	if investmentTruthy(investmentMappingValue(match, "always")) {
		return true, nil
	}
	if label := investmentMappingValue(match, "label"); label != nil {
		targets, err := investmentLoweredStrings(label)
		if err != nil {
			return false, err
		}
		labels := make(map[string]struct{}, len(artifact.Labels))
		for _, value := range artifact.Labels {
			labels[strings.ToLower(value)] = struct{}{}
		}
		intersects := false
		for _, target := range targets {
			if _, ok := labels[target]; ok {
				intersects = true
				break
			}
		}
		if !intersects {
			return false, nil
		}
	}
	if prefixes := investmentMappingValue(match, "path_prefix"); prefixes != nil {
		// Reads artifact PATHS, not path_prefix. The work-item call site
		// supplies none, so this arm rejects every artifact reaching it from
		// that path -- the reason three real rules are dead.
		//
		// The prefix list is resolved INSIDE the loop over artifact paths,
		// exactly where Python's inner `for prefix in target_prefixes` sits.
		// That is load-bearing, not stylistic: with a null `path_prefix:` and
		// an artifact carrying no paths, Python never enters the inner loop and
		// never raises -- it just rejects. Hoisting the resolution would refuse
		// a config Python classifies. Both directions are pinned by cases over
		// the same file.
		found := false
		for _, path := range artifact.Paths {
			targets, err := investmentIterate(prefixes)
			if err != nil {
				return false, err
			}
			for _, target := range targets {
				target = investmentResolveAlias(target)
				if target == nil || target.Kind != yaml.ScalarNode || target.Tag != yamlStrTag {
					return false, investmentTypeError(
						"startswith first arg must be str or a tuple of str, not %s: a "+
							"path_prefix entry is not a string",
						investmentPythonTypeName(target))
				}
				if strings.HasPrefix(path, target.Value) {
					found = true
					break
				}
			}
			if found {
				break
			}
		}
		if !found {
			return false, nil
		}
	}
	if component := investmentMappingValue(match, "component"); component != nil {
		contains, err := investmentComponentContains(component, artifact.Component)
		if err != nil {
			return false, err
		}
		if !contains {
			return false, nil
		}
	}
	return true, nil
}

// investmentComponentContains mirrors
// `artifact.get("component") not in match_criteria["component"]`, negated.
//
// `in` is overloaded in Python and the config's shape picks the meaning. A LIST
// (or dict) is membership -- exact equality, NOT case-folded, unlike the label
// arm. A bare STRING is substring containment, which is a latent Python bug
// this mirrors rather than fixes: `component: analytics` matches the work-item
// call site's "" for every artifact, because "" is a substring of everything.
func investmentComponentContains(node *yaml.Node, component *string) (bool, error) {
	node = investmentResolveAlias(node)
	if node == nil || node.Tag == yamlNullTag {
		return false, investmentTypeError(
			"argument of type 'NoneType' is not a container or iterable: a rule's " +
				"component criterion is null")
	}
	if node.Kind == yaml.ScalarNode && node.Tag == yamlStrTag {
		if component == nil {
			return false, investmentTypeError(
				"'in <string>' requires string as left operand, not NoneType: the " +
					"artifact has no component key and the criterion is a bare string")
		}
		return strings.Contains(node.Value, *component), nil
	}
	candidates, err := investmentIterate(node)
	if err != nil {
		// `x in 5` reports containment, not iteration, so the message differs
		// from investmentIterate's even though the class is the same.
		return false, investmentTypeError(
			"argument of type '%s' is not iterable: a rule's component criterion "+
				"is not a container", investmentPythonTypeName(node))
	}
	for _, candidate := range candidates {
		candidate = investmentResolveAlias(candidate)
		if candidate == nil {
			continue
		}
		if candidate.Tag == yamlNullTag {
			if component == nil {
				return true, nil
			}
			continue
		}
		if candidate.Kind == yaml.ScalarNode && candidate.Tag == yamlStrTag &&
			component != nil && candidate.Value == *component {
			return true, nil
		}
	}
	return false, nil
}

// investmentLoweredStrings mirrors
// `set(lbl.lower() for lbl in match_criteria["label"])`.
//
// The generator is consumed in full before the intersection is taken, so a
// non-string entry raises even when an earlier entry would have matched.
func investmentLoweredStrings(node *yaml.Node) ([]string, error) {
	entries, err := investmentIterate(node)
	if err != nil {
		return nil, err
	}
	lowered := make([]string, 0, len(entries))
	for _, entry := range entries {
		entry = investmentResolveAlias(entry)
		if entry == nil || entry.Kind != yaml.ScalarNode || entry.Tag != yamlStrTag {
			return nil, investmentAttributeError(
				"'%s' object has no attribute 'lower': a label entry is not a string",
				investmentPythonTypeName(entry))
		}
		lowered = append(lowered, strings.ToLower(entry.Value))
	}
	return lowered, nil
}

// investmentIterate mirrors `for x in obj` over a safe_load'ed value: a list
// yields its items, a dict yields its KEYS, and a string yields its CHARACTERS
// (which is why a bare-string `label:` silently matches single letters rather
// than the word). Anything else is not iterable and raises TypeError.
func investmentIterate(node *yaml.Node) ([]*yaml.Node, error) {
	node = investmentResolveAlias(node)
	if node == nil || node.Tag == yamlNullTag {
		return nil, investmentTypeError("'NoneType' object is not iterable")
	}
	switch node.Kind {
	case yaml.SequenceNode:
		return node.Content, nil
	case yaml.MappingNode:
		keys := make([]*yaml.Node, 0, len(node.Content)/2)
		seen := make(map[string]struct{}, len(node.Content)/2)
		for index := 0; index+1 < len(node.Content); index += 2 {
			key := node.Content[index]
			if _, duplicate := seen[key.Value]; duplicate {
				continue
			}
			seen[key.Value] = struct{}{}
			keys = append(keys, key)
		}
		return keys, nil
	case yaml.ScalarNode:
		if node.Tag != yamlStrTag {
			return nil, investmentTypeError(
				"'%s' object is not iterable", investmentPythonTypeName(node))
		}
		characters := make([]*yaml.Node, 0, len(node.Value))
		for _, character := range node.Value {
			characters = append(characters, &yaml.Node{
				Kind: yaml.ScalarNode, Tag: yamlStrTag, Value: string(character),
			})
		}
		return characters, nil
	default:
		return nil, investmentTypeError(
			"'%s' object is not iterable", investmentPythonTypeName(node))
	}
}

// investmentTruthy mirrors Python truthiness for a safe_load'ed value, which is
// what `if match_criteria.get("always")` actually tests. An absent key is None
// and therefore false; so are false, 0, "" and the empty container.
func investmentTruthy(node *yaml.Node) bool {
	node = investmentResolveAlias(node)
	if node == nil || node.Tag == yamlNullTag {
		return false
	}
	switch node.Kind {
	case yaml.SequenceNode, yaml.MappingNode:
		return len(node.Content) > 0
	case yaml.ScalarNode:
		switch node.Tag {
		case yamlBoolTag:
			var value bool
			return node.Decode(&value) == nil && value
		case yamlIntTag, yamlFloatTag:
			var value float64
			return node.Decode(&value) == nil && value != 0
		default:
			return node.Value != ""
		}
	default:
		return true
	}
}

// investmentPyYAMLBooleans are the plain scalars PyYAML's resolver reads as
// BOOLEANS and yaml.v3's does not.
//
// PyYAML implements YAML 1.1, whose bool set is
// yes/no/on/off/true/false in three casings each; yaml.v3 implements the YAML
// 1.2 core schema, where only true/false are boolean and everything else is an
// ordinary string. That difference is not cosmetic here, and it is not
// symmetric either -- it was fail-open in the arm that matters most:
//
//	match:
//	  always: no
//
// is False to Python, so the rule falls through to its remaining criteria; to
// yaml.v3 it is the non-empty string "no", which is TRUTHY, so the rule
// short-circuits and MATCHES EVERY ARTIFACT. One unquoted word in the config
// and the Go engine classifies everything under one rule while Python does not.
// The same difference makes `label: [no]` a bool (which has no .lower(), so
// Python raises) rather than a label that simply never matches.
//
// Normalising here, once, over the whole document is deliberate: doing it at
// each read site would leave whichever site was added last silently on the 1.2
// reading. Only PLAIN scalars are rewritten -- a quoted "no" is a string to both
// resolvers, and an explicitly tagged `!!str no` carries TaggedStyle, so both
// keep their string reading.
//
// Residual, NOT mirrored and not plausible in this config: the two resolvers
// also disagree about YAML 1.1 sexagesimals (`1:30` is 90 to PyYAML, a string
// to yaml.v3) and about plain timestamps (`2024-01-02` is a date to PyYAML,
// which this port then refuses as unrepresentable). Both land on Go refusing or
// rejecting rather than inventing a value. Leading-zero integers are refused
// explicitly; see investmentPrioritySortKey.
var investmentPyYAMLBooleans = map[string]string{
	"yes": "true", "Yes": "true", "YES": "true",
	"on": "true", "On": "true", "ON": "true",
	"no": "false", "No": "false", "NO": "false",
	"off": "false", "Off": "false", "OFF": "false",
}

func investmentApplyPyYAMLBooleans(node *yaml.Node) {
	if node == nil {
		return
	}
	if node.Kind == yaml.ScalarNode && node.Tag == yamlStrTag && node.Style == 0 {
		if canonical, ok := investmentPyYAMLBooleans[node.Value]; ok {
			node.Tag = yamlBoolTag
			node.Value = canonical
		}
	}
	// Aliases carry no Content, so this cannot cycle; the anchor itself is
	// reached once, in place, wherever it is defined.
	for _, child := range node.Content {
		investmentApplyPyYAMLBooleans(child)
	}
}

// investmentDocumentBody unwraps the document node yaml.Unmarshal produces.
// A file that is empty or only comments leaves the root node zero-valued, which
// is how "safe_load returned None" is detected without confusing it with an
// explicit `{}`.
func investmentDocumentBody(root *yaml.Node) *yaml.Node {
	if root == nil || root.Kind == 0 {
		return nil
	}
	if root.Kind == yaml.DocumentNode {
		if len(root.Content) == 0 {
			return nil
		}
		return investmentResolveAlias(root.Content[0])
	}
	return investmentResolveAlias(root)
}

// investmentMappingValue returns the value for `key`, or nil when the key is
// absent -- the distinction the whole node-walking decode exists to preserve.
//
// The LAST occurrence wins, because a duplicate mapping key is not an error in
// PyYAML: it silently keeps the last one. yaml.v3's typed decoder rejects the
// document instead, which is why this walks Content rather than decoding.
func investmentMappingValue(mapping *yaml.Node, key string) *yaml.Node {
	if mapping == nil || mapping.Kind != yaml.MappingNode {
		return nil
	}
	var found *yaml.Node
	for index := 0; index+1 < len(mapping.Content); index += 2 {
		if mapping.Content[index].Value == key {
			found = mapping.Content[index+1]
		}
	}
	return found
}

// investmentResolveAlias follows a YAML alias to its anchor, as PyYAML does
// before any of the above ever sees the value.
func investmentResolveAlias(node *yaml.Node) *yaml.Node {
	for node != nil && node.Kind == yaml.AliasNode {
		node = node.Alias
	}
	return node
}

// investmentPythonTypeName names the Python type `yaml.safe_load` would have
// produced for a node, so a mirrored exception's message says what Python's
// says. Only the CLASS is compared by the oracle; this is for the human reading
// the failure.
func investmentPythonTypeName(node *yaml.Node) string {
	if node == nil {
		return "NoneType"
	}
	switch node.Kind {
	case yaml.SequenceNode:
		return "list"
	case yaml.MappingNode:
		return "dict"
	}
	switch node.Tag {
	case yamlNullTag:
		return "NoneType"
	case yamlBoolTag:
		return "bool"
	case yamlIntTag:
		return "int"
	case yamlFloatTag:
		return "float"
	case yamlStrTag:
		return "str"
	case yamlSeqTag:
		return "list"
	case yamlMapTag:
		return "dict"
	default:
		return strings.TrimPrefix(node.Tag, "!!")
	}
}

func investmentString(value string) *string {
	return &value
}

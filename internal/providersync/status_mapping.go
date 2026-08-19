package providersync

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Go port of providers/status_mapping.py -- `load_status_mapping` plus the two
// `StatusMapping` normalizers.
//
// The mapping rules are READ FROM THE REAL config file rather than transcribed
// into Go. A transcribed copy is a second, unversioned source of truth that
// drifts silently; the path is supplied by the caller, mirroring Python's
// `load_status_mapping(path)`, so both engines can be pointed at one file and
// compared by the status/mapping/load oracle pair.
//
// Per D16 this mirrors Python BUG-FOR-BUG. The quirks reproduced deliberately
// are enumerated in status_mapping_reachability_test.go, each with a tripwire
// that fails in BOTH directions -- if the quirk is fixed upstream, and if the
// config section it depends on is renamed away.
//
// Scalar resolution, Python str()/repr(), and full Unicode case mapping live in
// status_mapping_pyyaml.go: go-yaml implements YAML 1.2 and PyYAML implements
// YAML 1.1, so agreeing on what a bare scalar even IS is a porting obligation,
// not something either library provides for free.

// Mirrors WorkItemStatusCategory in models/work_items.py. `_as_status_category`
// returns None for anything outside this set, and the loader then SKIPS that
// category silently -- a typo'd category name contributes nothing and raises
// nothing.
var validStatusCategories = map[string]bool{
	"backlog": true, "todo": true, "in_progress": true, "in_review": true,
	"blocked": true, "done": true, "canceled": true, "unknown": true,
}

// Mirrors WorkItemType in models/work_items.py. NOTE this set has TEN members
// while typePriority below has only EIGHT: "pr" and "merge_request" are valid
// work-item types that `_as_work_item_type` accepts and the loader will happily
// index, but they appear in NO priority list.
var validWorkItemTypes = map[string]bool{
	"story": true, "task": true, "bug": true, "epic": true, "pr": true,
	"merge_request": true, "issue": true, "incident": true, "chore": true,
	"unknown": true,
}

// _STATUS_PRIORITY: "if multiple label rules match, prefer more terminal states".
// Complete with respect to validStatusCategories (all 8 present).
var statusPriority = []string{
	"done", "canceled", "blocked", "in_review", "in_progress", "todo", "backlog", "unknown",
}

// _TYPE_PRIORITY. INCOMPLETE with respect to validWorkItemTypes -- "pr" and
// "merge_request" are absent. Mirrored as-is.
var typePriority = []string{
	"incident", "bug", "epic", "story", "task", "chore", "issue", "unknown",
}

// statusMappingProviders mirrors the hardcoded tuple in `load_status_mapping`:
//
//	for provider in ("jira", "github", "gitlab"):
//
// The checked-in config ALSO carries a `linear` section, which this tuple
// excludes, so no linear index is ever built. That is CHAOS-3505 and it is
// reproduced, not corrected -- see the reachability test.
var statusMappingProviders = []string{"jira", "github", "gitlab"}

// StatusMapping mirrors the frozen Python dataclass.
type StatusMapping struct {
	StatusByProvider      map[string]map[string]string
	LabelStatusByProvider map[string]map[string]string
	TypeByProvider        map[string]map[string]string
	LabelTypeByProvider   map[string]map[string]string
}

// normKey mirrors `_norm_key`:
//
//	" ".join((value or "").strip().lower().split())
//
// Two parts a naive port drops: the INTERNAL WHITESPACE COLLAPSE ("In  Progress"
// and " in progress " must both normalize to "in progress"), and the fact that
// Python's .lower() is full Unicode case mapping rather than Go's simple
// per-rune folding -- see normKeyLower.
func normKey(value string) string {
	return strings.Join(strings.Fields(normKeyLower(value)), " ")
}

// pythonStr reproduces Python's `str(raw)` for a YAML value, which is what
// `_index_values` applies before normalizing:
//
//	key = _norm_key(str(raw))
//
// For a plain string this is the identity. For any OTHER value it is Python's
// repr, and reproducing it exactly is load-bearing rather than pedantic:
// status_mapping.yaml:82 reads `- type: bug` (note the space), which YAML parses
// as a MAPPING, not the string "type:bug" the author intended. The resulting
// index key is the literal Python dict repr `{'type': 'bug'}`. Go's own
// formatting would render that node as `map[type:bug]` and diverge on the single
// most user-visible entry in the file.
//
// That misparse is CHAOS-3512: GitHub items carrying the conventional
// `type:bug` label classify as "issue" instead of "bug". It is fixed
// PYTHON-FIRST and then re-mirrored here -- this port reproduces the CURRENT
// behaviour, and the tripwire in status_mapping_reachability_test.go fails the
// moment either side changes, so the re-mirror cannot drift silently.
func pythonStr(node *yaml.Node) (string, error) {
	node = resolveAlias(node)
	if node == nil {
		return "None", nil
	}
	if node.Kind == yaml.ScalarNode {
		return pythonScalarStr(node)
	}
	return pythonRepr(node)
}

// pythonRepr reproduces Python's `repr()` for the YAML value kinds this config
// can contain. Anything it cannot represent faithfully is a hard error, never a
// best-effort fallback: a silently approximated key would produce a Go index
// that differs from Python's while every comparison still reported a match.
func pythonRepr(node *yaml.Node) (string, error) {
	node = resolveAlias(node)
	if node == nil {
		return "None", nil
	}
	switch node.Kind {
	case yaml.ScalarNode:
		return pythonScalarRepr(node)
	case yaml.MappingNode:
		// Python reprs a dict AFTER duplicate-key collapse, so the repr is built
		// from the collapsed entries rather than the raw node content.
		entries, err := mappingNodeEntries(node)
		if err != nil {
			return "", err
		}
		parts := make([]string, 0, len(entries))
		for _, entry := range entries {
			key, err := pythonRepr(entry.KeyNode)
			if err != nil {
				return "", err
			}
			val, err := pythonRepr(entry.Value)
			if err != nil {
				return "", err
			}
			parts = append(parts, key+": "+val)
		}
		return "{" + strings.Join(parts, ", ") + "}", nil
	case yaml.SequenceNode:
		parts := make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			val, err := pythonRepr(item)
			if err != nil {
				return "", err
			}
			parts = append(parts, val)
		}
		return "[" + strings.Join(parts, ", ") + "]", nil
	default:
		return "", fmt.Errorf(
			"pythonRepr: unsupported YAML node kind %v at line %d -- refusing to "+
				"approximate, because an approximated index key diverges from Python "+
				"while still comparing as present", node.Kind, node.Line)
	}
}

func resolveAlias(node *yaml.Node) *yaml.Node {
	for node != nil && node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	return node
}

// pyTruthy mirrors Python's truth test, which the loader leans on constantly via
// `x or {}` / `x or []`. This is why a config whose `status_categories` is an
// EMPTY list is fine (falsy, becomes {}) while a NON-EMPTY list raises: the
// difference is truthiness, not shape.
func pyTruthy(node *yaml.Node) bool {
	node = resolveAlias(node)
	if node == nil {
		return false
	}
	switch node.Kind {
	case yaml.MappingNode, yaml.SequenceNode:
		return len(node.Content) > 0
	case yaml.ScalarNode:
		switch resolvePyScalar(node) {
		case pyScalarNull:
			return false
		case pyScalarBool:
			return pyBoolIsTrue(node.Value)
		case pyScalarInt:
			parsed, err := pyParseInt(node.Value)
			return err == nil && parsed != 0
		case pyScalarFloat:
			parsed, err := pyParseFloat(node.Value)
			return err == nil && parsed != 0
		case pyScalarString:
			return node.Value != ""
		default:
			return true
		}
	}
	return true
}

// mappingEntry is one key/value pair of a YAML mapping, in the order Python's
// dict would iterate it.
type mappingEntry struct {
	Key     string
	KeyNode *yaml.Node
	Value   *yaml.Node
}

// mappingNodeEntries collapses a mapping node's content the way PyYAML's
// construction into a Python dict does.
//
// DUPLICATE KEYS: PyYAML does not reject them -- it assigns each in turn, so the
// LAST value wins while the key keeps the position of its FIRST appearance
// (`d={}; d['a']=1; d['b']=2; d['a']=3` leaves order [a, b] with a==3). go-yaml
// keeps BOTH pairs in Content, so a port that simply iterates Content applies a
// duplicated category TWICE (a union), and one that takes the first match reads
// the wrong value outright. Both are silent-wrong-value, which is why this is
// mirrored exactly rather than declared.
func mappingNodeEntries(node *yaml.Node) ([]mappingEntry, error) {
	node = resolveAlias(node)
	if node == nil || node.Kind != yaml.MappingNode {
		return nil, nil
	}
	entries := make([]mappingEntry, 0, len(node.Content)/2)
	position := make(map[string]int, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		keyNode := resolveAlias(node.Content[i])
		if keyNode == nil {
			continue
		}
		key, err := pythonStr(keyNode)
		if err != nil {
			return nil, err
		}
		entry := mappingEntry{Key: key, KeyNode: keyNode, Value: node.Content[i+1]}
		if at, seen := position[key]; seen {
			entries[at] = entry // last value wins, first position kept
			continue
		}
		position[key] = len(entries)
		entries = append(entries, entry)
	}
	return entries, nil
}

// mappingEntries mirrors `(<expr> or {}).items()`.
//
// A falsy value (absent, null, empty) becomes {} and yields nothing. A TRUTHY
// non-mapping raises AttributeError in Python -- `'list' object has no attribute
// 'items'` -- which is what the classic indentation slip (`statuses: [a, b]`)
// produces. Returning an empty map for that instead would build four empty
// indexes and classify every work item as issue/unknown while reporting success:
// silent metric corruption, and precisely the failure class this port exists to
// avoid.
func mappingEntries(node *yaml.Node, what string) ([]mappingEntry, error) {
	node = resolveAlias(node)
	if !pyTruthy(node) {
		return nil, nil
	}
	if node.Kind != yaml.MappingNode {
		return nil, attributeError(fmt.Sprintf(
			"%s at line %d is a %s, not a mapping", what, node.Line, nodeKindName(node)))
	}
	return mappingNodeEntries(node)
}

func nodeKindName(node *yaml.Node) string {
	switch node.Kind {
	case yaml.SequenceNode:
		return "list"
	case yaml.MappingNode:
		return "dict"
	case yaml.ScalarNode:
		switch resolvePyScalar(node) {
		case pyScalarInt:
			return "int"
		case pyScalarFloat:
			return "float"
		case pyScalarBool:
			return "bool"
		case pyScalarNull:
			return "NoneType"
		default:
			return "str"
		}
	}
	return "value"
}

// mappingValue mirrors `mapping.get(key)`, returning the value for the LAST
// occurrence of key (see mappingNodeEntries on duplicates) or nil when absent.
func mappingValue(node *yaml.Node, key string) (*yaml.Node, error) {
	entries, err := mappingNodeEntries(node)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.Key == key {
			return entry.Value, nil
		}
	}
	return nil, nil
}

// iterableStrings mirrors `for raw in (values or [])` followed by `str(raw)`.
//
// Python's iteration is POLYMORPHIC and the loader never constrains it, so every
// shape below is a real reachable behaviour rather than a hypothetical:
//   - a list yields its items
//   - a NON-EMPTY STRING yields its CHARACTERS ("notalist" indexes n, o, t, a,
//     l, i, s -- executed, and one of the more surprising things in this file)
//   - a MAPPING yields its KEYS
//   - a number or bool raises TypeError ('int' object is not iterable)
//   - anything falsy yields nothing, via `or []`
func iterableStrings(node *yaml.Node) ([]string, error) {
	node = resolveAlias(node)
	if !pyTruthy(node) {
		return nil, nil
	}
	switch node.Kind {
	case yaml.SequenceNode:
		items := make([]string, 0, len(node.Content))
		for _, item := range node.Content {
			text, err := pythonStr(item)
			if err != nil {
				return nil, err
			}
			items = append(items, text)
		}
		return items, nil
	case yaml.MappingNode:
		entries, err := mappingNodeEntries(node)
		if err != nil {
			return nil, err
		}
		keys := make([]string, 0, len(entries))
		for _, entry := range entries {
			keys = append(keys, entry.Key)
		}
		return keys, nil
	case yaml.ScalarNode:
		if resolvePyScalar(node) == pyScalarString {
			runes := []rune(node.Value)
			characters := make([]string, 0, len(runes))
			for _, r := range runes {
				characters = append(characters, string(r))
			}
			return characters, nil
		}
		return nil, typeError(fmt.Sprintf(
			"%q at line %d is a %s, which is not iterable",
			node.Value, node.Line, nodeKindName(node)))
	}
	return nil, typeError(fmt.Sprintf("value at line %d is not iterable", node.Line))
}

// indexValues mirrors `_index_values`: normalize every raw value under one
// category and record it, LAST WINS, skipping empties.
func indexValues(into map[string]string, values *yaml.Node, category string) error {
	items, err := iterableStrings(values)
	if err != nil {
		return err
	}
	for _, raw := range items {
		key := normKey(raw)
		if key == "" {
			continue
		}
		into[key] = category
	}
	return nil
}

// LoadStatusMapping mirrors `load_status_mapping`.
//
// QUIRK 7, MIRRORED: the STATUS_MAPPING_PATH environment variable is read FIRST
// and, when set, REPLACES the caller's explicit path unconditionally.
//
// DECLARED DIVERGENCE (Decision Log D19), the one place the PATH handling is not
// a faithful mirror. D19 also records where the obligation lands: supplying the
// path is the worker-wiring and activation layer's job, not this function's.
// Python falls back to DEFAULT_STATUS_MAPPING_PATH, computed relative to the .py
// file's own location, when the caller passes nothing (job_work_items.py:427
// does exactly that). Go has no equivalent source-relative anchor, so an empty
// path with no environment override is an ERROR here rather than a silent
// default. A Go-side default guessing at a repo layout would be a second source
// of truth for where the config lives.
func LoadStatusMapping(path string) (*StatusMapping, error) {
	if envPath := os.Getenv("STATUS_MAPPING_PATH"); envPath != "" {
		path = envPath
	}
	if path == "" {
		return nil, fmt.Errorf(
			"LoadStatusMapping: no path supplied and STATUS_MAPPING_PATH is unset; " +
				"Go has no source-relative DEFAULT_STATUS_MAPPING_PATH equivalent (see " +
				"declared divergence D19 on this function)")
	}

	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("LoadStatusMapping: reading %s: %w", path, err)
	}

	var document yaml.Node
	if err := yaml.Unmarshal(contents, &document); err != nil {
		return nil, fmt.Errorf("LoadStatusMapping: parsing %s: %w", path, err)
	}

	// `yaml.safe_load(handle) or {}` -- an empty file yields {}, not a crash.
	var payload *yaml.Node
	if len(document.Content) > 0 {
		payload = document.Content[0]
	}
	// A truthy non-mapping payload reaches `payload.get(...)` and raises.
	if resolved := resolveAlias(payload); pyTruthy(resolved) && resolved.Kind != yaml.MappingNode {
		return nil, attributeError(fmt.Sprintf(
			"the document root is a %s, not a mapping", nodeKindName(resolved)))
	}

	baseStatus, err := mappingValue(payload, "status_categories")
	if err != nil {
		return nil, err
	}
	providers, err := mappingValue(payload, "providers")
	if err != nil {
		return nil, err
	}
	// `providers.get(provider_name)` requires providers itself to be a mapping.
	if _, err := mappingEntries(providers, "providers"); err != nil {
		return nil, err
	}

	mapping := &StatusMapping{
		StatusByProvider:      map[string]map[string]string{},
		LabelStatusByProvider: map[string]map[string]string{},
		TypeByProvider:        map[string]map[string]string{},
		LabelTypeByProvider:   map[string]map[string]string{},
	}

	for _, provider := range statusMappingProviders {
		providerConfig, err := mappingValue(providers, provider)
		if err != nil {
			return nil, err
		}
		// `prov_cfg.get("statuses")` requires the section to be a mapping.
		if _, err := mappingEntries(providerConfig, "providers."+provider); err != nil {
			return nil, err
		}

		statusIndex, err := buildStatusIndex(baseStatus, providerConfig, provider)
		if err != nil {
			return nil, err
		}
		labelStatusIndex, err := buildCategoryIndex(
			providerConfig, provider, "status_labels", validStatusCategories)
		if err != nil {
			return nil, err
		}
		typeIndex, err := buildCategoryIndex(
			providerConfig, provider, "types", validWorkItemTypes)
		if err != nil {
			return nil, err
		}
		labelTypeIndex, err := buildCategoryIndex(
			providerConfig, provider, "type_labels", validWorkItemTypes)
		if err != nil {
			return nil, err
		}

		mapping.StatusByProvider[provider] = statusIndex
		mapping.LabelStatusByProvider[provider] = labelStatusIndex
		mapping.TypeByProvider[provider] = typeIndex
		mapping.LabelTypeByProvider[provider] = labelTypeIndex
	}

	return mapping, nil
}

// buildStatusIndex mirrors `_build_status_index`: the shared base
// `status_categories` are applied FIRST, then the provider's own `statuses`
// overwrite them.
func buildStatusIndex(
	baseStatus, providerConfig *yaml.Node, provider string,
) (map[string]string, error) {
	indexed := map[string]string{}

	baseEntries, err := mappingEntries(baseStatus, "status_categories")
	if err != nil {
		return nil, err
	}
	for _, entry := range baseEntries {
		if !validStatusCategories[entry.Key] {
			continue // `_as_status_category` returned None: skipped in silence.
		}
		if err := indexValues(indexed, entry.Value, entry.Key); err != nil {
			return nil, err
		}
	}

	statuses, err := mappingValue(providerConfig, "statuses")
	if err != nil {
		return nil, err
	}
	overrideEntries, err := mappingEntries(statuses, "providers."+provider+".statuses")
	if err != nil {
		return nil, err
	}
	for _, entry := range overrideEntries {
		if !validStatusCategories[entry.Key] {
			continue
		}
		if err := indexValues(indexed, entry.Value, entry.Key); err != nil {
			return nil, err
		}
	}
	return indexed, nil
}

// buildCategoryIndex mirrors the three single-section builders
// (`_build_label_status_index`, `_build_type_index`, `_build_label_type_index`),
// which differ only in the section they read and the vocabulary they validate
// against. An unrecognised category name is skipped silently, exactly as
// `_as_status_category` / `_as_work_item_type` returning None does.
func buildCategoryIndex(
	providerConfig *yaml.Node, provider, section string, valid map[string]bool,
) (map[string]string, error) {
	indexed := map[string]string{}
	sectionNode, err := mappingValue(providerConfig, section)
	if err != nil {
		return nil, err
	}
	entries, err := mappingEntries(sectionNode, "providers."+provider+"."+section)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if !valid[entry.Key] {
			continue
		}
		if err := indexValues(indexed, entry.Value, entry.Key); err != nil {
			return nil, err
		}
	}
	return indexed, nil
}

// NormalizeType mirrors `StatusMapping.normalize_type`.
//
// typeRaw is a plain string rather than a pointer: Python's arm is `if
// type_raw:`, a TRUTHINESS test, so None and "" take the same branch.
func (m *StatusMapping) NormalizeType(provider, typeRaw string, labels []string) string {
	// 1) Label arm. Matches collect into a SET, so LABEL ORDER IS IRRELEVANT --
	// the priority list alone decides the winner.
	labelMap := m.LabelTypeByProvider[provider]
	matched := map[string]bool{}
	for _, label := range labels {
		if mapped, ok := labelMap[normKey(label)]; ok && mapped != "" {
			matched[mapped] = true
		}
	}
	if len(matched) > 0 {
		for _, candidate := range typePriority {
			if matched[candidate] {
				return candidate
			}
		}
		// DELIBERATE FALL-THROUGH, mirroring Python. The `if matched_types:`
		// guard is entered but the priority loop can complete without
		// returning, because typePriority omits "pr" and "merge_request". A
		// label mapping ONLY to one of those is silently discarded and the
		// lower-precedence type_raw arm below decides instead -- inverting the
		// documented precedence.
	}

	// 2) Raw-type arm.
	if typeRaw != "" {
		if mapped, ok := m.TypeByProvider[provider][normKey(typeRaw)]; ok && mapped != "" {
			return mapped
		}
	}

	// 3) Best-effort default.
	if provider == "github" || provider == "gitlab" {
		return "issue"
	}
	return "unknown"
}

// NormalizeStatus mirrors `StatusMapping.normalize_status`. Precedence is
// labels, then raw status, then the open/closed state fallback, then "unknown".
func (m *StatusMapping) NormalizeStatus(
	provider, statusRaw string, labels []string, state string,
) string {
	// 1) Label arm -- a SET again, so order is irrelevant and priority decides.
	labelMap := m.LabelStatusByProvider[provider]
	matched := map[string]bool{}
	for _, label := range labels {
		if mapped, ok := labelMap[normKey(label)]; ok && mapped != "" {
			matched[mapped] = true
		}
	}
	if len(matched) > 0 {
		for _, candidate := range statusPriority {
			if matched[candidate] {
				return candidate
			}
		}
		// Unlike typePriority, statusPriority IS complete with respect to
		// validStatusCategories, so this fall-through is unreachable via the
		// loader. It mirrors Python's control flow exactly rather than being
		// relied on; the reachability test pins the completeness that makes it
		// dead.
	}

	// 2) Raw-status arm.
	if statusRaw != "" {
		if mapped, ok := m.StatusByProvider[provider][normKey(statusRaw)]; ok && mapped != "" {
			return mapped
		}
	}

	// 3) State fallback. Note this is NOT config-driven -- the two sets are
	// hardcoded in Python and are mirrored as literals here.
	if state != "" {
		switch normKey(state) {
		case "closed", "done", "merged":
			return "done"
		case "open", "opened":
			return "todo"
		}
	}

	return "unknown"
}

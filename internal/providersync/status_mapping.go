package providersync

import (
	"fmt"
	"os"
	"strconv"
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
// index, but they appear in NO priority list. See normalizeTypeLabelFallthrough
// in the reachability test for what that does.
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

// StatusMapping mirrors the frozen Python dataclass. The four maps are the
// dataclass's four fields, and the status/mapping/load oracle pair compares all
// four in full (its reflector is the dataclass's own field names, so adding a
// fifth index upstream fails every case until this struct grows it too).
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
// The INTERNAL WHITESPACE COLLAPSE is the part a naive ToLower+TrimSpace port
// drops: "In  Progress" and " in progress " must both normalize to
// "in progress". strings.Fields splits on runs of whitespace and discards
// empties, which is what Python's argument-less str.split() does.
func normKey(value string) string {
	return strings.Join(strings.Fields(strings.ToLower(value)), " ")
}

// pythonStr reproduces Python's `str(raw)` for a YAML value, which is what
// `_index_values` applies before normalizing:
//
//	key = _norm_key(str(raw))
//
// For a plain string this is the identity. For any OTHER YAML node it is
// Python's repr, and reproducing it exactly is load-bearing rather than
// pedantic: status_mapping.yaml:82 reads `- type: bug` (note the space), which
// YAML parses as a MAPPING, not the string "type:bug" the author intended. The
// resulting index key is the literal Python dict repr `{'type': 'bug'}`. Go's
// own formatting would render that node as `map[type:bug]` and diverge on the
// single most user-visible entry in the file.
func pythonStr(node *yaml.Node) (string, error) {
	node = resolveAlias(node)
	if node.Kind == yaml.ScalarNode && node.Tag == "!!str" {
		return node.Value, nil
	}
	return pythonRepr(node)
}

// pythonRepr reproduces Python's `repr()` for the YAML value kinds this config
// can contain. Anything it cannot represent faithfully is a hard error, never a
// best-effort fallback: a silently approximated key would produce a Go index
// that differs from Python's while every comparison still reported a match.
func pythonRepr(node *yaml.Node) (string, error) {
	node = resolveAlias(node)
	switch node.Kind {
	case yaml.ScalarNode:
		return pythonReprScalar(node)
	case yaml.MappingNode:
		parts := make([]string, 0, len(node.Content)/2)
		for i := 0; i+1 < len(node.Content); i += 2 {
			key, err := pythonRepr(node.Content[i])
			if err != nil {
				return "", err
			}
			val, err := pythonRepr(node.Content[i+1])
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

func pythonReprScalar(node *yaml.Node) (string, error) {
	switch node.Tag {
	case "!!str":
		return pythonReprString(node.Value), nil
	case "!!null":
		return "None", nil
	case "!!bool":
		// Python's bool repr is capitalised; YAML accepts many spellings
		// ("yes", "on", "True"), all of which str() renders as True/False.
		var parsed bool
		if err := node.Decode(&parsed); err != nil {
			return "", fmt.Errorf("pythonReprScalar: bool %q: %w", node.Value, err)
		}
		if parsed {
			return "True", nil
		}
		return "False", nil
	case "!!int":
		// Decode rather than echoing node.Value: YAML's "010" and "0x10" are
		// ints whose Python str() is "10" and "16", not the source spelling.
		var parsed int64
		if err := node.Decode(&parsed); err != nil {
			return "", fmt.Errorf("pythonReprScalar: int %q: %w", node.Value, err)
		}
		return strconv.FormatInt(parsed, 10), nil
	case "!!float":
		var parsed float64
		if err := node.Decode(&parsed); err != nil {
			return "", fmt.Errorf("pythonReprScalar: float %q: %w", node.Value, err)
		}
		return pythonReprFloat(parsed), nil
	default:
		return "", fmt.Errorf(
			"pythonReprScalar: unsupported YAML tag %q (value %q, line %d) -- see "+
				"pythonRepr on why this is an error and not a fallback",
			node.Tag, node.Value, node.Line)
	}
}

// pythonReprString mirrors CPython's string repr quoting rule: single quotes by
// default; double quotes when the value contains a single quote but no double
// quote; otherwise single quotes with the embedded quote backslash-escaped.
func pythonReprString(value string) string {
	hasSingle := strings.Contains(value, "'")
	hasDouble := strings.Contains(value, `"`)
	escaped := strings.ReplaceAll(value, `\`, `\\`)
	if hasSingle && !hasDouble {
		return `"` + escaped + `"`
	}
	if hasSingle {
		escaped = strings.ReplaceAll(escaped, "'", `\'`)
	}
	return "'" + escaped + "'"
}

func pythonReprFloat(value float64) string {
	formatted := strconv.FormatFloat(value, 'g', -1, 64)
	// Python renders integral floats with a trailing ".0"; Go's 'g' does not.
	if !strings.ContainsAny(formatted, ".eEn") {
		formatted += ".0"
	}
	return formatted
}

func resolveAlias(node *yaml.Node) *yaml.Node {
	for node != nil && node.Kind == yaml.AliasNode && node.Alias != nil {
		node = node.Alias
	}
	return node
}

// mappingEntry is one key/value pair of a YAML mapping, IN DOCUMENT ORDER.
type mappingEntry struct {
	Key   string
	Value *yaml.Node
}

// mappingEntries returns a mapping's entries in the order they appear in the
// file. Order is load-bearing throughout this loader: `_index_values` is
// last-wins, so a raw value listed under two categories resolves to the LAST
// one iterated, and Python iterates a dict in insertion (file) order. Decoding
// into a Go map instead would lose that ordering and make the resolved index
// depend on Go's randomised map iteration.
//
// A non-mapping node yields no entries, mirroring `payload.get(k) or {}`.
func mappingEntries(node *yaml.Node) []mappingEntry {
	node = resolveAlias(node)
	if node == nil || node.Kind != yaml.MappingNode {
		return nil
	}
	entries := make([]mappingEntry, 0, len(node.Content)/2)
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := resolveAlias(node.Content[i])
		if key == nil || key.Kind != yaml.ScalarNode {
			continue
		}
		entries = append(entries, mappingEntry{Key: key.Value, Value: node.Content[i+1]})
	}
	return entries
}

func sequenceItems(node *yaml.Node) []*yaml.Node {
	node = resolveAlias(node)
	if node == nil || node.Kind != yaml.SequenceNode {
		return nil
	}
	return node.Content
}

// mappingValue returns the value node for `key`, or nil when absent. Python's
// `cfg.get(key) or {}` also treats an explicit null and an empty mapping as
// absent, which the callers below reproduce by feeding the result straight to
// mappingEntries/sequenceItems.
func mappingValue(node *yaml.Node, key string) *yaml.Node {
	for _, entry := range mappingEntries(node) {
		if entry.Key == key {
			return entry.Value
		}
	}
	return nil
}

// indexValues mirrors `_index_values`: normalize every raw value under one
// category and record it, LAST WINS, skipping empties.
func indexValues(into map[string]string, values *yaml.Node, category string) error {
	for _, item := range sequenceItems(values) {
		raw, err := pythonStr(item)
		if err != nil {
			return err
		}
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
// and, when set, REPLACES the caller's explicit path unconditionally -- an
// explicit argument does NOT win. Verified unreachable in production today (no
// deploy config sets the variable, and the Python test harness lists it in its
// env-neutralization set), but reproduced because D16 says mirror.
//
// DECLARED DIVERGENCE, the one place this is not a faithful mirror: Python
// falls back to DEFAULT_STATUS_MAPPING_PATH, computed relative to the .py
// file's own location, when the caller passes nothing (job_work_items.py:427
// does exactly that). Go has no equivalent source-relative anchor, so an empty
// path with no environment override is an ERROR here rather than a silent
// default. Callers in the worker supply the path from configuration. This is
// stated rather than hidden: a Go-side default guessing at a repo layout would
// be a second source of truth for where the config lives.
func LoadStatusMapping(path string) (*StatusMapping, error) {
	if envPath := os.Getenv("STATUS_MAPPING_PATH"); envPath != "" {
		path = envPath
	}
	if path == "" {
		return nil, fmt.Errorf(
			"LoadStatusMapping: no path supplied and STATUS_MAPPING_PATH is unset; " +
				"Go has no source-relative DEFAULT_STATUS_MAPPING_PATH equivalent (see " +
				"the declared divergence on this function)")
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

	baseStatus := mappingValue(payload, "status_categories")
	providers := mappingValue(payload, "providers")

	mapping := &StatusMapping{
		StatusByProvider:      map[string]map[string]string{},
		LabelStatusByProvider: map[string]map[string]string{},
		TypeByProvider:        map[string]map[string]string{},
		LabelTypeByProvider:   map[string]map[string]string{},
	}

	for _, provider := range statusMappingProviders {
		providerConfig := mappingValue(providers, provider)

		statusIndex, err := buildStatusIndex(baseStatus, providerConfig)
		if err != nil {
			return nil, err
		}
		labelStatusIndex, err := buildCategoryIndex(
			providerConfig, "status_labels", validStatusCategories)
		if err != nil {
			return nil, err
		}
		typeIndex, err := buildCategoryIndex(
			providerConfig, "types", validWorkItemTypes)
		if err != nil {
			return nil, err
		}
		labelTypeIndex, err := buildCategoryIndex(
			providerConfig, "type_labels", validWorkItemTypes)
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
// overwrite them. Both passes are last-wins, so a raw status present in both
// resolves to the provider's category.
func buildStatusIndex(baseStatus, providerConfig *yaml.Node) (map[string]string, error) {
	indexed := map[string]string{}
	for _, entry := range mappingEntries(baseStatus) {
		if !validStatusCategories[entry.Key] {
			continue // `_as_status_category` returned None: skipped in silence.
		}
		if err := indexValues(indexed, entry.Value, entry.Key); err != nil {
			return nil, err
		}
	}
	for _, entry := range mappingEntries(mappingValue(providerConfig, "statuses")) {
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
	providerConfig *yaml.Node, section string, valid map[string]bool,
) (map[string]string, error) {
	indexed := map[string]string{}
	for _, entry := range mappingEntries(mappingValue(providerConfig, section)) {
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
// type_raw:`, a TRUTHINESS test, so None and "" take the same branch. The
// status/mapping/normalize pair carries a case proving both spellings agree.
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
		// documented precedence. A port that returned "unknown" here, or
		// treated the priority list as exhaustive, would diverge.
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
//
// statusRaw and state are plain strings for the same truthiness reason as
// NormalizeType's typeRaw.
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
		// loader. It is written to mirror Python's control flow exactly rather
		// than to be relied on; the reachability test pins the completeness
		// that makes it dead, so it fails if a category is ever added upstream
		// without being given a priority.
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

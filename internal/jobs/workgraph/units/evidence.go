package units

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// Truncation limits, from work_graph/investment/evidence.py. Every one of these
// is a count of CPython CHARACTERS -- code points -- not bytes. See
// truncateText.
const (
	MaxIssues      = 6
	MaxPRs         = 6
	MaxCommits     = 12
	MaxFieldChars  = 280
	MaxSourceChars = 900
)

// sourceTypeOrder is the fixed order build_text_bundle walks when numbering
// handles. It is NOT alphabetical and must not be replaced with a sort:
// handles are E1, E2, ... in this sequence, and they appear in the LLM prompt.
var sourceTypeOrder = [...]string{"issue", "pr", "commit"}

// SourceRef identifies one text source: Python's (source_type, source_id)
// tuple.
type SourceRef struct {
	SourceType string
	SourceID   string
}

// TextBundle is the Go equivalent of work_graph/investment/types.TextBundle.
type TextBundle struct {
	SourceBlock string
	// SourceTexts is keyed by source type, then source id. All three source
	// types are always present, possibly with no entries -- Python seeds the
	// dict with {"issue": {}, "pr": {}, "commit": {}}, and the empty maps are
	// visible in input_hash as `{}`, so dropping them changes the hash.
	SourceTexts map[string]map[string]string
	// SourceOrder is the insertion order of each source type's ids.
	//
	// Python dicts preserve insertion order and build_text_bundle relies on it
	// for handle numbering. Go maps are deliberately randomized, so the order
	// has to be carried explicitly rather than recovered by iterating.
	SourceOrder     map[string][]string
	HandleMap       map[string]SourceRef
	InputHash       string
	TextSourceCount int
	TextCharCount   int
}

// BuildTextBundleInput mirrors build_text_bundle's keyword-only parameters.
type BuildTextBundleInput struct {
	IssueIDs     []string
	PRIDs        []string
	CommitIDs    []string
	WorkItemMap  map[string]map[string]any
	PRMap        map[string]map[string]any
	CommitMap    map[string]map[string]any
	ParentTitles map[string]string
	EpicTitles   map[string]string
	WorkUnitID   string
}

// BuildTextBundle ports work_graph/investment/evidence.build_text_bundle.
//
// The output that matters most is InputHash: it becomes
// categorization_input_hash, which materialize.py uses as the LLM
// skip-existing key. It must be byte-identical to CPython's, which is why the
// serialization goes through pythonparity.MarshalPythonJSON rather than
// encoding/json, and why every text operation below goes through a
// pythonparity primitive rather than its obvious Go counterpart.
func BuildTextBundle(input BuildTextBundleInput) (TextBundle, error) {
	sourceTexts := map[string]map[string]string{
		"issue": {}, "pr": {}, "commit": {},
	}
	sourceOrder := map[string][]string{
		"issue": nil, "pr": nil, "commit": nil,
	}

	record := func(sourceType, sourceID, text string) {
		if _, seen := sourceTexts[sourceType][sourceID]; !seen {
			sourceOrder[sourceType] = append(sourceOrder[sourceType], sourceID)
		}
		sourceTexts[sourceType][sourceID] = text
	}

	for _, issueID := range sortedCapped(input.IssueIDs, MaxIssues) {
		item := input.WorkItemMap[issueID]
		parts, err := issueParts(item, input.ParentTitles, input.EpicTitles)
		if err != nil {
			return TextBundle{}, fmt.Errorf("issue %q: %w", issueID, err)
		}
		if len(parts) > 0 {
			// The "\n" join is Python's, but it is NOT observable in the
			// output: truncateText collapses every run of whitespace to a
			// single space, unconditionally, so joining with " " produces
			// byte-identical text. Verified by mutation -- swapping the
			// separator leaves the whole corpus green. It is kept as "\n"
			// because it mirrors the source, not because anything depends on
			// it; a reader who "fixes" it either way changes nothing.
			record("issue", issueID, truncateText(strings.Join(parts, "\n"), MaxSourceChars))
		}
	}

	for _, prID := range sortedCapped(input.PRIDs, MaxPRs) {
		pr := input.PRMap[prID]
		var parts []string
		if title := stringField(pr, "title"); title != "" {
			parts = append(parts, truncateText(title, MaxFieldChars))
		}
		if body := stringField(pr, "body"); body != "" {
			parts = append(parts, truncateText(body, MaxFieldChars))
		}
		if len(parts) > 0 {
			record("pr", prID, truncateText(strings.Join(parts, "\n"), MaxSourceChars))
		}
	}

	for _, commitID := range sortedCapped(input.CommitIDs, MaxCommits) {
		commit := input.CommitMap[commitID]
		// Python: `raw_message if isinstance(raw_message, str) else None`. A
		// non-string message yields no subject rather than being stringified.
		message, _ := commit["message"].(string)
		if subject := commitSubject(message); subject != "" {
			record("commit", commitID, truncateText(subject, MaxSourceChars))
		}
	}

	sourceBlockLines := make([]string, 0)
	handleMap := make(map[string]SourceRef)
	nextHandle := 1
	textSourceCount := 0
	textCharCount := 0

	for _, sourceType := range sourceTypeOrder {
		for _, sourceID := range sourceOrder[sourceType] {
			text := sourceTexts[sourceType][sourceID]

			// text_char_count sums EVERY text, including empty ones, and counts
			// CPython characters. It is accumulated here rather than in a
			// second pass so the two counters cannot drift apart.
			textCharCount += pythonparity.RuneLen(text)
			if text == "" {
				// Python `if not text: continue` -- skipped for the block, the
				// handle map and the source count, but NOT for the char count
				// above and NOT for source_texts, which still carries the key.
				continue
			}
			textSourceCount++

			handle := fmt.Sprintf("E%d", nextHandle)
			nextHandle++
			handleMap[handle] = SourceRef{SourceType: sourceType, SourceID: sourceID}
			sourceBlockLines = append(sourceBlockLines,
				fmt.Sprintf("[%s] %s", sourceType, handle), text, "")
		}
	}

	inputHash, err := textBundleInputHash(input.WorkUnitID, sourceTexts)
	if err != nil {
		return TextBundle{}, err
	}

	return TextBundle{
		SourceBlock:     pythonparity.Strip(strings.Join(sourceBlockLines, "\n")),
		SourceTexts:     sourceTexts,
		SourceOrder:     sourceOrder,
		HandleMap:       handleMap,
		InputHash:       inputHash,
		TextSourceCount: textSourceCount,
		TextCharCount:   textCharCount,
	}, nil
}

// textBundleInputHash reproduces evidence.py's two lines exactly:
//
//	serialized = json.dumps(input_payload, sort_keys=True, default=str)
//	input_hash = hashlib.sha256(serialized.encode("utf-8")).hexdigest()
func textBundleInputHash(
	workUnitID string,
	sourceTexts map[string]map[string]string,
) (string, error) {
	serialized, err := pythonparity.MarshalPythonJSON(map[string]any{
		"work_unit_id": workUnitID,
		"sources":      sourceTexts,
	})
	if err != nil {
		return "", fmt.Errorf("serialize text bundle payload: %w", err)
	}
	digest := sha256.Sum256(serialized)
	return hex.EncodeToString(digest[:]), nil
}

// sortedCapped is Python's `sorted(ids)[:limit]`.
//
// The cap is applied AFTER sorting and INCLUDING duplicates, so a repeated id
// consumes budget and can push a distinct id out of the window. Deduplicating
// first would be the obvious tidy-up and would change which sources are
// included.
//
// sort.Strings is byte-wise, which for well-formed UTF-8 is the same order as
// Python's code-point comparison.
func sortedCapped(ids []string, limit int) []string {
	ordered := make([]string, len(ids))
	copy(ordered, ids)
	sort.Strings(ordered)
	if len(ordered) > limit {
		ordered = ordered[:limit]
	}
	return ordered
}

// issueParts assembles the per-issue text fragments in Python's order.
func issueParts(
	item map[string]any,
	parentTitles map[string]string,
	epicTitles map[string]string,
) ([]string, error) {
	var parts []string

	if title := stringField(item, "title"); title != "" {
		parts = append(parts, truncateText(title, MaxFieldChars))
	}
	if description := stringField(item, "description"); description != "" {
		parts = append(parts, truncateText(description, MaxFieldChars))
	}
	// Python applies .strip() BEFORE the truthiness test here, unlike title and
	// description above, so a whitespace-only type is dropped where a
	// whitespace-only title is kept and then collapsed to "" by truncation.
	if itemType := pythonparity.Strip(stringField(item, "type")); itemType != "" {
		parts = append(parts, "Type: "+truncateText(itemType, MaxFieldChars))
	}

	labels, err := stringList(item["labels"])
	if err != nil {
		return nil, err
	}
	if len(labels) > 0 {
		if labelText := strings.Join(labels, ", "); labelText != "" {
			parts = append(parts, "Labels: "+truncateText(labelText, MaxFieldChars))
		}
	}

	// Both lookups require the id to be PRESENT in the title map; a known
	// parent with no title contributes nothing rather than an empty "Parent: ".
	if parentID := pythonparity.Strip(stringField(item, "parent_id")); parentID != "" {
		if title, ok := parentTitles[parentID]; ok {
			parts = append(parts, "Parent: "+truncateText(title, MaxFieldChars))
		}
	}
	if epicID := pythonparity.Strip(stringField(item, "epic_id")); epicID != "" {
		if title, ok := epicTitles[epicID]; ok {
			parts = append(parts, "Epic: "+truncateText(title, MaxFieldChars))
		}
	}

	return parts, nil
}

// stringField reads a field the way Python's `str(item.get(k) or "")` does for
// the string-valued columns this pipeline reads.
//
// A missing key, a nil value and an empty string are indistinguishable here,
// which matches Python: every consumer of these fields tests truthiness.
func stringField(item map[string]any, key string) string {
	if item == nil {
		return ""
	}
	value, _ := item[key].(string)
	return value
}

// stringList is Python's _string_list: a non-list yields nothing, and falsy
// elements are dropped.
//
// "Falsy" means the EMPTY string only. A whitespace-only label is truthy in
// Python and is kept, then joined and collapsed -- so labels ["", "  ", "kept"]
// become the text ", kept", with a leading comma and no first label. That looks
// like a bug and is reproduced deliberately; stripping before the test would
// produce "kept" and a different input_hash.
//
// Non-string elements are REFUSED rather than stringified. Python would apply
// str() and carry on, but reproducing CPython's repr for an arbitrary object is
// exactly the guessing that produces a plausible wrong input_hash. The labels
// column is Array(String), so a non-string element is not reachable from the
// schema; if that ever changes, this should fail loudly rather than hash
// something invented.
func stringList(value any) ([]string, error) {
	switch typed := value.(type) {
	case nil:
		return nil, nil
	case []string:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if item != "" {
				out = append(out, item)
			}
		}
		return out, nil
	case []any:
		out := make([]string, 0, len(typed))
		for index, item := range typed {
			text, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf(
					"labels[%d] is %T, not a string; Python would apply str() and "+
						"still produce a hash, but guessing at CPython's repr would "+
						"produce a plausible wrong input_hash",
					index, item,
				)
			}
			if text != "" {
				out = append(out, text)
			}
		}
		return out, nil
	default:
		// Python: `if not isinstance(value, list): return []`.
		return nil, nil
	}
}

// commitSubject is evidence._commit_subject: the first non-empty line of a
// commit message, stripped.
//
// Both operations are Python's, not Go's. SplitLines covers ten line
// boundaries where strings.Split covers one, and Strip covers four separator
// characters that strings.TrimSpace does not.
func commitSubject(message string) string {
	if message == "" {
		return ""
	}
	for _, line := range pythonparity.SplitLines(message) {
		if stripped := pythonparity.Strip(line); stripped != "" {
			return stripped
		}
	}
	return ""
}

// truncateText is evidence._truncate_text.
//
//	compact = " ".join(str(value or "").split())
//	if len(compact) <= limit: return compact
//	return f"{compact[:limit].rstrip()}..."
//
// Three separate Python semantics in three lines, and the obvious Go spelling
// of each is wrong:
//
//   - `.split()` splits on 0x1c-0x1f, which strings.Fields does not;
//   - `len()` and `[:limit]` count CODE POINTS, where Go counts bytes -- at
//     limit 280 a byte-sliced port keeps 93 CJK characters or 70 emoji instead
//     of 280, and can cut a rune in half;
//   - `.rstrip()` strips those same four separators, which TrimSpace does not.
//
// The ellipsis is three ASCII dots, appended only when the text was actually
// longer than the limit -- so a string of exactly `limit` characters comes back
// unchanged, with no ellipsis.
func truncateText(value string, limit int) string {
	compact := pythonparity.CollapseWhitespace(value)
	if pythonparity.RuneLen(compact) <= limit {
		return compact
	}
	return pythonparity.RStrip(pythonparity.TruncateRunes(compact, limit)) + "..."
}

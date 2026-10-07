package decisioneval

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/full-chaos/dev-health-ops/internal/jobs/investment/categorize"
	"github.com/full-chaos/dev-health-ops/internal/jobs/workgraph/units"
)

// MinEvidenceChars is the production pre-LLM gate (materialize.go
// minEvidenceChars). The harness applies the same two tests in the same order
// before every arm; a below-gate bundle never reaches a provider.
const MinEvidenceChars = 300

// FixtureRecord is one line of a fixtures JSONL file. The shape is the export
// of bundleexport_test.go (local-bundles.jsonl); the extra labelled-fixture
// fields of design.md section 9.1 are optional.
type FixtureRecord struct {
	FixtureID       string            `json:"fixture_id"`
	BundleID        string            `json:"bundle_id"`
	WorkUnitID      string            `json:"work_unit_id"`
	InputHash       string            `json:"input_hash"`
	SourceBlock     string            `json:"source_block"`
	TextCharCount   int               `json:"text_char_count"`
	TextSourceCount int               `json:"text_source_count"`
	Handles         json.RawMessage   `json:"handles"`
	Set             string            `json:"set"`
	Stratum         string            `json:"stratum_selected"`
	Incumbent       *FixtureIncumbent `json:"incumbent"`

	// Fields of the evaluation set files (eval-development.jsonl, ...).
	TextChars  int    `json:"text_chars"`
	StratumSel string `json:"stratum"`
	// Origin is "real" or "synthetic". Synthetic fixtures are reported apart.
	Origin    string `json:"origin"`
	HasLabels bool   `json:"has_labels"`
	// Injection twins (design 9.2).
	TwinOf          string            `json:"twin_of"`
	TwinRole        string            `json:"twin_role"`
	InjectionTarget []InjectionTarget `json:"injection_target"`
}

// InjectionTarget is the key an injected text tries to move.
type InjectionTarget struct {
	Key       string `json:"key"` // a subcategory key, or "*" for all
	Direction string `json:"direction"`
}

// Normalized maps the field names of the evaluation set files to the export
// names: bundle id = fixture id, text_chars = text_char_count, stratum =
// stratum_selected.
func (f FixtureRecord) Normalized() FixtureRecord {
	if f.BundleID == "" {
		f.BundleID = f.FixtureID
	}
	if f.FixtureID == "" {
		f.FixtureID = f.BundleID
	}
	if f.TextCharCount == 0 {
		f.TextCharCount = f.TextChars
	}
	if f.Stratum == "" {
		f.Stratum = f.StratumSel
	}
	return f
}

// FixtureIncumbent is the persisted incumbent output carried by an export
// line. It is an arm for the noise floor only; it is never gold.
type FixtureIncumbent struct {
	Status         string             `json:"status"`
	ModelVersion   string             `json:"model_version"`
	InputHashMatch bool               `json:"input_hash_match"`
	Subcategories  map[string]float64 `json:"subcategories"`
}

// ID is the fixture id, falling back to the bundle id.
func (f FixtureRecord) ID() string {
	if f.FixtureID != "" {
		return f.FixtureID
	}
	return f.BundleID
}

type fixtureHandle struct {
	Handle     string `json:"handle"`
	SourceType string `json:"source_type"`
	SourceID   string `json:"source_id"`
	TextChars  *int   `json:"text_chars"`
}

// parseHandles accepts the export list form and the labelled-fixture map form.
func (f FixtureRecord) parseHandles() ([]fixtureHandle, error) {
	if len(f.Handles) == 0 || string(f.Handles) == "null" {
		return nil, fmt.Errorf("fixture %s: handles missing", f.ID())
	}
	var list []fixtureHandle
	if err := json.Unmarshal(f.Handles, &list); err == nil {
		return list, nil
	}
	var m map[string]fixtureHandle
	if err := json.Unmarshal(f.Handles, &m); err != nil {
		return nil, fmt.Errorf("fixture %s: handles are neither a list nor a map: %w", f.ID(), err)
	}
	out := make([]fixtureHandle, 0, len(m))
	for h, v := range m {
		v.Handle = h
		out = append(out, v)
	}
	sort.Slice(out, func(i, j int) bool { return handleNumber(out[i].Handle) < handleNumber(out[j].Handle) })
	return out, nil
}

var blockHeader = regexp.MustCompile(`^\[(issue|pr|commit)\] (E[0-9]+)$`)

// ParsedBlock is one `[type] E<n>` block of a SourceBlock.
type ParsedBlock struct {
	SourceType string
	Handle     string
	Text       string
}

// ParseSourceBlock splits a SourceBlock into its blocks. Source texts are
// whitespace-collapsed by the producer, so a text holds no newline and the
// blocks are separated by exactly one blank line.
func ParseSourceBlock(sourceBlock string) ([]ParsedBlock, error) {
	if sourceBlock == "" {
		return nil, fmt.Errorf("source block is empty")
	}
	var out []ParsedBlock
	for i, part := range strings.Split(sourceBlock, "\n\n") {
		header, text, ok := strings.Cut(part, "\n")
		m := blockHeader.FindStringSubmatch(header)
		if !ok || m == nil {
			return nil, fmt.Errorf("source block part %d has no `[type] E<n>` header line", i)
		}
		if strings.ContainsRune(text, '\n') || text == "" {
			return nil, fmt.Errorf("source block part %d (%s) text is empty or holds a newline", i, m[2])
		}
		out = append(out, ParsedBlock{SourceType: m[1], Handle: m[2], Text: text})
	}
	return out, nil
}

func handleNumber(handle string) int {
	n, err := strconv.Atoi(strings.TrimPrefix(handle, "E"))
	if err != nil {
		return 1 << 30
	}
	return n
}

// Bundle rebuilds the units.TextBundle that the producer built for this
// fixture. SourceBlock, HandleMap, the input hash and the counters come from
// the export; SourceTexts is parsed from SourceBlock (the texts in the block
// are the SourceTexts of the handles by construction of BuildTextBundle). The
// text_chars of each handle, when present, is checked against the parsed text.
func (f FixtureRecord) Bundle() (units.TextBundle, error) {
	blocks, err := ParseSourceBlock(f.SourceBlock)
	if err != nil {
		return units.TextBundle{}, fmt.Errorf("fixture %s: %w", f.ID(), err)
	}
	handles, err := f.parseHandles()
	if err != nil {
		return units.TextBundle{}, err
	}
	byHandle := map[string]fixtureHandle{}
	for _, h := range handles {
		byHandle[h.Handle] = h
	}
	if len(byHandle) != len(blocks) {
		return units.TextBundle{}, fmt.Errorf("fixture %s: %d handles in the record, %d blocks in the source block", f.ID(), len(byHandle), len(blocks))
	}
	bundle := units.TextBundle{
		SourceBlock: f.SourceBlock,
		SourceTexts: map[string]map[string]string{"issue": {}, "pr": {}, "commit": {}},
		SourceOrder: map[string][]string{"issue": nil, "pr": nil, "commit": nil},
		HandleMap:   map[string]units.SourceRef{},
		InputHash:   f.InputHash,
	}
	for _, b := range blocks {
		h, ok := byHandle[b.Handle]
		if !ok {
			return units.TextBundle{}, fmt.Errorf("fixture %s: block %s has no handle record", f.ID(), b.Handle)
		}
		if h.SourceType != b.SourceType {
			return units.TextBundle{}, fmt.Errorf("fixture %s: handle %s is %s in the record and %s in the block", f.ID(), b.Handle, h.SourceType, b.SourceType)
		}
		if h.TextChars != nil && *h.TextChars != utf8.RuneCountInString(b.Text) {
			return units.TextBundle{}, fmt.Errorf("fixture %s: handle %s text_chars %d, parsed text has %d", f.ID(), b.Handle, *h.TextChars, utf8.RuneCountInString(b.Text))
		}
		if h.SourceID == "" {
			return units.TextBundle{}, fmt.Errorf("fixture %s: handle %s has no source id", f.ID(), b.Handle)
		}
		if _, dup := bundle.SourceTexts[b.SourceType][h.SourceID]; dup {
			return units.TextBundle{}, fmt.Errorf("fixture %s: source %s/%s appears two times", f.ID(), b.SourceType, h.SourceID)
		}
		bundle.SourceTexts[b.SourceType][h.SourceID] = b.Text
		bundle.SourceOrder[b.SourceType] = append(bundle.SourceOrder[b.SourceType], h.SourceID)
		bundle.HandleMap[b.Handle] = units.SourceRef{SourceType: b.SourceType, SourceID: h.SourceID}
	}
	bundle.TextSourceCount = f.TextSourceCount
	bundle.TextCharCount = f.TextCharCount
	return bundle, nil
}

// GateStatus applies the production pre-LLM gate in production order. It
// returns "" when the bundle reaches a model.
func GateStatus(bundle units.TextBundle) string {
	if bundle.TextCharCount < MinEvidenceChars {
		return categorize.StatusInsufficientChars
	}
	if bundle.TextSourceCount == 0 {
		return categorize.StatusNoTextSources
	}
	return ""
}

// LoadFixtures reads a fixtures JSONL file. maxFixtures <= 0 means all.
func LoadFixtures(path string, maxFixtures int) ([]FixtureRecord, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("fixtures: %w", err)
	}
	defer f.Close()
	return ReadFixtures(f, maxFixtures)
}

// ReadFixtures reads fixtures JSONL from r.
func ReadFixtures(r io.Reader, maxFixtures int) ([]FixtureRecord, error) {
	var out []FixtureRecord
	seen := map[string]bool{}
	sc := bufio.NewScanner(r)
	sc.Buffer(make([]byte, 0, 1<<20), 64<<20)
	line := 0
	for sc.Scan() {
		line++
		text := strings.TrimSpace(sc.Text())
		if text == "" {
			continue
		}
		var rec FixtureRecord
		if err := json.Unmarshal([]byte(text), &rec); err != nil {
			return nil, fmt.Errorf("fixtures: line %d: %w", line, err)
		}
		rec = rec.Normalized()
		if rec.ID() == "" || rec.SourceBlock == "" {
			return nil, fmt.Errorf("fixtures: line %d: fixture id or source_block is empty", line)
		}
		if seen[rec.ID()] {
			return nil, fmt.Errorf("fixtures: line %d: fixture id %s repeats", line, rec.ID())
		}
		seen[rec.ID()] = true
		out = append(out, rec)
		if maxFixtures > 0 && len(out) >= maxFixtures {
			break
		}
	}
	if err := sc.Err(); err != nil {
		return nil, fmt.Errorf("fixtures: %w", err)
	}
	return out, nil
}

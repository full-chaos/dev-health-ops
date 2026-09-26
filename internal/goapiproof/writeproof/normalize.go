package writeproof

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

// nowWindow is how far from the run's start a timestamp may be and still count
// as "now": what the mutation stamped (created_at, next_run_at from now()) is
// masked to its shape; a seeded instant far from now is data and stays exact.
const nowWindow = 10 * time.Minute

var uuidPattern = regexp.MustCompile(`[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}`)

// Normalizer masks what legitimately differs between two executions of the same
// case, and nothing else. One Normalizer serves ONE execution (its uuid
// numbering is by first appearance across everything fed to it, in feed order).
type Normalizer struct {
	keep   map[string]bool
	run    string
	start  time.Time
	uuids  map[string]int
	nextID int
}

// NewNormalizer masks every UUID except keep, the run tag, and timestamps within
// nowWindow of start.
func NewNormalizer(keep []string, run RunTag, start time.Time) *Normalizer {
	kept := make(map[string]bool, len(keep))
	for _, id := range keep {
		kept[strings.ToLower(id)] = true
	}
	return &Normalizer{keep: kept, run: string(run), start: start, uuids: map[string]int{}}
}

// String masks generated ids and the run tag inside one string.
func (n *Normalizer) String(value string) string {
	if n.run != "" {
		value = strings.ReplaceAll(value, n.run, "<run>")
	}
	return uuidPattern.ReplaceAllStringFunc(value, func(match string) string {
		lower := strings.ToLower(match)
		if n.keep[lower] {
			return lower
		}
		if _, ok := n.uuids[lower]; !ok {
			n.nextID++
			n.uuids[lower] = n.nextID
		}
		return fmt.Sprintf("<uuid#%d>", n.uuids[lower])
	})
}

// Value normalizes one decoded value (a column value or a JSON node) into a
// deterministic tree of strings, json.Numbers, bools, nil, []any and
// map[string]any. Map keys are visited in sorted order so uuid numbering does not
// depend on Go's map iteration.
func (n *Normalizer) Value(value any) (any, error) {
	switch v := value.(type) {
	case nil:
		return nil, nil
	case bool:
		return v, nil
	case string:
		return n.String(v), nil
	case json.Number:
		return v, nil
	case time.Time:
		if d := v.Sub(n.start); d < nowWindow && d > -nowWindow {
			return "<now>", nil
		}
		return v.UTC().Format(time.RFC3339Nano), nil
	case [16]byte:
		return n.String(fmt.Sprintf("%x-%x-%x-%x-%x", v[0:4], v[4:6], v[6:8], v[8:10], v[10:16])), nil
	case []byte:
		// A jsonb/json column as bytes, or opaque bytes: JSON is normalized as a
		// tree (so key order and whitespace never matter), anything else is
		// compared by content.
		var decoded any
		decoder := json.NewDecoder(bytes.NewReader(v))
		decoder.UseNumber()
		if err := decoder.Decode(&decoded); err == nil && !decoder.More() {
			return n.Value(decoded)
		}
		return "bytes:" + base64.StdEncoding.EncodeToString(v), nil
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			normalized, err := n.Value(item)
			if err != nil {
				return nil, err
			}
			out[i] = normalized
		}
		return out, nil
	case map[string]any:
		keys := make([]string, 0, len(v))
		for key := range v {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		out := make(map[string]any, len(v))
		for _, key := range keys {
			normalized, err := n.Value(v[key])
			if err != nil {
				return nil, err
			}
			out[n.String(key)] = normalized
		}
		return out, nil
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64, float32, float64:
		return json.Number(fmt.Sprint(v)), nil
	}
	// Anything else (pgtype.Numeric, netip.Prefix, ...) goes through its own JSON
	// form, which every pgtype value provides.
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("writeproof: cannot normalize a %T value: %w", value, err)
	}
	var decoded any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("writeproof: cannot normalize a %T value: %w", value, err)
	}
	return n.Value(decoded)
}

// Effects is everything one execution produced, normalized: the GraphQL response
// and each comparison table's rows.
type Effects struct {
	Case     string           `json:"case"`
	Response any              `json:"response"`
	Tables   map[string][]any `json:"tables"`
}

// Rows is the total number of rows across the tables.
func (e Effects) Rows() int {
	total := 0
	for _, rows := range e.Tables {
		total += len(rows)
	}
	return total
}

// Canonical is the effects' deterministic JSON encoding (sorted keys, no HTML
// escaping, no trailing newline).
func (e Effects) Canonical() ([]byte, error) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(e); err != nil {
		return nil, fmt.Errorf("writeproof: encode effects: %w", err)
	}
	return bytes.TrimRight(buffer.Bytes(), "\n"), nil
}

// Digest is "sha256:" plus the hex SHA-256 of the canonical effects: the value a
// write_executed receipt stores as side_effect_digest and a case commits as its
// baseline.
func (e Effects) Digest() (string, error) {
	canonical, err := e.Canonical()
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

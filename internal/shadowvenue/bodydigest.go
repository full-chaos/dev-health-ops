package shadowvenue

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"slices"
	"strings"
)

// bodyLimit is the most a slot's answer may hold; a larger one stops the drive (it would be digested cut).
const bodyLimit = 4 << 20

// VolatileKeys are the JSON keys dropped from a row's normalized body, by route key, each with the reason it differs
// between two runs of the same legacy answer on the same data.
type VolatileKeys map[string]map[string]string

// PinnedVolatile is the volatile set the driver uses. A key is pinned only when a legacy-versus-legacy run shows it
// differs (bodydiff); it starts empty.
var PinnedVolatile = VolatileKeys{}

// BodyDigest is the digest of one answer: its length, the sha256 of its raw bytes and the sha256 of its normalized form.
// The body itself is never kept.
type BodyDigest struct {
	Len       int
	SHA, Norm string
}

// DigestBody digests an answer of the route key.
func DigestBody(key string, raw []byte, frozen ThrowawayIDs, volatile VolatileKeys) BodyDigest {
	raw256 := sha256.Sum256(raw)
	out := BodyDigest{Len: len(raw), SHA: hex.EncodeToString(raw256[:])}
	norm := normalizeBody(key, raw, frozen, volatile)
	n256 := sha256.Sum256(norm)
	out.Norm = hex.EncodeToString(n256[:])
	return out
}

// normalizeBody is the canonical form: JSON with sorted keys, the frozen throwaway org and user ids replaced by a
// placeholder of their kind, the pinned volatile keys of the row dropped at every level, and the state of a URL
// dropped. A body that is no JSON is text with the same id replacement.
func normalizeBody(key string, raw []byte, frozen ThrowawayIDs, volatile VolatileKeys) []byte {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if json.Valid(raw) && dec.Decode(&v) == nil {
		var out bytes.Buffer
		enc := json.NewEncoder(&out)
		enc.SetEscapeHTML(false)
		if enc.Encode(walkBody(v, volatile[key], frozen)) == nil {
			return out.Bytes()
		}
	}
	return []byte(frozen.replaceIDs(string(raw)))
}

func walkBody(v any, drop map[string]string, frozen ThrowawayIDs) any {
	switch x := v.(type) {
	case map[string]any:
		out := make(map[string]any, len(x))
		for k, val := range x {
			if _, volatile := drop[k]; volatile {
				continue
			}
			out[frozen.replaceIDs(k)] = walkBody(val, drop, frozen)
		}
		return out
	case []any:
		out := make([]any, len(x))
		for i, val := range x {
			out[i] = walkBody(val, drop, frozen)
		}
		return out
	case string:
		return frozen.replaceIDs(dropURLState(x))
	}
	return v
}

// dropURLState replaces the value of the state query parameter of an http(s) URL string with a fixed word.
func dropURLState(s string) string {
	u, err := url.Parse(s)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.RawQuery == "" {
		return s
	}
	parts := strings.Split(u.RawQuery, "&")
	changed := false
	for i, p := range parts {
		if name, _, ok := strings.Cut(p, "="); ok && name == "state" {
			parts[i] = "state={state}"
			changed = true
		}
	}
	if !changed {
		return s
	}
	slices.Sort(parts)
	u.RawQuery = strings.Join(parts, "&")
	return u.String()
}

// BodyDiff lists the cells (row, caller, k, route key) whose normalized body digest differs between two slots files of
// the same plan. A cell in one file only, or a sent slot with no digest, is an error: the files are not a pair.
func BodyDiff(a, b []Slot) ([]string, error) {
	type cell struct {
		row, caller string
		k           int
	}
	index := func(slots []Slot) map[cell]Slot {
		out := map[cell]Slot{}
		for _, s := range slots {
			if !s.Gap() {
				out[cell{s.Row, s.Caller, s.K}] = s
			}
		}
		return out
	}
	ia, ib := index(a), index(b)
	if len(ia) != len(ib) {
		return nil, fmt.Errorf("bodydiff: the files hold %d and %d sent slots: not a pair", len(ia), len(ib))
	}
	var diffs []string
	for c, sa := range ia {
		sb, ok := ib[c]
		switch {
		case !ok:
			return nil, fmt.Errorf("bodydiff: row %s %s k=%d is in one file only", c.row, c.caller, c.k)
		case sa.BodyNormSHA == "" || sb.BodyNormSHA == "":
			return nil, fmt.Errorf("bodydiff: row %s %s k=%d has no body digest", c.row, c.caller, c.k)
		case sa.Key() != sb.Key():
			return nil, fmt.Errorf("bodydiff: row %s %s k=%d is on two routes", c.row, c.caller, c.k)
		case sa.Status != sb.Status || sa.BodyNormSHA != sb.BodyNormSHA:
			diffs = append(diffs, fmt.Sprintf("row %s\t%s\tk=%d\t%s\tstatus %d/%d", c.row, c.caller, c.k, sa.Key(), sa.Status, sb.Status))
		}
	}
	slices.Sort(diffs)
	return diffs, nil
}

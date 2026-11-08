package shadowvenue

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"slices"
	"strconv"
	"strings"
	"unicode"
)

const keysHeader = "seq\tpath\thmac12"

// Leaf is one leaf of an answer: its normalized path and the first 12 hex of the HMAC of its value. The value is not kept.
type Leaf struct{ Path, HMAC12 string }

// NewBodyKey writes a 32-byte random key as hex to a new owner-only file; an existing file is never touched.
func NewBodyKey(path string) error {
	raw := make([]byte, 32)
	if _, err := rand.Read(raw); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("bodykey: %s: %w", path, errors.Unwrap(err))
	}
	if _, err := file.WriteString(hex.EncodeToString(raw) + "\n"); err != nil {
		_ = file.Close()
		return err
	}
	return file.Close()
}

// ReadBodyKey reads a key file: a regular owner-only file of 64 hex characters.
func ReadBodyKey(path string) ([]byte, error) {
	info, err := os.Stat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 || info.Size() > 128 {
		return nil, fmt.Errorf("body key %s: want an owner-only (0600) regular file", path)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("body key %s: unreadable", path)
	}
	key, err := hex.DecodeString(strings.TrimSpace(string(raw)))
	if err != nil || len(key) != 32 {
		return nil, fmt.Errorf("body key %s: want 64 hex characters", path)
	}
	return key, nil
}

// CreateKeysFile opens a new owner-only side file; an existing file is never overwritten.
func CreateKeysFile(path string) (*os.File, error) {
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return nil, fmt.Errorf("bodykeys-out %s: %w", path, errors.Unwrap(err))
	}
	return file, nil
}

func leafHMAC(key, value []byte) string {
	mac := hmac.New(sha256.New, key)
	mac.Write(value)
	return hex.EncodeToString(mac.Sum(nil))[:12]
}

// escapeKey makes an object key one path segment that no other structure can produce: ~ / [ ] and every control
// character are escaped, so a segment holds none of the characters that separate segments or end a line.
func escapeKey(k string) string {
	var b strings.Builder
	for _, r := range k {
		switch {
		case r == '~':
			b.WriteString("~0")
		case r == '/':
			b.WriteString("~1")
		case r == '[':
			b.WriteString("~2")
		case r == ']':
			b.WriteString("~3")
		case unicode.IsControl(r):
			fmt.Fprintf(&b, "~x%02X", r)
		default:
			b.WriteRune(r)
		}
	}
	return b.String()
}

// BodyLeaves lists the leaves of an answer: path ($ is the root, /key for an object key with ~ / [ ] and control
// characters escaped as ~0 ~1 ~2 ~3 ~xHH, [] for an array element, so two structures never share a path), HMAC-SHA256 of the leaf's canonical JSON value under key. A body that is no JSON is one leaf, $raw.
func BodyLeaves(key, raw []byte) []Leaf {
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.UseNumber()
	var v any
	if !json.Valid(raw) || dec.Decode(&v) != nil {
		return []Leaf{{"$raw", leafHMAC(key, raw)}}
	}
	var out []Leaf
	var walk func(path string, v any)
	walk = func(path string, v any) {
		switch x := v.(type) {
		case map[string]any:
			if len(x) == 0 {
				out = append(out, Leaf{path, leafHMAC(key, []byte("{}"))})
			}
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			slices.Sort(keys)
			for _, k := range keys {
				walk(path+"/"+escapeKey(k), x[k])
			}
		case []any:
			if len(x) == 0 {
				out = append(out, Leaf{path, leafHMAC(key, []byte("[]"))})
			}
			for _, e := range x {
				walk(path+"[]", e)
			}
		default:
			canon, _ := json.Marshal(x)
			out = append(out, Leaf{path, leafHMAC(key, canon)})
		}
	}
	walk("$", v)
	return out
}

// ParseKeysFile reads a side file: the leaves by slot seq.
func ParseKeysFile(raw []byte) (map[int][]Leaf, error) {
	lines := strings.Split(strings.TrimRight(string(raw), "\n"), "\n")
	if len(lines) == 0 || lines[0] != keysHeader {
		return nil, errors.New("keys: the first line is not the side file header")
	}
	out := map[int][]Leaf{}
	for n, line := range lines[1:] {
		f := strings.Split(line, "\t")
		seq, err := strconv.Atoi(f[0])
		if len(f) != 3 || err != nil || len(f[2]) != 12 {
			return nil, fmt.Errorf("keys: line %d is not seq, path and hmac12", n+2)
		}
		out[seq] = append(out[seq], Leaf{f[1], f[2]})
	}
	return out, nil
}

// KeyDiff lists, sorted, the leaf paths whose HMACs differ between two answers or that only one answer has.
func KeyDiff(a, b []Leaf) []string {
	count := map[Leaf]int{}
	for _, l := range a {
		count[l]++
	}
	for _, l := range b {
		count[l]--
	}
	seen := map[string]bool{}
	var paths []string
	for l, n := range count {
		if n != 0 && !seen[l.Path] {
			seen[l.Path] = true
			paths = append(paths, l.Path)
		}
	}
	slices.Sort(paths)
	return paths
}

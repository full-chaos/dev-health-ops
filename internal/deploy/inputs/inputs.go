// Package inputs reads the data file of a `dho deploy` run: one KEY=VALUE
// buffer, read once, hashed and parsed from that same buffer. The file is
// data, never code: every key has one regex per target and an unknown key is
// a refusal. A refusal never echoes a value; it names the line and the key.
package inputs

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
)

const (
	// Version is the one inputs schema this binary reads (line DHO_DEPLOY_INPUTS=1).
	Version  = "1"
	MaxBytes = 65536
	MaxLines = 300

	versionKey = "DHO_DEPLOY_INPUTS"
)

// Refusal is why an inputs buffer is not accepted. NotBuilt marks a form the
// kit runs and this binary does not run yet.
type Refusal struct {
	Line     int
	Reason   string
	NotBuilt bool
	Class    string
	Key      string
}

func (r *Refusal) Error() string {
	s := "inputs: " + r.Reason
	if r.Line > 0 {
		s = fmt.Sprintf("inputs line %d: %s", r.Line, r.Reason)
	}
	if r.NotBuilt {
		s += " (not in dho yet: use the kit)"
	}
	return s
}

func refuse(class, key string, line int, format string, a ...any) *Refusal {
	return &Refusal{Line: line, Reason: fmt.Sprintf(format, a...), Class: class, Key: key}
}

func notBuilt(class, key string, line int, format string, a ...any) *Refusal {
	return &Refusal{Line: line, Reason: fmt.Sprintf(format, a...), NotBuilt: true, Class: class, Key: key}
}

// Reason classes of an inputs decision: the fixed set of the structured event.
const (
	ClassTooLarge        = "too_large"
	ClassNulByte         = "nul_byte"
	ClassBadByte         = "bad_byte"
	ClassTooManyLines    = "too_many_lines"
	ClassNotKeyValue     = "not_key_value"
	ClassNoEntries       = "no_entries"
	ClassVersionFirst    = "version_first"
	ClassVersionMismatch = "version_mismatch"
	ClassKitFile         = "kit_file"
	ClassDuplicateKey    = "duplicate_key"
	ClassBadValue        = "bad_value"
	ClassMissingKey      = "missing_key"
	ClassUnknownKey      = "unknown_key"
	ClassRepeatLimit     = "repeat_limit"
	ClassForbiddenKey    = "forbidden_key"
	ClassKitTarget       = "kit_target"
	ClassKitKey          = "kit_key"
	ClassHookRow         = "hook_row"
	ClassAuthRow         = "auth_row"
	ClassRevRule         = "rev_rule"
	ClassShaRule         = "sha_rule"
	ClassDigestRule      = "digest_rule"
	ClassStdinUnreadable = "stdin_unreadable"
	ClassTargetMismatch  = "target_mismatch"
)

// Classes is the closed set of reason classes.
var Classes = []string{
	ClassTooLarge, ClassNulByte, ClassBadByte, ClassTooManyLines, ClassNotKeyValue, ClassNoEntries,
	ClassVersionFirst, ClassVersionMismatch, ClassKitFile, ClassDuplicateKey, ClassBadValue, ClassMissingKey,
	ClassUnknownKey, ClassRepeatLimit, ClassForbiddenKey, ClassKitTarget, ClassKitKey, ClassHookRow,
	ClassAuthRow, ClassRevRule, ClassShaRule, ClassDigestRule, ClassStdinUnreadable, ClassTargetMismatch,
}

// scrubKey keeps a key only when it is one of the fixed key names of this
// binary; a key from the input that is not (an unknown key) is input bytes.
func scrubKey(k string) string {
	switch k {
	case versionKey, "TARGET", "RUN_TAG", "AUTHOR", "RENDER_FILE", "PRODROLL_INPUTS":
		return k
	}
	for _, t := range targets {
		if _, ok := t.keys[k]; ok || t.notBuilt[k] {
			return k
		}
	}
	return ""
}

// Read takes the whole buffer in ONE read pass with a bound; the caller hashes
// and parses only what Read returned.
func Read(r io.Reader) ([]byte, error) {
	if r == nil {
		return []byte{}, nil
	}
	return io.ReadAll(io.LimitReader(r, MaxBytes+1))
}

// SHA256 is the lower-case hex sha256 of buf.
func SHA256(buf []byte) string {
	sum := sha256.Sum256(buf)
	return hex.EncodeToString(sum[:])
}

// Inputs is a parsed buffer. Values holds the scalar keys, Lists the
// repeatable ones, in file order.
type Inputs struct {
	SHA256    string
	Target    string
	RunTag    string
	Author    string
	Values    map[string]string
	Lists     map[string][]string
	DevHealth *DevHealth
	lines     map[string][]int
}

// line is the first file line of key k (0 = none).
func (in *Inputs) line(k string) int {
	if l := in.lines[k]; len(l) > 0 {
		return l[0]
	}
	return 0
}

// keySpec is one key of a target. match is the kit regex of the key; a kit
// regex that nests counted repeats past RE2's limit is a func with the same
// verdicts (inputs_test.go pins them against the kit's own verdicts).
type keySpec struct {
	match     func(string) bool
	text      string
	maxRepeat int // 0 = a scalar key
	required  bool
}

type targetSpec struct {
	keys     map[string]keySpec
	notBuilt map[string]bool
	check    func(*Inputs) error
}

var (
	keyLine   = regexp.MustCompile(`^([A-Z][A-Z0-9_]{0,47})=(.*)$`)
	reTarget  = regexp.MustCompile(`^(dev-health|ingress-nginx|f1pipe|acr)$`)
	reRunTag  = regexp.MustCompile(`^[a-z0-9][a-z0-9-]{0,31}$`)
	reAuthor  = regexp.MustCompile(`^[a-z][a-z0-9-]{1,31}$`)
	kitTarget = map[string]bool{"ingress-nginx": true, "f1pipe": true, "acr": true}
)

type entry struct {
	line       int
	key, value string
}

// Parse checks buf and returns the inputs, or a *Refusal. The sha256 is of
// buf itself, so the hash and the parse can never see different bytes.
func Parse(buf []byte) (*Inputs, error) {
	in, err := parse(buf)
	if r, ok := err.(*Refusal); ok {
		r.Key = scrubKey(r.Key)
	}
	return in, err
}

func parse(buf []byte) (*Inputs, error) {
	in := &Inputs{SHA256: SHA256(buf), Values: map[string]string{}, Lists: map[string][]string{}, lines: map[string][]int{}}
	if len(buf) > MaxBytes {
		return in, refuse(ClassTooLarge, "", 0, "larger than %d bytes", MaxBytes)
	}
	bad, badLine, nulLine, line := 0, 0, 0, 1
	for _, b := range buf {
		switch {
		case b == '\n':
			line++
			continue
		case b == 0 && nulLine == 0:
			nulLine = line
		}
		if b < 0x20 || b > 0x7e {
			bad++
			if badLine == 0 {
				badLine = line
			}
		}
	}
	if nulLine > 0 {
		return in, refuse(ClassNulByte, "", nulLine, "the buffer holds a NUL byte")
	}
	if bad > 0 {
		return in, refuse(ClassBadByte, "", badLine, "%d byte(s) outside printable ASCII + LF (TAB, CR, non-ASCII); the first is on this line", bad)
	}
	lines := strings.Split(string(buf), "\n")
	if len(lines) > 0 && lines[len(lines)-1] == "" {
		lines = lines[:len(lines)-1]
	}
	if len(lines) > MaxLines {
		return in, refuse(ClassTooManyLines, "", 0, "more than %d lines", MaxLines)
	}
	var entries []entry
	for i, l := range lines {
		if l == "" || strings.HasPrefix(l, "#") {
			continue
		}
		m := keyLine.FindStringSubmatch(l)
		if m == nil {
			return in, refuse(ClassNotKeyValue, "", i+1, "not KEY=VALUE (KEY = [A-Z][A-Z0-9_]*, no space)")
		}
		entries = append(entries, entry{i + 1, m[1], m[2]})
	}
	if len(entries) == 0 {
		return in, refuse(ClassNoEntries, "", 0, "no KEY=VALUE line")
	}
	first := entries[0]
	switch {
	case first.key == "PRODROLL_INPUTS":
		return in, refuse(ClassKitFile, first.key, first.line, "a kit inputs file (PRODROLL_INPUTS); dho reads %s=%s", versionKey, Version)
	case first.key != versionKey:
		return in, refuse(ClassVersionFirst, first.key, first.line, "the first KEY=VALUE line must be %s=%s", versionKey, Version)
	case first.value != Version:
		return in, refuse(ClassVersionMismatch, versionKey, first.line, "inputs version is not %s", Version)
	}
	entries = entries[1:]

	targetLine := 0
	for _, e := range entries {
		if e.key != "TARGET" {
			continue
		}
		if targetLine != 0 {
			return in, refuse(ClassDuplicateKey, "TARGET", e.line, "duplicate key TARGET")
		}
		targetLine = e.line
		if !reTarget.MatchString(e.value) {
			return in, refuse(ClassBadValue, "TARGET", e.line, "bad value for TARGET")
		}
		in.Target = e.value
	}
	if targetLine == 0 {
		return in, refuse(ClassMissingKey, "TARGET", 0, "missing key TARGET")
	}
	if kitTarget[in.Target] {
		return in, notBuilt(ClassKitTarget, "TARGET", targetLine, "TARGET names a kit-only target")
	}
	spec := targets[in.Target]

	for _, e := range entries {
		switch e.key {
		case "TARGET":
			continue
		case versionKey:
			return in, refuse(ClassDuplicateKey, versionKey, e.line, "duplicate key %s", versionKey)
		case "RUN_TAG", "AUTHOR":
			re := reRunTag
			if e.key == "AUTHOR" {
				re = reAuthor
			}
			if _, dup := in.Values[e.key]; dup {
				return in, refuse(ClassDuplicateKey, e.key, e.line, "duplicate key %s", e.key)
			}
			if !re.MatchString(e.value) {
				return in, refuse(ClassBadValue, e.key, e.line, "bad value for %s", e.key)
			}
			in.Values[e.key] = e.value
			in.lines[e.key] = append(in.lines[e.key], e.line)
			continue
		case "RENDER_FILE":
			return in, refuse(ClassForbiddenKey, e.key, e.line, "RENDER_FILE is not a dho key: dho renders tree B on the node and gates it on RENDER_SHA256")
		}
		if spec.notBuilt[e.key] {
			return in, notBuilt(ClassKitKey, e.key, e.line, "key %s", e.key)
		}
		ks, known := spec.keys[e.key]
		if !known {
			return in, refuse(ClassUnknownKey, e.key, e.line, "unknown key for this target")
		}
		if !ks.match(e.value) {
			return in, refuse(ClassBadValue, e.key, e.line, "bad value for %s", e.key)
		}
		if ks.maxRepeat > 0 {
			in.Lists[e.key] = append(in.Lists[e.key], e.value)
			in.lines[e.key] = append(in.lines[e.key], e.line)
			if len(in.Lists[e.key]) > ks.maxRepeat {
				return in, refuse(ClassRepeatLimit, e.key, e.line, "more than %d %s lines", ks.maxRepeat, e.key)
			}
			continue
		}
		if _, dup := in.Values[e.key]; dup {
			return in, refuse(ClassDuplicateKey, e.key, e.line, "duplicate key %s", e.key)
		}
		in.Values[e.key] = e.value
		in.lines[e.key] = append(in.lines[e.key], e.line)
	}
	required := []string{"RUN_TAG", "AUTHOR"}
	for k, ks := range spec.keys {
		if ks.required {
			required = append(required, k)
		}
	}
	sort.Strings(required)
	for _, k := range required {
		_, scalar := in.Values[k]
		if !scalar && len(in.Lists[k]) == 0 {
			return in, refuse(ClassMissingKey, k, 0, "missing key %s", k)
		}
	}
	in.RunTag, in.Author = in.Values["RUN_TAG"], in.Values["AUTHOR"]
	if err := spec.check(in); err != nil {
		return in, err
	}
	return in, nil
}

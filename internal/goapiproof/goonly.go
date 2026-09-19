package goapiproof

import (
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"sync"
)

// The go-only proof class.
//
// An operation whose Python execution path was deleted cannot be proven by
// comparing two planes: the Python baseline leg answers with the deletion
// error instead of data. Admit refuses that pair as a response carrying
// GraphQL errors, which is right for every operation that has not been
// through a deletion, and leaves a deleted one unprovable forever.
//
// The go-only class admits exactly one more shape, and only for an operation
// the go-served ledger (goserved_ledger.json) names:
//
//   - the baseline leg answered with the deletion error and nothing else
//     (DeletionErrorMatches), and
//   - the candidate leg passed every check Admit applies to a candidate,
//     with a root that is present, non-null and non-empty.
//
// The receipt it produces is the cited-mismatch arm of the enablement
// predicate with one citation, GoOnlyCitationPrefix..., built by
// NewGoOnlyCitation and read by ParseGoOnlyCitation. Nothing else may write
// that prefix: a declared baseline defect or a stochastic-leaf class that
// carries it is refused before a request is sent, and both receipt writers
// refuse a citation that is not the ledger's own.

// GoOnlyCitationPrefix starts the one citation that marks a go-only receipt.
const GoOnlyCitationPrefix = "GO-ONLY:"

// ProvenUnderGoOnly is the word a report prints for an operation proven by
// the go-only class. It is never a two-plane match.
const ProvenUnderGoOnly = "go_only"

// VerdictGoOnly is the verdict word a terminal line prints for such an
// operation.
const VerdictGoOnly = "PROVEN_GO_ONLY"

//go:embed goserved_ledger.json
var goServedLedgerJSON []byte

// GoServedGuard names one test that keeps proving the kernel of a
// deleted-Python operation against the retained oracle.
type GoServedGuard struct {
	File string `json:"file"`
	Test string `json:"test"`
}

// GoServedEntry is one operation whose Python execution path is deleted.
type GoServedEntry struct {
	Operation string `json:"operation"`
	// TwoPlaneOpsSHA is the ops build at which this operation last had a
	// two-plane match receipt.
	TwoPlaneOpsSHA string          `json:"two_plane_ops_sha"`
	Guards         []GoServedGuard `json:"guards"`
}

// GoServedLedger is the machine-readable list of deleted-Python operations.
type GoServedLedger struct {
	// MessageTemplate is the exact message of the deletion error, with the
	// operation name as "{operation}".
	MessageTemplate string          `json:"deletion_error_message"`
	Entries         []GoServedEntry `json:"entries"`
}

var (
	hexSHA40        = regexp.MustCompile(`^[0-9a-f]{40}$`)
	citationToken   = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
	operationPlace  = "{operation}"
	defaultLedger   *GoServedLedger
	defaultLedgerMu sync.Once
	defaultLedgerEr error
)

// ParseGoServedLedger decodes and validates a ledger document.
func ParseGoServedLedger(raw []byte) (*GoServedLedger, error) {
	var ledger GoServedLedger
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ledger); err != nil {
		return nil, fmt.Errorf("goapiproof: go-served ledger: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return nil, errors.New("goapiproof: go-served ledger carries bytes after its JSON value")
	}
	if strings.Count(ledger.MessageTemplate, operationPlace) != 1 {
		return nil, fmt.Errorf("goapiproof: go-served ledger message template must contain %s exactly once", operationPlace)
	}
	if len(ledger.Entries) == 0 {
		return nil, errors.New("goapiproof: go-served ledger names no operation")
	}
	seen := map[string]bool{}
	for _, entry := range ledger.Entries {
		if !citationToken.MatchString(entry.Operation) {
			return nil, fmt.Errorf("goapiproof: go-served ledger operation %q is not a plain name", entry.Operation)
		}
		if seen[entry.Operation] {
			return nil, fmt.Errorf("goapiproof: go-served ledger names %s twice", entry.Operation)
		}
		seen[entry.Operation] = true
		if !hexSHA40.MatchString(entry.TwoPlaneOpsSHA) {
			return nil, fmt.Errorf("goapiproof: go-served ledger %s: two_plane_ops_sha must be 40 lowercase hex characters", entry.Operation)
		}
		if len(entry.Guards) == 0 {
			return nil, fmt.Errorf("goapiproof: go-served ledger %s names no guard test: a deleted-Python operation with nothing keeping its kernel honest cannot be proven", entry.Operation)
		}
		guards := map[string]bool{}
		for _, guard := range entry.Guards {
			if strings.TrimSpace(guard.File) == "" || !citationToken.MatchString(guard.Test) {
				return nil, fmt.Errorf("goapiproof: go-served ledger %s: guard %+v needs a file and a plain test name", entry.Operation, guard)
			}
			if guards[guard.Test] {
				return nil, fmt.Errorf("goapiproof: go-served ledger %s: guard %s is named twice", entry.Operation, guard.Test)
			}
			guards[guard.Test] = true
		}
	}
	return &ledger, nil
}

// DefaultGoServedLedger is the ledger this binary was built with.
func DefaultGoServedLedger() (*GoServedLedger, error) {
	defaultLedgerMu.Do(func() {
		defaultLedger, defaultLedgerEr = ParseGoServedLedger(goServedLedgerJSON)
	})
	return defaultLedger, defaultLedgerEr
}

// Entry returns the ledger entry for operation.
func (l *GoServedLedger) Entry(operation string) (GoServedEntry, bool) {
	if l == nil {
		return GoServedEntry{}, false
	}
	for _, entry := range l.Entries {
		if entry.Operation == operation {
			return entry, true
		}
	}
	return GoServedEntry{}, false
}

// Operations lists the ledger's operations, sorted.
func (l *GoServedLedger) Operations() []string {
	if l == nil {
		return nil
	}
	operations := make([]string, 0, len(l.Entries))
	for _, entry := range l.Entries {
		operations = append(operations, entry.Operation)
	}
	sort.Strings(operations)
	return operations
}

// ExpectedMessage is the exact message of the deletion error for operation.
func (l *GoServedLedger) ExpectedMessage(operation string) string {
	if l == nil {
		return ""
	}
	return strings.Replace(l.MessageTemplate, operationPlace, operation, 1)
}

// DeletionErrorMatches reports whether snapshot is exactly the deletion
// error for operation and nothing else, or names the first way it is not.
//
// Exactly: one error; its message equal to the ledger's template; its path
// equal to [root]; no key besides message, locations and path; and no data
// beyond a null root. The class name of the server-side exception is not on
// the wire, so the message text and path are the discriminator.
func (l *GoServedLedger) DeletionErrorMatches(operation, root string, snapshot Snapshot) (bool, string) {
	if l == nil {
		return false, "no go-served ledger"
	}
	if root == "" {
		return false, "the operation declares no response root"
	}
	if len(snapshot.Errors) != 1 {
		return false, fmt.Sprintf("baseline carried %d GraphQL errors, the deletion error is exactly one", len(snapshot.Errors))
	}
	failure := snapshot.Errors[0]
	for key := range failure {
		switch key {
		case "message", "locations", "path":
		default:
			return false, fmt.Sprintf("baseline error carries key %q, the deletion error carries message, locations and path only", key)
		}
	}
	message, _ := failure["message"].(string)
	if message != l.ExpectedMessage(operation) {
		return false, "baseline error message is not the deletion error's message for this operation"
	}
	path, _ := failure["path"].([]any)
	if len(path) != 1 || path[0] != root {
		return false, fmt.Sprintf("baseline error path is not [%q]", root)
	}
	if snapshot.TrailingBytes {
		return false, "baseline body carried bytes after its JSON value"
	}
	switch data := snapshot.Data.(type) {
	case nil:
	case map[string]any:
		value, present := data[root]
		if len(data) != 1 || !present || value != nil {
			return false, "baseline data is more than a null root"
		}
	default:
		return false, fmt.Sprintf("baseline data is a %T", snapshot.Data)
	}
	return true, ""
}

// GoOnlyCitation is the decoded form of a go-only citation.
type GoOnlyCitation struct {
	Operation      string
	TwoPlaneOpsSHA string
	Guards         []string
}

func (c GoOnlyCitation) String() string {
	return GoOnlyCitationPrefix + "op=" + c.Operation + ";two_plane=" + c.TwoPlaneOpsSHA + ";guards=" + strings.Join(c.Guards, ",")
}

// HasGoOnlyPrefix reports whether a citation claims to be a go-only one.
// Case and surrounding blanks do not hide the claim.
func HasGoOnlyPrefix(citation string) bool {
	return strings.HasPrefix(strings.ToUpper(strings.TrimLeft(citation, blankCitationCutset)), GoOnlyCitationPrefix)
}

// NewGoOnlyCitation is the ONE constructor of a go-only citation. It refuses
// unless the ledger names the operation, its two-plane sha and at least one
// guard.
func NewGoOnlyCitation(ledger *GoServedLedger, operation string) (string, error) {
	entry, ok := ledger.Entry(operation)
	if !ok {
		return "", fmt.Errorf("goapiproof: %q is not in the go-served ledger: no go-only citation can be built for it", operation)
	}
	if !hexSHA40.MatchString(entry.TwoPlaneOpsSHA) || len(entry.Guards) == 0 {
		return "", fmt.Errorf("goapiproof: the go-served ledger entry for %q has no two-plane sha or no guard", operation)
	}
	guards := make([]string, 0, len(entry.Guards))
	for _, guard := range entry.Guards {
		guards = append(guards, guard.Test)
	}
	return GoOnlyCitation{Operation: operation, TwoPlaneOpsSHA: entry.TwoPlaneOpsSHA, Guards: guards}.String(), nil
}

// ParseGoOnlyCitation is the ONE parser. Every field is a plain token, so it
// accepts only the exact text GoOnlyCitation.String produces.
func ParseGoOnlyCitation(citation string) (GoOnlyCitation, error) {
	rest, found := strings.CutPrefix(citation, GoOnlyCitationPrefix)
	if !found {
		return GoOnlyCitation{}, fmt.Errorf("goapiproof: citation does not start with %q", GoOnlyCitationPrefix)
	}
	fields := strings.Split(rest, ";")
	if len(fields) != 3 {
		return GoOnlyCitation{}, fmt.Errorf("goapiproof: go-only citation has %d fields, expected op, two_plane, guards", len(fields))
	}
	var parsed GoOnlyCitation
	values := make([]string, 3)
	for i, key := range []string{"op=", "two_plane=", "guards="} {
		value, ok := strings.CutPrefix(fields[i], key)
		if !ok {
			return GoOnlyCitation{}, fmt.Errorf("goapiproof: go-only citation field %d does not start with %q", i+1, key)
		}
		values[i] = value
	}
	parsed.Operation, parsed.TwoPlaneOpsSHA = values[0], values[1]
	if !citationToken.MatchString(parsed.Operation) || !hexSHA40.MatchString(parsed.TwoPlaneOpsSHA) {
		return GoOnlyCitation{}, errors.New("goapiproof: go-only citation names a malformed operation or sha")
	}
	if values[2] == "" {
		return GoOnlyCitation{}, errors.New("goapiproof: go-only citation names no guard")
	}
	named := map[string]bool{}
	for _, guard := range strings.Split(values[2], ",") {
		if !citationToken.MatchString(guard) {
			return GoOnlyCitation{}, fmt.Errorf("goapiproof: go-only citation guard %q is not a plain test name", guard)
		}
		if named[guard] {
			return GoOnlyCitation{}, fmt.Errorf("goapiproof: go-only citation names guard %q twice", guard)
		}
		named[guard] = true
		parsed.Guards = append(parsed.Guards, guard)
	}
	return parsed, nil
}

// ValidateGoOnlyReceiptCitations is the writer-side check. A receipt whose
// baseline_defect array carries the go-only prefix anywhere must carry
// exactly one citation, that citation must be the ledger's own for the
// receipt's operation, and the terminal state must be the mismatch arm. A
// receipt without the prefix passes untouched.
func ValidateGoOnlyReceiptCitations(ledger *GoServedLedger, operation, terminalState string, citations []string) error {
	prefixed := false
	for _, citation := range citations {
		if HasGoOnlyPrefix(citation) {
			prefixed = true
		}
	}
	if !prefixed {
		return nil
	}
	if len(citations) != 1 {
		return fmt.Errorf("goapiproof: refusing a receipt with %d citations one of which claims the go-only prefix: a go-only receipt carries exactly its own citation", len(citations))
	}
	if terminalState != TerminalStateMismatch {
		return fmt.Errorf("goapiproof: refusing a go-only citation on a %q receipt: the class is the mismatch arm only", terminalState)
	}
	parsed, err := ParseGoOnlyCitation(citations[0])
	if err != nil {
		return err
	}
	want, err := NewGoOnlyCitation(ledger, operation)
	if err != nil {
		return err
	}
	if parsed.String() != want {
		return fmt.Errorf("goapiproof: refusing a go-only citation that is not the ledger's own for %q", operation)
	}
	return nil
}

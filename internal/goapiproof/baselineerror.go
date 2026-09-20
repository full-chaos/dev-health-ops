package goapiproof

import (
	"fmt"
	"regexp"
	"strings"
)

// The declared-baseline-error class.
//
// A request whose Python baseline answers a GraphQL error because of a defect
// in the Python resolver cannot be compared: Admit refuses a baseline that
// errored, because agreement on a failure is not proof. Leaving such a request
// refused leaves the Go answer for that scope unmeasured. The class measures
// the Go leg alone, against what the page asks for, and only where the request
// declares the Python failure by ticket, reason and message token.
//
// What the class admits, all of it required:
//
//   - the request declares the error (DeclaredBaselineError) and the operation
//     is not a go-served one (that class has its own rule);
//   - the baseline leg answered exactly one GraphQL error, at the operation's
//     root path, carrying the declared token, with a null root and nothing
//     else in `data`; a baseline that answered without errors makes the
//     declaration stale and is refused, and a baseline error that is not the
//     declared one is refused as an errored response;
//   - the candidate leg passed every check Admit applies to a candidate, with a
//     resolved root;
//   - the candidate's answer has the declared shape: an empty list, or a
//     non-empty list whose elements all carry the scope the request named.
//
// The verdict is `unsupported` with ProvenUnderDeclaredBaselineError. It is
// never a match and never a cited mismatch, so the enablement predicate, which
// admits only those two, does not read it: the class records that the Go leg
// was measured, it cannot authorize a routing change on its own.

const (
	// ProvenUnderDeclaredBaselineError is the word a report prints for a
	// request measured under this class. It is never a two-plane match.
	ProvenUnderDeclaredBaselineError = "declared_baseline_error"

	// VerdictDeclaredBaselineError is the verdict word a terminal line prints
	// for such a request. It is never a two-plane match.
	VerdictDeclaredBaselineError = "PROVEN_GO_LEG_DECLARED_BASELINE_ERROR"

	// RefusalStaleBaselineErrorDeclaration: the request declares a baseline
	// error and the baseline answered without one, so the declaration matched
	// nothing.
	RefusalStaleBaselineErrorDeclaration = "declared_baseline_error_matched_nothing"
	// RefusalDeclaredBaselineErrorGoLeg: the baseline error was the declared
	// one and the Go answer does not have the declared shape.
	RefusalDeclaredBaselineErrorGoLeg = "declared_baseline_error_go_leg_unmet"
	// RefusalInvalidDeclaredBaselineError: the declaration itself is malformed
	// or sits on an operation the class does not apply to.
	RefusalInvalidDeclaredBaselineError = "declared_baseline_error_invalid"
)

// GoAnswerShape is what the Go leg must answer for a declared request.
type GoAnswerShape string

const (
	// GoAnswerEmpty: the list is present and empty (an unresolved scope
	// answers no rows).
	GoAnswerEmpty GoAnswerShape = "empty"
	// GoAnswerNonEmpty: the list is present and non-empty, every element
	// carries the request's scope (the request's own ScopeEcho, and the
	// declaration's UniformField when set).
	GoAnswerNonEmpty GoAnswerShape = "non_empty"
)

// DeclaredBaselineError declares that the Python baseline of one request
// answers a GraphQL error because Python is wrong, and what the Go leg must
// answer instead.
type DeclaredBaselineError struct {
	// Ticket owns the Python defect.
	Ticket string
	// Reason states the defect in words.
	Reason string
	// MessageToken is a plain token the baseline error's message must carry
	// (a database error code, for example). A baseline error without it is not
	// the declared one.
	MessageToken string
	// List is the dotted path, with its leading "data." segment, of the list
	// the Go answer is checked on.
	List string
	// Answer is the shape the list must have on the Go leg.
	Answer GoAnswerShape
	// UniformField, when set with GoAnswerNonEmpty, is a field every element
	// must carry with one and the same non-empty value: the answer is for one
	// scope, whatever the scope's own echo can name.
	UniformField string
}

var (
	baselineErrorTicket = regexp.MustCompile(`^CHAOS-[0-9]+$`)
	baselineErrorToken  = regexp.MustCompile(`^[A-Za-z0-9_]+$`)
)

// validateDeclaredBaselineError refuses a declaration that could not decide
// anything.
func validateDeclaredBaselineError(d *DeclaredBaselineError, echo []ScopeEcho) error {
	if d == nil {
		return nil
	}
	switch {
	case !baselineErrorTicket.MatchString(d.Ticket):
		return fmt.Errorf("goapiproof: declared baseline error ticket %q is not a CHAOS-<n> id", d.Ticket)
	case strings.TrimSpace(d.Reason) == "":
		return fmt.Errorf("goapiproof: declared baseline error %s states no reason", d.Ticket)
	case !baselineErrorToken.MatchString(d.MessageToken):
		return fmt.Errorf("goapiproof: declared baseline error %s: message token %q is not a plain token", d.Ticket, d.MessageToken)
	case !strings.HasPrefix(d.List, "data."):
		return fmt.Errorf("goapiproof: declared baseline error %s: list %q must start with \"data.\"", d.Ticket, d.List)
	case d.Answer != GoAnswerEmpty && d.Answer != GoAnswerNonEmpty:
		return fmt.Errorf("goapiproof: declared baseline error %s: answer %q is not one of %q, %q", d.Ticket, d.Answer, GoAnswerEmpty, GoAnswerNonEmpty)
	case d.UniformField != "" && d.Answer != GoAnswerNonEmpty:
		return fmt.Errorf("goapiproof: declared baseline error %s: a uniform field needs a non-empty answer", d.Ticket)
	case d.Answer == GoAnswerEmpty && len(echo) > 0:
		return fmt.Errorf("goapiproof: declared baseline error %s: an empty answer has no elements to echo a scope", d.Ticket)
	}
	return nil
}

// baselineMatches reports whether snapshot is exactly the declared Python
// failure for root and nothing else, or names the first way it is not.
func (d *DeclaredBaselineError) baselineMatches(root string, snapshot Snapshot) (bool, string) {
	if root == "" {
		return false, "the operation declares no response root"
	}
	if len(snapshot.Errors) != 1 {
		return false, fmt.Sprintf("baseline carried %d GraphQL errors, the declared failure is exactly one", len(snapshot.Errors))
	}
	failure := snapshot.Errors[0]
	message, _ := failure["message"].(string)
	if !strings.Contains(message, d.MessageToken) {
		return false, fmt.Sprintf("baseline error message does not carry the declared token %q", d.MessageToken)
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

// goLegUnmet returns why the candidate's answer does not have the declared
// shape, or "" when it does. echo is the request's own scope requirements.
func (d *DeclaredBaselineError) goLegUnmet(candidate Snapshot, echo []ScopeEcho) string {
	list, ok := listAt(candidate.Data, d.List)
	if !ok {
		return fmt.Sprintf("candidate answered no list at %s", d.List)
	}
	switch d.Answer {
	case GoAnswerEmpty:
		if len(list) != 0 {
			return fmt.Sprintf("%s holds %d element(s); the declared answer is empty", d.List, len(list))
		}
		return ""
	case GoAnswerNonEmpty:
		if len(list) == 0 {
			return fmt.Sprintf("%s is empty; the declared answer is non-empty, so nothing shows the scope was applied", d.List)
		}
		if detail := scopeNotReflected(candidate.Data, candidate.Data, echo); detail != "" {
			return detail
		}
		if d.UniformField != "" {
			first, ok := fieldAt(list[0], d.UniformField)
			if !ok || first == "" {
				return fmt.Sprintf("%s[0] carries no %s", d.List, d.UniformField)
			}
			for i, elem := range list {
				if got, ok := fieldAt(elem, d.UniformField); !ok || got != first {
					return fmt.Sprintf("%s[%d] does not carry the same %s as the first element: the answer is not for one scope", d.List, i, d.UniformField)
				}
			}
		}
		return ""
	}
	return fmt.Sprintf("answer shape %q is not decidable", d.Answer)
}

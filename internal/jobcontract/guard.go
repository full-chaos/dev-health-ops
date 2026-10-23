package jobcontract

import (
	"errors"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/platform/credshape"
)

// ErrCredentialShapedValue is matched by errors.Is for every refusal of a
// string that has the shape of a credential.
var ErrCredentialShapedValue = errors.New("credential-shaped value")

// ValueRefusal names where a credential-shaped string sits and the rule that
// matched it. It never holds the value, its length or its offset.
type ValueRefusal struct {
	Path string
	Rule string
}

func (refusal *ValueRefusal) Error() string {
	return refusal.Path + ": credential-shaped value (rule " + refusal.Rule + ")"
}

func (refusal *ValueRefusal) Is(target error) bool { return target == ErrCredentialShapedValue }

const arrayElementMarker = "\x00[]"

// valueGuard checks every string and object key of an envelope while the token
// walk reads it. keys[n] labels the value at nesting depth n.
type valueGuard struct {
	scanner *credshape.Scanner
	keys    [maxJSONDepth + 2]string
}

func (guard *valueGuard) checkString(depth int, text string) error {
	if rule := guard.scanner.Match(text); rule != "" {
		return guard.refuse(depth, rule)
	}
	return nil
}

func (guard *valueGuard) checkKey(depth int, key string) error {
	guard.keys[depth+1] = key
	if rule := guard.scanner.Match(key); rule != "" {
		guard.keys[depth+1] = "*"
		return guard.refuse(depth+1, rule)
	}
	return nil
}

// refuse builds the path only on refusal. A key that is not a plain lower-case
// identifier prints as "*", so free text never reaches the error.
func (guard *valueGuard) refuse(depth int, rule string) error {
	var path strings.Builder
	for index := 1; index <= depth; index++ {
		segment := guard.keys[index]
		if segment == arrayElementMarker {
			path.WriteString("[]")
			continue
		}
		if path.Len() > 0 {
			path.WriteByte('.')
		}
		if safeSegment(segment) {
			path.WriteString(segment)
		} else {
			path.WriteByte('*')
		}
	}
	if path.Len() == 0 {
		path.WriteByte('$')
	}
	return &ValueRefusal{Path: path.String(), Rule: rule}
}

func safeSegment(segment string) bool {
	if segment == "" || len(segment) > 64 || segment[0] < 'a' || segment[0] > 'z' {
		return false
	}
	for index := 1; index < len(segment); index++ {
		char := segment[index]
		if (char < 'a' || char > 'z') && (char < '0' || char > '9') && char != '_' {
			return false
		}
	}
	return true
}

// decodeGuarded is decodeStrict plus the value guard, for an envelope that
// enters or leaves the queue. The merge-base loader (decodeLenient) is not
// guarded: it reads history, not queue input.
func decodeGuarded(data []byte, maxBytes int, destination any) error {
	scanner, err := credshape.Default()
	if err != nil {
		return errors.New("credential guard is unavailable")
	}
	guard := valueGuard{scanner: scanner}
	return decodeJSON(data, maxBytes, destination, true, &guard)
}

func asValueRefusal(err error) (*ValueRefusal, bool) {
	var refusal *ValueRefusal
	if errors.As(err, &refusal) {
		return refusal, true
	}
	return nil, false
}

package venueoracle

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math/big"
	"net/url"
	"regexp"
	"sort"
	"strings"
)

// A golden holds no token value. The recorder projects every JWT in a
// recorded body or header to its decoded claims as tagged leaves, Diff applies
// the same projection to the Go side, a per-golden Scrub (GoldenSpec.Scrub)
// turns any other token field into a typed placeholder, and the recorder
// refuses a candidate that still holds a token shape (TokenShapesIn).

// tokenShape is one shape of credential a golden must never hold.
type tokenShape struct {
	name string
	re   *regexp.Regexp
}

// jwtShape is a JWT: a base64url JSON header and payload, then a signature.
var jwtShape = regexp.MustCompile(`eyJ[A-Za-z0-9_-]{4,}\.eyJ[A-Za-z0-9_-]{4,}\.[A-Za-z0-9_-]*`)

// undecodableJWT starts the projection of a token whose header or payload is
// not a JSON object: a measurement that could not be made. The recorder
// refuses a candidate holding it and Diff fails on it, because two different
// undecodable tokens would otherwise compare equal.
const undecodableJWT = "<jwt|undecodable"

// tokenShapes are the shapes the recorder refuses and the gate test walks every
// golden for. Each is anchored on a prefix or structure that a stable id or a
// route name never has.
var tokenShapes = []tokenShape{
	{"jwt", jwtShape},
	{"github-token", regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{36,}`)},
	{"github-fine-grained-pat", regexp.MustCompile(`github_pat_[A-Za-z0-9_]{22,}`)},
	{"gitlab-pat", regexp.MustCompile(`glpat-[A-Za-z0-9_-]{20,}`)},
	{"slack-token", regexp.MustCompile(`xox[abprs]-[A-Za-z0-9-]{10,}`)},
	{"stripe-key", regexp.MustCompile(`\b[sr]k_(live|test)_[A-Za-z0-9]{16,}`)},
	{"aws-access-key", regexp.MustCompile(`\b(AKIA|ASIA)[A-Z0-9]{16}\b`)},
	{"google-api-key", regexp.MustCompile(`AIza[A-Za-z0-9_-]{35}`)},
	{"private-key-block", regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`)},
	{"anthropic-key", regexp.MustCompile(`sk-ant-[A-Za-z0-9_-]{20,}`)},
	{"openai-key", regexp.MustCompile(`\bsk-(proj-)?[A-Za-z0-9_-]{32,}`)},
	{"linear-key", regexp.MustCompile(`lin_api_[A-Za-z0-9]{30,}`)},
	{"authorization-credential", regexp.MustCompile(`(?i)\b(bearer|basic)\s+[A-Za-z0-9._~+/=-]{20,}`)},
}

// TokenShapeNames are the names of the shapes, in the order tested.
func TokenShapeNames() []string {
	names := make([]string, len(tokenShapes))
	for i, shape := range tokenShapes {
		names[i] = shape.name
	}
	return names
}

// TokenShapesIn is the sorted names of the token shapes text holds.
func TokenShapesIn(text string) []string {
	var found []string
	for _, shape := range tokenShapes {
		if shape.re.MatchString(text) {
			found = append(found, shape.name)
		}
	}
	sort.Strings(found)
	return found
}

// volatileTokenClaims keep their name and type in a projection but not their
// value: two processes minting the same caller's token differ in them.
var volatileTokenClaims = map[string]bool{"iat": true, "exp": true, "nbf": true, "jti": true}

// ProjectTokens replaces every JWT in text with its decoded claims as tagged
// leaves, "<jwt|name=type:value|...>" sorted by name (the header's alg is the
// claim "alg" of the header, written "hdr.alg"). A volatile claim keeps its
// name and type and loses its value; the signature is dropped; a value is
// percent-encoded so a quote or separator cannot break the surrounding JSON.
// The result holds no token shape and projecting it again changes nothing.
func ProjectTokens(text string) string {
	return jwtShape.ReplaceAllStringFunc(text, projectJWT)
}

func projectJWT(token string) string {
	parts := strings.SplitN(token, ".", 3)
	header, ok := decodeSegment(parts[0])
	if !ok {
		return undecodableJWT + ":header>"
	}
	payload, ok := decodeSegment(parts[1])
	if !ok {
		return undecodableJWT + ":payload>"
	}
	leaves := map[string]string{}
	for name, value := range header {
		leaves["hdr."+name] = leaf(name, value)
	}
	for name, value := range payload {
		leaves[name] = leaf(name, value)
	}
	// The lifetime is contract and the claims it is made of are volatile, so
	// the differences are leaves of their own.
	for _, pair := range [][2]string{{"exp", "iat"}, {"nbf", "iat"}} {
		if span, ok := claimDifference(payload, pair[0], pair[1]); ok {
			leaves[pair[0]+"-"+pair[1]] = "number:" + url.QueryEscape(span)
		}
	}
	names := make([]string, 0, len(leaves))
	for name := range leaves {
		names = append(names, name)
	}
	sort.Strings(names)
	var out strings.Builder
	out.WriteString("<jwt")
	for _, name := range names {
		fmt.Fprintf(&out, "|%s=%s", url.QueryEscape(name), leaves[name])
	}
	out.WriteString(">")
	return out.String()
}

// claimDifference is a-b as an exact rational when both claims are numbers.
func claimDifference(payload map[string]any, a, b string) (string, bool) {
	left, lok := payload[a].(json.Number)
	right, rok := payload[b].(json.Number)
	if !lok || !rok {
		return "", false
	}
	x, xok := new(big.Rat).SetString(left.String())
	y, yok := new(big.Rat).SetString(right.String())
	if !xok || !yok {
		return "", false
	}
	return new(big.Rat).Sub(x, y).RatString(), true
}

// decodeSegment decodes one JWT segment as a JSON object, keeping every
// number as its literal: a claim past 2^53 must not round, and 1 is not 1.0.
func decodeSegment(segment string) (map[string]any, bool) {
	raw, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(segment, "="))
	if err != nil {
		return nil, false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var claims map[string]any
	if err := decoder.Decode(&claims); err != nil || claims == nil {
		return nil, false
	}
	return claims, true
}

// leaf is "type:value" for one claim.
func leaf(name string, value any) string {
	kind := "string"
	switch value.(type) {
	case nil:
		kind = "null"
	case bool:
		kind = "boolean"
	case json.Number:
		kind = "number"
	case []any:
		kind = "array"
	case map[string]any:
		kind = "object"
	}
	if volatileTokenClaims[name] {
		return kind + ":"
	}
	var text string
	if s, ok := value.(string); ok {
		text = s
	} else {
		raw, _ := json.Marshal(value)
		text = string(raw)
	}
	return kind + ":" + url.QueryEscape(text)
}

// project is what the golden stores and compares of one piece of text: its
// JWTs projected, then the spec's Scrub.
func (g *Golden) project(text string) string {
	text = ProjectTokens(text)
	if g.spec.Scrub != nil {
		text = g.spec.Scrub(text)
	}
	return text
}

// projectResponse is response with every header value and the body projected.
func (g *Golden) projectResponse(response Response) Response {
	out := clone(response)
	out.Body = g.project(out.Body)
	for name, value := range out.Headers {
		out.Headers[name] = g.project(value)
	}
	return out
}

// tokenShapeErr is an error naming the token shapes the golden's recorded
// text holds: a golden stores none.
func tokenShapeErr(path string, raw []byte) error {
	if strings.Contains(string(raw), undecodableJWT) {
		return fmt.Errorf("golden %s holds a token whose header or payload could not be decoded: the projection cannot tell it from any other undecodable token, so it is not recorded", path)
	}
	if found := TokenShapesIn(string(raw)); len(found) > 0 {
		return fmt.Errorf("golden %s holds a token shape (%s): a golden stores no token value, because a stored credential is a leaked credential; turn it into a typed placeholder with GoldenSpec.Scrub instead of recording it", path, strings.Join(found, ", "))
	}
	return nil
}

// undecodableErr is an error when a projected response holds a token the
// projection could not decode.
func undecodableErr(request Request, response Response) error {
	values := []string{response.Body}
	for _, value := range response.Headers {
		values = append(values, value)
	}
	for _, value := range values {
		if strings.Contains(value, undecodableJWT) {
			return fmt.Errorf("%s: a response holds a token whose header or payload is not a JSON object, so its claims cannot be compared", request.Name)
		}
	}
	return nil
}

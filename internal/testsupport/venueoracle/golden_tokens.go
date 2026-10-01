package venueoracle

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
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
	// match, when set, decides instead of re (the JWT shape needs a decode).
	match func(text string) bool
}

// jwtCandidate is the text that may be a JWT or JWE: three to five dotted
// base64url segments. Whether it is one is decided by its first segment
// (jwtParts), not by a prefix: a header with legal leading whitespace does not
// begin "eyJ".
var jwtCandidate = regexp.MustCompile(`[A-Za-z0-9_-]{2,}(?:\.[A-Za-z0-9_-]*){2,4}`)

// jwtParts is the dotted segments of candidate and whether the first is a JWT
// header: it begins "eyJ" or decodes to a JSON object.
func jwtParts(candidate string) ([]string, bool) {
	parts := strings.Split(candidate, ".")
	if strings.HasPrefix(parts[0], "eyJ") {
		return parts, true
	}
	_, ok := decodeSegment(parts[0])
	return parts, ok
}

// hasJWT reports whether text holds a JWT or JWE.
func hasJWT(text string) bool {
	for _, candidate := range jwtCandidate.FindAllString(text, -1) {
		if _, ok := jwtParts(candidate); ok {
			return true
		}
	}
	return false
}

// undecodableJWT starts the projection of a token whose header or payload is
// not a JSON object: a measurement that could not be made. The recorder
// refuses a candidate holding it and Diff fails on it, because two different
// undecodable tokens would otherwise compare equal.
const undecodableJWT = "<jwt|undecodable"

// tokenShapes are the shapes the recorder refuses and the gate test walks every
// golden for. Each is anchored on a prefix or structure that a stable id or a
// route name never has.
var tokenShapes []tokenShape

// init fills the list: the encoded-credential shape reads the list itself.
func init() {
	tokenShapes = []tokenShape{
		{name: "jwt", match: hasJWT},
		{name: "github-token", re: regexp.MustCompile(`\bgh[pousr]_[A-Za-z0-9]{36,}`)},
		{name: "github-fine-grained-pat", re: regexp.MustCompile(`github_pat_[A-Za-z0-9_]{22,}`)},
		{name: "gitlab-pat", re: regexp.MustCompile(`glpat-[A-Za-z0-9_-]{20,}`)},
		{name: "slack-token", re: regexp.MustCompile(`xox[abprs]-[A-Za-z0-9-]{10,}`)},
		{name: "stripe-key", re: regexp.MustCompile(`\b[sr]k_(live|test)_[A-Za-z0-9]{16,}`)},
		{name: "aws-access-key", re: regexp.MustCompile(`\b(AKIA|ASIA)[A-Z0-9]{16}\b`)},
		{name: "google-api-key", re: regexp.MustCompile(`AIza[A-Za-z0-9_-]{35}`)},
		{name: "private-key-block", re: regexp.MustCompile(`-----BEGIN [A-Z ]*PRIVATE KEY-----`)},
		{name: "anthropic-key", re: regexp.MustCompile(`sk-ant-[A-Za-z0-9_-]{20,}`)},
		{name: "openai-key", re: regexp.MustCompile(`\bsk-(proj-)?[A-Za-z0-9_-]{32,}`)},
		{name: "linear-key", re: regexp.MustCompile(`lin_api_[A-Za-z0-9]{30,}`)},
		{name: "customer-push-token", re: regexp.MustCompile(`fcpush_[A-Za-z0-9_-]{16,}`)},
		{name: "authorization-credential", match: hasAuthorizationCredential},
		{name: "encoded-credential", match: func(text string) bool { return hasEncodedCredential(text, 2) }},
	}
}

// maxPackDepth bounds how deep a body packed in a body is unpacked.
const maxPackDepth = 8

var authorizationScheme = regexp.MustCompile(`(?i)\b(?:bearer|basic)\s+([A-Za-z0-9._~+/=-]{8,})`)

// hasAuthorizationCredential reports a Bearer or Basic credential: long, or
// short with a digit in it. A short word after "Basic" in prose has neither.
func hasAuthorizationCredential(text string) bool {
	for _, match := range authorizationScheme.FindAllStringSubmatch(text, -1) {
		if len(match[1]) >= 20 || strings.ContainsAny(match[1], "0123456789") {
			return true
		}
	}
	return false
}

// encodedRun is a base64 or base64url run long enough to carry a credential.
var encodedRun = regexp.MustCompile(`[A-Za-z0-9+/_-]{24,}={0,2}`)

// hasEncodedCredential reports a token shape inside a base64 or base64url run
// of text, to depth encodings deep: a token a Scrub-less golden holds encoded
// is still a token.
func hasEncodedCredential(text string, depth int) bool {
	if depth <= 0 {
		return false
	}
	for _, run := range encodedRun.FindAllString(text, 2000) {
		run = strings.TrimRight(run, "=")
		for _, encoding := range []*base64.Encoding{base64.RawStdEncoding, base64.RawURLEncoding} {
			raw, err := encoding.DecodeString(run)
			if err != nil || len(raw) == 0 {
				continue
			}
			decoded := string(raw)
			for _, shape := range tokenShapes {
				if shape.name == "encoded-credential" {
					continue
				}
				if shape.match != nil && shape.match(decoded) || shape.re != nil && shape.re.MatchString(decoded) {
					return true
				}
			}
			if hasEncodedCredential(decoded, depth-1) {
				return true
			}
		}
	}
	return false
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
		if shape.match != nil && shape.match(text) || shape.re != nil && shape.re.MatchString(text) {
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
	return jwtCandidate.ReplaceAllStringFunc(text, func(candidate string) string {
		parts, ok := jwtParts(candidate)
		if !ok {
			return candidate
		}
		if len(parts) == 5 {
			// A compact JWE: its claims are encrypted, so nothing can be compared.
			return undecodableJWT + ":encrypted>"
		}
		// A fourth part is text after the token (a trailing dot, a word).
		rest := ""
		if len(parts) > 3 {
			rest = "." + strings.Join(parts[3:], ".")
		}
		return projectJWT(parts[:3]) + rest
	})
}

func projectJWT(parts []string) string {
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
	// Nothing may follow the object: bytes the projection cannot see.
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
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
// JWTs projected, then the spec's Scrub. A packed body (PackBody) is unpacked,
// projected and packed again, so a token inside it is projected too; one that
// does not unpack is an error.
func (g *Golden) project(text string) (string, error) {
	if strings.HasPrefix(text, packedPrefix) {
		raw, err := unpackBody(text)
		if err != nil {
			return "", err
		}
		projected, err := g.project(raw)
		if err != nil || projected == raw {
			return text, err
		}
		return PackBody([]byte(projected)), nil
	}
	text = ProjectTokens(text)
	if g.spec.Scrub != nil {
		text = g.spec.Scrub(text)
	}
	return text, nil
}

// projectResponse is response with every header value and the body projected.
func (g *Golden) projectResponse(response Response) (Response, error) {
	out := clone(response)
	var err error
	if out.Body, err = g.project(out.Body); err != nil {
		return out, err
	}
	for name, value := range out.Headers {
		if out.Headers[name], err = g.project(value); err != nil {
			return out, err
		}
	}
	return out, nil
}

// tokenShapeErr is an error naming the token shapes the golden's recorded
// text holds: a golden stores none.
func tokenShapeErr(path string, raw []byte) error {
	texts := []string{string(raw)}
	// A packed body is opaque to a scan of the file: look inside every one.
	var file goldenFile
	if err := json.Unmarshal(raw, &file); err != nil {
		return fmt.Errorf("golden %s is not a golden file: %w", path, err)
	}
	var bodies []string
	for _, request := range file.Requests {
		bodies = append(bodies, request.Body)
		for _, value := range request.Headers {
			bodies = append(bodies, value)
		}
	}
	for _, rows := range file.Rows {
		bodies = append(bodies, rows.Rows)
	}
	for _, body := range bodies {
		// Unpack as deep as it goes: a packed body may hold a packed body.
		for depth := 0; strings.HasPrefix(body, packedPrefix); depth++ {
			if depth >= maxPackDepth {
				return fmt.Errorf("golden %s holds a body packed more than %d deep, so it cannot be checked for tokens", path, maxPackDepth)
			}
			unpacked, err := unpackBody(body)
			if err != nil {
				return fmt.Errorf("golden %s holds a packed body that does not unpack, so it cannot be checked for tokens: %w", path, err)
			}
			texts = append(texts, unpacked)
			body = unpacked
		}
	}
	for _, text := range texts {
		if strings.Contains(text, undecodableJWT) {
			return fmt.Errorf("golden %s holds a token whose header or payload could not be decoded: the projection cannot tell it from any other undecodable token, so it is not recorded", path)
		}
	}
	for _, text := range texts {
		if found := TokenShapesIn(text); len(found) > 0 {
			return fmt.Errorf("golden %s holds a token shape (%s): a golden stores no token value, because a stored credential is a leaked credential; turn it into a typed placeholder with GoldenSpec.Scrub instead of recording it", path, strings.Join(found, ", "))
		}
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

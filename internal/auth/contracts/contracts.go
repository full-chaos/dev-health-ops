// Package contracts validates documents against the Auth Control Plane v1
// wire contracts and provides the Go types for them.
//
// THE PACKAGE PATH CARRIES NO VERSION AND THE WIRE FORMAT DOES -- that
// asymmetry is deliberate (chris, 2026-09-02: "Not sure auth versioning is a
// thing"). The on-disk contracts stay under contracts/auth/v1/ and every
// document still declares its own schema_version, because the WIRE is the
// thing that has to stay compatible with deployed readers. The Go package is
// an internal implementation of whatever the current wire contract is, and
// versioning it too would mean carrying a second, redundant axis that can
// disagree with the first. Do not "restore" a /v1 segment here to match the
// directory: the directory is versioned for a reason this package is not.
//
// The JSON Schema documents under contracts/auth/v1/jsonschema/ are the
// SOURCE OF TRUTH for the wire format; the Go types here are an
// implementation of that contract, never the other way round. The same
// schemas and the same golden fixtures are validated by the Python client
// (src/dev_health_ops/authclient) and the TypeScript client in
// dev-health-web. A language that keeps its own copy of the corpus is the
// drift class the cross-language goldens exist to catch.
//
// # Library choice, and why the obvious pick is wrong
//
// Validation uses github.com/google/jsonschema-go, whose doc.go states it
// supports "draft 2020-12 and draft-07" and that other drafts are "not
// supported". These schemas declare draft 2020-12.
//
// github.com/xeipuuv/gojsonschema must NEVER be used here: it maxes out at
// Draft 7 (its draft.go), so pointed at a 2020-12 schema it silently accepts
// constructs it cannot interpret -- reproducing the very defect that
// full-document validation exists to close. github.com/invopop/jsonschema is
// a schema GENERATOR, not a validator. Both are already present in the acr
// module's graph, so picking by autocomplete lands on the wrong one; this
// comment is here because the choice is not obvious from the import list.
// The version is pinned to v0.4.3, byte-identical to acr's pin in its own
// go.mod, so the two Go validators in this fleet cannot drift apart.
//
// # Two deviations from the spec that shape every schema in this directory
//
// google/jsonschema-go documents both under a "Deviations from the
// specification" heading in its doc.go, and both are load-bearing here:
//
//  1. "format" is recorded and then IGNORED during validation -- it does not
//     even produce an annotation. The Python and TypeScript validators DO
//     assert it. So a constraint expressed only as "format" is enforced in
//     two languages out of three. Every such constraint in these schemas
//     therefore carries a "pattern" beside it as the cross-language floor,
//     and TestEveryFormatConstraintHasAPatternBeside enforces that rule
//     rather than leaving it to review.
//  2. Regular expressions are Go's regexp (RE2), which differs from ECMA 262
//     -- most significantly in having no back-references, and no lookaround.
//     A pattern using either means something different in Go than in the
//     other two runtimes, silently and on the permissive side.
//     TestEveryPatternStaysInTheCommonRegexSubset enforces the intersection.
package contracts

import (
	"encoding/json"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/google/jsonschema-go/jsonschema"
)

// contractsSubpath is the repo-relative home of the v1 wire schemas.
var contractsSubpath = filepath.Join("contracts", "auth", "v1")

// RepoRoot returns the ops repo root that owns the contracts directory,
// searching upward from dir.
//
// It returns an error rather than falling back to a default, because
// validating against the wrong contracts directory reports success while
// checking a different contract -- a silent pass, which is the failure mode
// this whole package exists to prevent.
func RepoRoot(dir string) (string, error) {
	abs, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("resolving %s: %w", dir, err)
	}
	for {
		if info, statErr := os.Stat(filepath.Join(abs, contractsSubpath, "jsonschema")); statErr == nil && info.IsDir() {
			return abs, nil
		}
		parent := filepath.Dir(abs)
		if parent == abs {
			return "", fmt.Errorf(
				"no ancestor of %s contains %s -- refusing to guess a repo root",
				dir, filepath.Join(contractsSubpath, "jsonschema"),
			)
		}
		abs = parent
	}
}

// ContractsDir returns contracts/auth/v1 beneath root.
func ContractsDir(root string) string { return filepath.Join(root, contractsSubpath) }

// SchemaPath returns the schema file for one wire surface, e.g. "principal.v1".
func SchemaPath(root, surface string) string {
	return filepath.Join(ContractsDir(root), "jsonschema", surface+".schema.json")
}

type validatorCache struct {
	mu    sync.Mutex
	byKey map[string]*jsonschema.Resolved
}

var cache = validatorCache{byKey: map[string]*jsonschema.Resolved{}}

// wireIDPrefix is the $id namespace every contract schema declares. A $ref
// leaving one schema for another resolves under it.
const wireIDPrefix = "https://fullchaos.dev/contracts/auth/v1/"

// localLoader resolves a cross-schema $ref to the file beneath root and
// refuses everything else. It never fetches over the network: a $ref that
// escapes the contracts namespace is a hard error, because following it would
// validate against a document this tree does not own.
func localLoader(root string) jsonschema.Loader {
	return func(uri *url.URL) (*jsonschema.Schema, error) {
		full := uri.String()
		rel, ok := strings.CutPrefix(full, wireIDPrefix)
		if !ok || rel == "" || strings.Contains(rel, "..") {
			return nil, fmt.Errorf("refusing $ref %q: only %s... is loadable", full, wireIDPrefix)
		}
		path := filepath.Join(ContractsDir(root), filepath.FromSlash(rel))
		if err := requireInsideContracts(root, path); err != nil {
			return nil, err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("reading referenced schema %s: %w", path, err)
		}
		var schema jsonschema.Schema
		if err := json.Unmarshal(raw, &schema); err != nil {
			return nil, fmt.Errorf("parsing referenced schema %s: %w", path, err)
		}
		return &schema, nil
	}
}

// requireInsideContracts refuses a path whose real location, after following
// symlinks, is outside the contracts directory. The textual namespace check
// cannot see that an in-tree file is a link to somewhere else.
func requireInsideContracts(root, path string) error {
	base, err := filepath.EvalSymlinks(ContractsDir(root))
	if err != nil {
		return fmt.Errorf("resolving contracts directory: %w", err)
	}
	real, err := filepath.EvalSymlinks(path)
	if err != nil {
		return fmt.Errorf("resolving referenced schema %s: %w", path, err)
	}
	if real != base && !strings.HasPrefix(real, base+string(filepath.Separator)) {
		return fmt.Errorf("refusing referenced schema %s: it resolves to %s, outside %s", path, real, base)
	}
	return nil
}

// Validator returns the resolved Draft 2020-12 validator for one wire surface.
//
// A schema that cannot be read, parsed or resolved is a hard error, never a
// skip: "could not validate" and "validated cleanly" are indistinguishable
// from a caller that only checks whether an error came back from Validate.
func Validator(root, surface string) (*jsonschema.Resolved, error) {
	path := SchemaPath(root, surface)

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if resolved, ok := cache.byKey[path]; ok {
		return resolved, nil
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("reading wire schema for %q: %w", surface, err)
	}
	var schema jsonschema.Schema
	if err := json.Unmarshal(raw, &schema); err != nil {
		return nil, fmt.Errorf("parsing wire schema %s: %w", path, err)
	}
	resolved, err := schema.Resolve(&jsonschema.ResolveOptions{Loader: localLoader(root)})
	if err != nil {
		return nil, fmt.Errorf("resolving wire schema %s: %w", path, err)
	}
	cache.byKey[path] = resolved
	return resolved, nil
}

// Validate reports whether document satisfies the named wire surface.
//
// document must be a decoded JSON value (map[string]any and friends), not a
// Go struct: validating the struct would validate the Go type's rendering of
// the contract rather than the bytes actually on the wire, which is the
// tautology these contracts exist to avoid.
//
// NOTE ON ERROR SHAPE: google/jsonschema-go returns ONE error and stops at
// the first violation; it does not enumerate every violation the way the
// Python and TypeScript validators do, and it reports a JSON Pointer into
// the SCHEMA rather than into the instance. Callers that need every
// violation, or an instance location, must not infer either from this error.
// The golden tests handle that asymmetry explicitly rather than pretending
// it is not there.
func Validate(root, surface string, document any) error {
	resolved, err := Validator(root, surface)
	if err != nil {
		return err
	}
	if err := resolved.Validate(document); err != nil {
		return fmt.Errorf("%s: %w", surface, err)
	}
	return nil
}

// RefuseDecimalForm refuses a raw JSON number token written in decimal or
// exponent form: any token containing '.', 'e' or 'E', even when its value is
// integral (1.0, 1e0).
//
// It decides on the TOKEN TEXT, never on a float64 parse. By the time a float64
// exists the token has been rounded: 9007199254740990.1 and
// 1.0000000000000000001 are indistinguishable from the integers they round to,
// so no magnitude bound can be sound. An integer token is exact and never
// reaches this refusal. The Python twin is
// authclient.contracts.refuse_decimal_form.
//
// Wire-integer TYPE for a field decoded by encoding/json into an
// integer-typed Go field needs no helper: the decoder already refuses 403.0,
// which is how ErrorEnvelope.Status and RetryAfterSeconds are guarded. This is
// for custom unmarshalers that hold the raw token (Revision).
func RefuseDecimalForm(token string) error {
	if strings.ContainsAny(token, ".eE") {
		return fmt.Errorf(
			"%s is in decimal or exponent form; refused because a float64 reader has "+
				"already rounded such a token (9007199254740990.1 reads as 9007199254740990), "+
				"so it is indistinguishable from the integer it rounds to. "+
				"Send an integer token, which stays exact", token)
	}
	return nil
}

// ParseWireTimestamp is the ONE timestamp rule for every timestamp member of
// the event surfaces (envelope created_at, payload.change.expires_at, audit
// occurred_at). A value passes when it names a real calendar day (the pattern
// bounds components, not the calendar: 2026-02-30 is lexically valid) and its
// UTC instant lies in 0001-01-01..9999-12-31 (the pattern bounds the LOCAL
// fields, and an offset can carry the instant out of range: 0001-01-01T00:00:00+23:59
// is UTC year 0000). Python's twin is authclient.contracts.parse_rfc3339; both
// replay the same timestamp_vectors from the provisioning manifest.
func ParseWireTimestamp(surface, field, raw string) (time.Time, error) {
	parsed, err := time.Parse(time.RFC3339, raw)
	if err != nil {
		return time.Time{}, fmt.Errorf("%s: /%s = %q is not a valid RFC 3339 instant: %w", surface, field, raw, err)
	}
	if err := checkInstantInRange(surface, field, parsed); err != nil {
		return time.Time{}, err
	}
	return parsed, nil
}

func checkInstantInRange(surface, field string, moment time.Time) error {
	if year := moment.UTC().Year(); year < 1 || year > 9999 {
		return fmt.Errorf("%s: /%s = %s normalises to UTC year %d, outside 0001-01-01..9999-12-31 "+
			"(not a valid RFC 3339 instant for this contract): the pattern bounds the local fields, "+
			"the offset carries the instant past the bound", surface, field, moment.Format(time.RFC3339Nano), year)
	}
	return nil
}

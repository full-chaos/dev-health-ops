package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// errorFixtureManifest mirrors contracts/auth/v1/examples/error/manifest.json.
//
// A separate type from fixtureManifest rather than a shared one, because this
// corpus's reject_by_client entries carry an http_status the principal corpus
// has no use for. Widening the shared struct would put a field on the
// principal manifest that is always zero, which reads as "not set" rather than
// "not applicable" to the next person.
type errorFixtureManifest struct {
	Accept []struct {
		File string `json:"file"`
	} `json:"accept"`
	Reject []struct {
		File                   string `json:"file"`
		ExpectInstanceLocation string `json:"expect_instance_location"`
		ExpectKeyword          string `json:"expect_keyword"`
		Why                    string `json:"why"`
	} `json:"reject"`
	// RejectByClient documents are VALID against the schema and every language
	// client MUST still refuse them. HTTPStatus is the status of the response
	// line the document arrived on -- the schema never sees it, which is
	// exactly why these checks cannot live in the schema.
	RejectByClient []struct {
		File       string `json:"file"`
		HTTPStatus int    `json:"http_status"`
		Why        string `json:"why"`
	} `json:"reject_by_client"`
}

// goMessageForErrorKeyword extends goMessageForKeyword with the keywords this
// corpus asserts and the principal corpus does not.
//
// Every token here was READ OFF the library rather than guessed: a temporary
// probe validated one fixture per keyword and printed the real error string.
// Guessing would have been the natural move and would have been wrong at least
// once -- "not" is reported as `not: validated against <anonymous schema>`,
// which contains the keyword only by luck of phrasing, and `type` is reported
// with the offending value first.
var goMessageForErrorKeyword = map[string]string{
	"maximum": "maximum:",
	"not":     "not:",
	"type":    "type:",
}

// errorTokenFor resolves a manifest keyword to the token Go's validator emits,
// consulting the shared table first so the two cannot disagree about a keyword
// they both cover.
func errorTokenFor(keyword string) (string, bool) {
	if token, ok := goMessageForKeyword[keyword]; ok {
		return token, true
	}
	token, ok := goMessageForErrorKeyword[keyword]
	return token, ok
}

const (
	errorFixtureDir          = "contracts/auth/v1/examples/error"
	errorFixtureManifestPath = "contracts/auth/v1/examples/error/manifest.json"
)

var errorManifestBasename = filepath.Base(errorFixtureManifestPath)

func errorDir(t *testing.T) string {
	t.Helper()
	return filepath.Join(testRoot(t), errorFixtureDir)
}

func loadErrorManifest(t *testing.T) errorFixtureManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), errorFixtureManifestPath))
	if err != nil {
		t.Fatalf("reading error fixture manifest: %v", err)
	}
	var manifest errorFixtureManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parsing error fixture manifest: %v", err)
	}
	return manifest
}

func loadErrorFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(errorDir(t), name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return raw
}

// errorNow is a reference instant later than every fixture's occurred_at
// except the deliberately far-future one, so the skew check is deterministic
// rather than depending on when the suite runs. Without it the corpus would
// start failing on its own once real time passed 2027.
var errorNow = time.Date(2026, 9, 3, 6, 0, 0, 0, time.UTC)

func TestErrorCorpusIsNotEmptyInEitherDirection(t *testing.T) {
	// A table-driven test over an empty slice reports zero failures and the
	// package still prints "ok" -- indistinguishable from a suite that ran.
	manifest := loadErrorManifest(t)
	if len(manifest.Accept) == 0 {
		t.Error("manifest declares no accept fixtures")
	}
	if len(manifest.Reject) == 0 {
		t.Error("manifest declares no reject fixtures")
	}
	if len(manifest.RejectByClient) == 0 {
		t.Error("manifest declares no client-enforced fixtures")
	}
}

func TestEveryErrorFixtureOnDiskIsClaimedByTheManifest(t *testing.T) {
	manifest := loadErrorManifest(t)
	entries, err := os.ReadDir(errorDir(t))
	if err != nil {
		t.Fatalf("reading fixture dir: %v", err)
	}
	onDisk := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == errorManifestBasename || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		onDisk[entry.Name()] = true
	}
	claimed := map[string]bool{}
	for _, a := range manifest.Accept {
		claimed[a.File] = true
	}
	for _, r := range manifest.Reject {
		claimed[r.File] = true
	}
	for _, r := range manifest.RejectByClient {
		claimed[r.File] = true
	}
	var unclaimed, missing []string
	for name := range onDisk {
		if !claimed[name] {
			unclaimed = append(unclaimed, name)
		}
	}
	for name := range claimed {
		if !onDisk[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(unclaimed)
	sort.Strings(missing)
	if len(unclaimed) > 0 {
		t.Errorf("fixture files no runner opens: %v", unclaimed)
	}
	if len(missing) > 0 {
		t.Errorf("manifest claims absent files: %v", missing)
	}
}

func TestEveryErrorManifestKeywordHasAGoMessageToken(t *testing.T) {
	// Totality. An unmapped keyword must FAIL rather than be skipped: a skip
	// means a reject fixture whose rule is checked in Python and ajv and
	// silently unchecked in Go, while all three suites stay green.
	for _, entry := range loadErrorManifest(t).Reject {
		if _, ok := errorTokenFor(entry.ExpectKeyword); !ok {
			t.Errorf("no Go message token registered for keyword %q (fixture %s)",
				entry.ExpectKeyword, entry.File)
		}
	}
}

func TestAcceptedErrorFixturesValidate(t *testing.T) {
	// The positive controls. A validator that is too strict passes every
	// rejection test while breaking every real caller.
	root := testRoot(t)
	for _, entry := range loadErrorManifest(t).Accept {
		t.Run(entry.File, func(t *testing.T) {
			var document any
			if err := json.Unmarshal(loadErrorFixture(t, entry.File), &document); err != nil {
				t.Fatalf("fixture is not valid JSON: %v", err)
			}
			if err := Validate(root, ErrorSurface, document); err != nil {
				t.Fatalf("accept fixture was rejected: %v", err)
			}
		})
	}
}

func TestRejectedErrorFixturesAreRejectedByTheDeclaredRule(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadErrorManifest(t).Reject {
		t.Run(entry.File, func(t *testing.T) {
			var document any
			if err := json.Unmarshal(loadErrorFixture(t, entry.File), &document); err != nil {
				t.Fatalf("fixture is not valid JSON: %v", err)
			}
			err := Validate(root, ErrorSurface, document)
			if err == nil {
				t.Fatalf("validated cleanly; expected rejection by %q at %q (%s)",
					entry.ExpectKeyword, entry.ExpectInstanceLocation, entry.Why)
			}
			token, ok := errorTokenFor(entry.ExpectKeyword)
			if !ok {
				t.Fatalf("no Go message token registered for keyword %q", entry.ExpectKeyword)
			}
			if !strings.Contains(err.Error(), token) {
				t.Fatalf("rejected, but not by %q. token %q absent from: %v",
					entry.ExpectKeyword, token, err)
			}
		})
	}
}

func TestClientEnforcedErrorFixturesValidateButAreRefused(t *testing.T) {
	// The category's whole point, in one assertion pair. The first half proves
	// the SCHEMA does not catch these, which is what makes the client check
	// load-bearing rather than belt-and-braces; the second proves the client
	// does.
	root := testRoot(t)
	for _, entry := range loadErrorManifest(t).RejectByClient {
		t.Run(entry.File, func(t *testing.T) {
			raw := loadErrorFixture(t, entry.File)
			var document any
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatalf("fixture is not valid JSON: %v", err)
			}
			if err := Validate(root, ErrorSurface, document); err != nil {
				t.Fatalf("client-enforced fixture must VALIDATE, got: %v", err)
			}
			if _, err := ParseErrorEnvelope(root, raw, entry.HTTPStatus, errorNow); err == nil {
				t.Fatalf("client accepted a document it must refuse (%s)", entry.Why)
			}
		})
	}
}

func TestTransientStatusesMatchTheSchema(t *testing.T) {
	// Two sources of truth for one fact is the drift this programme exists to
	// remove. The duplication is allowed only because this test makes it
	// detectable -- so read the schema rather than restating it, or the
	// assertion compares a literal against itself.
	raw, err := os.ReadFile(filepath.Join(testRoot(t),
		"contracts/auth/v1/jsonschema/error.v1.schema.json"))
	if err != nil {
		t.Fatalf("reading schema: %v", err)
	}
	var schema struct {
		If struct {
			Properties struct {
				Status struct {
					Enum []int `json:"enum"`
				} `json:"status"`
			} `json:"properties"`
		} `json:"if"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatalf("parsing schema: %v", err)
	}
	fromSchema := schema.If.Properties.Status.Enum
	if len(fromSchema) == 0 {
		t.Fatal("read no statuses out of the schema's if branch; the assertion would be vacuous")
	}
	if len(fromSchema) != len(transientStatuses) {
		t.Fatalf("schema says %v, client says %v", fromSchema, transientStatuses)
	}
	for _, status := range fromSchema {
		if !transientStatuses[status] {
			t.Errorf("schema marks %d transient, client does not", status)
		}
	}
}

func TestAMatchingStatusIsAccepted(t *testing.T) {
	// The accepting half of the status-agreement check. Without it a client
	// that refused EVERY envelope would pass all three reject_by_client cases.
	root := testRoot(t)
	envelope, err := ParseErrorEnvelope(root,
		loadErrorFixture(t, "valid-403-grant-absent.json"), 403, errorNow)
	if err != nil {
		t.Fatalf("a matching status was refused: %v", err)
	}
	if envelope.Status != 403 || envelope.ReasonCode != "grant_absent" {
		t.Fatalf("parsed wrong: %+v", envelope)
	}
	if envelope.RetryAfterSeconds != nil {
		t.Errorf("non-transient envelope carries retry guidance: %v", *envelope.RetryAfterSeconds)
	}
	if envelope.IsTransient() {
		t.Error("403 reported as transient")
	}
}

func TestATransientEnvelopeReportsItselfTransient(t *testing.T) {
	root := testRoot(t)
	envelope, err := ParseErrorEnvelope(root,
		loadErrorFixture(t, "valid-429-with-retry.json"), 429, errorNow)
	if err != nil {
		t.Fatalf("unexpected refusal: %v", err)
	}
	if !envelope.IsTransient() {
		t.Error("429 not reported as transient")
	}
	if envelope.RetryAfterSeconds == nil || *envelope.RetryAfterSeconds != 30 {
		t.Fatalf("retry guidance not parsed: %+v", envelope.RetryAfterSeconds)
	}
}

func TestClockSkewIsToleratedUpToTheBoundAndRefusedPastIt(t *testing.T) {
	// Both sides, because a one-sided bound is not a bound. The at-the-bound
	// case is what stops a future tightening from silently refusing envelopes
	// from a host a few seconds fast.
	root := testRoot(t)
	raw := loadErrorFixture(t, "valid-403-grant-absent.json")
	stamped := time.Date(2026, 9, 3, 5, 0, 0, 0, time.UTC)

	atBound := stamped.Add(-MaxClockSkew)
	if _, err := ParseErrorEnvelope(root, raw, 403, atBound); err != nil {
		t.Fatalf("exactly at the skew bound must be accepted, got: %v", err)
	}
	pastBound := atBound.Add(-time.Second)
	if _, err := ParseErrorEnvelope(root, raw, 403, pastBound); err == nil {
		t.Fatal("one second past the skew bound was accepted")
	}
}

func TestAPastTimestampIsNeverRefused(t *testing.T) {
	// The skew bound is one-directional on purpose: a queued or replayed
	// envelope is stamped in the past and must still parse. This test is what
	// stops the bound being made symmetric by someone tidying it.
	root := testRoot(t)
	raw := loadErrorFixture(t, "valid-403-grant-absent.json")
	muchLater := errorNow.AddDate(1, 0, 0)
	if _, err := ParseErrorEnvelope(root, raw, 403, muchLater); err != nil {
		t.Fatalf("a past timestamp was refused: %v", err)
	}
}

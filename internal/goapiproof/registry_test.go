package goapiproof

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func buildInfoServer(t *testing.T, status int, body string) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" {
			w.WriteHeader(http.StatusUnauthorized)
			return
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(server.Close)
	return server
}

func TestFetchBuildIdentityReturnsTheRunningCommit(t *testing.T) {
	server := buildInfoServer(t, http.StatusOK, `{"commit":"b18e56fa79cfe20ce0f75df148144b832d92be36","modified":false}`)
	commit, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"))
	if err != nil {
		t.Fatalf("FetchBuildIdentity: %v", err)
	}
	if commit != "b18e56fa79cfe20ce0f75df148144b832d92be36" {
		t.Fatalf("got %q", commit)
	}
}

// Every way the process can fail to identify itself is a REFUSAL. None of
// them may fall back to an operator-supplied name.
func TestFetchBuildIdentityRefusesAnUnidentifiableBuild(t *testing.T) {
	for name, testCase := range map[string]struct {
		status int
		body   string
	}{
		"empty commit":    {http.StatusOK, `{"commit":"","modified":false}`},
		"unknown commit":  {http.StatusOK, `{"commit":"unknown","modified":false}`},
		"modified tree":   {http.StatusOK, `{"commit":"b18e56fa7","modified":true}`},
		"route not there": {http.StatusNotFound, `not found`},
	} {
		t.Run(name, func(t *testing.T) {
			server := buildInfoServer(t, testCase.status, testCase.body)
			_, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
				StaticCredential("Authorization", "test", "Bearer x"))
			if !errors.Is(err, ErrNoBuildIdentity) {
				t.Fatalf("expected ErrNoBuildIdentity, got %v", err)
			}
		})
	}
}

// r3 P1 (reproduced): encoding/json's key-case shadowing lets a LATER,
// differently-cased key win over an EARLIER exact one for the same
// struct field -- {"modified":true,"MODIFIED":false} decoded Modified as
// false with the old plain-struct decode, defeating the modified-build
// refusal outright. exactBoolField (keyed on a real Go map, exact-string
// equal) closes it: "MODIFIED" and "modified" are two distinct map
// entries, so the exact "modified" key can never be shadowed by the
// other one, regardless of which comes first in the body.
func TestFetchBuildIdentityRefusesAModifiedBuildEvenUnderAShadowKey(t *testing.T) {
	for name, body := range map[string]string{
		"exact key first, shadow second": `{"commit":"b18e56fa7","modified":true,"MODIFIED":false}`,
		"shadow key first, exact second": `{"commit":"b18e56fa7","MODIFIED":false,"modified":true}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := buildInfoServer(t, http.StatusOK, body)
			_, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
				StaticCredential("Authorization", "test", "Bearer x"))
			if !errors.Is(err, ErrNoBuildIdentity) {
				t.Fatalf("a MODIFIED build (real key present, however ordered against a shadow) must refuse, got %v", err)
			}
		})
	}
}

// r3 P1 (reproduced): a commit byte the JSON string scanner cannot
// represent decodes SILENTLY into the Unicode replacement character
// (U+FFFD) rather than erroring -- the corrupted value would otherwise be
// written as current_candidate_build, a 4-column foreign-key value every
// routing row and receipt is keyed against.
func TestFetchBuildIdentityRefusesInvalidUTF8(t *testing.T) {
	server := buildInfoServer(t, http.StatusOK, "{\"commit\":\"b18e56\xff\",\"modified\":false}")
	_, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"))
	if err == nil {
		t.Fatal("a /buildinfo response containing invalid UTF-8 must refuse, not silently substitute U+FFFD into the commit")
	}
}

// r4 P1 (reproduced): `\ud800` is six valid ASCII bytes -- it passes
// utf8.Valid on the raw body -- but encoding/json unescapes an unpaired
// UTF-16 surrogate half to U+FFFD rather than erroring, so this body and
// one carrying a LITERAL U+FFFD commit decode to the identical Go string.
// Executed: `{"commit":"\ud800","modified":false}` -> commit == "�",
// the same value a genuinely corrupted build identity would produce.
func TestFetchBuildIdentityRefusesUnpairedSurrogateEscape(t *testing.T) {
	server := buildInfoServer(t, http.StatusOK, `{"commit":"\ud800aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","modified":false}`)
	_, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"))
	if err == nil {
		t.Fatal("a /buildinfo response carrying an unpaired UTF-16 surrogate escape must refuse, not silently collapse to U+FFFD")
	}
}

// r4 P2 (reproduced): `"modified": null` decodes through a plain
// `json.Unmarshal(null, &value)` into a bool as a documented NO-OP --
// value stays false, its zero value -- so a caller reading only the value
// (not exactBoolField's presence flag) could not tell an EXPLICIT "clean"
// from "the process would not say". Executed: this body enabled a row
// exactly like `"modified":false` would have.
func TestFetchBuildIdentityRefusesNullModified(t *testing.T) {
	server := buildInfoServer(t, http.StatusOK, `{"commit":"b18e56fa7","modified":null}`)
	_, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"))
	if err == nil {
		t.Fatal("`modified: null` must refuse -- an unknown cleanliness answer must never be read as false")
	}
}

// r4 P2 (reproduced), the other half: OMITTING `modified` entirely also
// reached exactBoolField's zero value with present=false, and the caller
// used to discard that flag (`modified, _, err := exactBoolField(...)`).
func TestFetchBuildIdentityRefusesAbsentModified(t *testing.T) {
	server := buildInfoServer(t, http.StatusOK, `{"commit":"b18e56fa7"}`)
	_, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"))
	if err == nil {
		t.Fatal("an absent `modified` key must refuse -- unknown cleanliness must never default to clean")
	}
}

// r3 P1 (team-lead's decoder sweep): /buildinfo must accept an
// unrecognised key the same way the catalog does -- refusing on it would
// disagree with a Python reader that simply never looks at it.
func TestFetchBuildIdentityAcceptsAnUnknownKey(t *testing.T) {
	server := buildInfoServer(t, http.StatusOK, `{"commit":"b18e56fa7","modified":false,"unexpected_future_field":"x"}`)
	commit, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"))
	if err != nil {
		t.Fatalf("an unrecognised key must not refuse the buildinfo body: %v", err)
	}
	if commit != "b18e56fa7" {
		t.Fatalf("commit = %q", commit)
	}
}

func TestFetchBuildIdentitySendsTheEnvelope(t *testing.T) {
	server := buildInfoServer(t, http.StatusOK, `{"commit":"abc","modified":false}`)
	// No Authorization header: the stub answers 401, and that must be a
	// distinct failure from "cannot identify its build" -- one is a
	// credential problem, the other is a deployment problem.
	_, err := FetchBuildIdentity(context.Background(), server.Client(), server.URL, nil)
	if err == nil {
		t.Fatal("an unauthenticated /buildinfo read must fail")
	}
	if errors.Is(err, ErrNoBuildIdentity) {
		t.Fatalf("a 401 is not an unidentifiable build: %v", err)
	}
}

// --candidate-build can FAIL a run; it can never supply the value.
func TestVerifyCandidateBuildTreatsTheFlagAsACrossCheck(t *testing.T) {
	if err := VerifyCandidateBuild("abc", "abc", nil); err != nil {
		t.Fatalf("a matching cross-check must pass: %v", err)
	}
	err := VerifyCandidateBuild("abc", "def", nil)
	if err == nil {
		t.Fatal("a mismatched --candidate-build must fail the run")
	}
	if !strings.Contains(err.Error(), "cross-check, never the source") {
		t.Fatalf("the failure must say what the flag is for, got %v", err)
	}
}

// A routing row naming a build the process is not is REPORTED, not
// refused. It was a refusal until CHAOS-5484: see StaleRoutingRows for
// why the comparison never protected what the refusal claimed to, and why
// enforcing it made every shadow operation permanently unprovable after a
// redeploy.
func TestStaleRoutingRowsNamesDisagreeingRowsAndOnlyThose(t *testing.T) {
	stale := StaleRoutingRows("abc", map[string]RoutingRow{
		"featureFlags": {Mode: "canary", CandidateBuild: "abc"},
		"hotspots":     {Mode: "canary", CandidateBuild: "stale-sha"},
		"flowMatrix":   {Mode: "shadow", CandidateBuild: "older-sha"},
		"unregistered": {Mode: "shadow"},
	})
	if got := stale["hotspots"]; got != "stale-sha" {
		t.Fatalf("a disagreeing canary row must be reported with the build it names, got %q", got)
	}
	// The row that made this a defect: a shadow row cannot be re-pointed
	// by any supported verb, so refusing on it made the operation
	// permanently unprovable.
	if got := stale["flowMatrix"]; got != "older-sha" {
		t.Fatalf("a disagreeing shadow row must be reported too, got %q", got)
	}
	if _, reported := stale["featureFlags"]; reported {
		t.Fatal("an agreeing row must not be reported")
	}
	if _, reported := stale["unregistered"]; reported {
		t.Fatal("a row naming no build at all has nothing to disagree with")
	}
	if len(stale) != 2 {
		t.Fatalf("expected exactly the two disagreeing rows, got %v", stale)
	}
}

// A stale routing row REFUSES the run again (r1 P1).
//
// The demotion assumed /buildinfo identifies the process that served the
// measured request. With more than one query-api replica and an edge that
// drops the per-request build header, it does not: /buildinfo can be
// answered by replica A while the measurement is served by replica B, and
// the resulting receipt names A. The routing row's build is the remaining
// cross-check that the fleet is on ONE build.
func TestAStaleRoutingRowRefusesTheRun(t *testing.T) {
	err := VerifyCandidateBuild("abc", "", map[string]RoutingRow{
		"featureFlags": {Mode: "canary", CandidateBuild: "abc"},
		"flowMatrix":   {Mode: "shadow", CandidateBuild: "older-sha"},
	})
	if err == nil {
		t.Fatal("a routing row naming another build must refuse the run")
	}
	if !strings.Contains(err.Error(), "flowMatrix points at older-sha") {
		t.Fatalf("the refusal must NAME the disagreeing row, got %v", err)
	}
	if strings.Contains(err.Error(), "featureFlags") {
		t.Fatalf("an agreeing row must not be reported, got %v", err)
	}
	// The refusal has to tell the operator how to clear it, or it is a
	// wall rather than a gate -- and it must name the MODE-PRESERVING
	// verb. `routing enable --candidate-build` re-points a row, but its
	// --mode is canary|primary only, so following that advice on a SHADOW
	// row silently flips it to canary.
	if !strings.Contains(err.Error(), "go-api-routing repoint") {
		t.Fatalf("the refusal must name the mode-preserving re-point verb, got %v", err)
	}
	if !strings.Contains(err.Error(), "would also change the mode") {
		t.Fatalf("the refusal must warn why `routing enable` is the wrong remedy here, got %v", err)
	}
}

func TestRowsAgreeingWithTheRunningBuildPassTheCheck(t *testing.T) {
	if err := VerifyCandidateBuild("abc", "", map[string]RoutingRow{
		"featureFlags": {Mode: "canary", CandidateBuild: "abc"},
		"unregistered": {Mode: "shadow"},
	}); err != nil {
		t.Fatalf("every row agrees, yet the run was refused: %v", err)
	}
}

// r5 P1 (reproduced): FetchRegistry used to refuse OUTRIGHT on an empty
// `operations` array -- correct for a write verb, wrong for `status`,
// which shares this exact function and needs the schema_digest a
// refusal destroyed along with everything else. Executed: `status`
// against a process that agrees on schema_digest but currently registers
// nothing reported `go plane schema_digest: UNREACHABLE (... registers no
// operations -- there is nothing to prove)`, identical to a genuinely
// DOWN process -- collapsing two facts an operator needs to tell apart.
// The refusal moved to enable's and repoint's own preflights (each tested
// separately); FetchRegistry itself now succeeds with an empty
// DocumentDigest map, matching the Python reader's own tolerance for
// this shape (`GoPlaneRegistry(schema_digest=..., operations={})`).
func TestFetchRegistryAcceptsAnEmptyRegistration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"schema_digest":"sha256:abc","operations":[]}`))
	}))
	t.Cleanup(server.Close)
	view, err := FetchRegistry(context.Background(), server.Client(), server.URL)
	if err != nil {
		t.Fatalf("a process agreeing on schema_digest but registering nothing must not refuse -- status needs the digest: %v", err)
	}
	if view.SchemaDigest != "sha256:abc" {
		t.Fatalf("schema_digest = %q, want it preserved even with an empty registration", view.SchemaDigest)
	}
	if view.DocumentDigest == nil || len(view.DocumentDigest) != 0 {
		t.Fatalf("DocumentDigest = %v, want a non-nil empty map (distinguishable from 'the go plane is unreachable')", view.DocumentDigest)
	}
}

// r6 F3(b) (reproduced): an ABSENT `operations` key or an explicit
// `"operations": null` used to fall through to the SAME empty-map
// success path an honest `"operations": []` gets -- so `status` printed
// [AGREE] and "the deployed go plane does not register this operation at
// all" for a response that is MALFORMED, not merely empty. Python's
// dispatcher refuses both (`payload.get("operations")` is None for
// either, and `isinstance(None, list)` is False); only a genuine JSON
// array -- empty included -- is accepted.
func TestFetchRegistryRefusesAMissingOrNullOperationsKey(t *testing.T) {
	for name, body := range map[string]string{
		"operations key absent entirely": `{"schema_digest":"sha256:abc"}`,
		"operations explicitly null":     `{"schema_digest":"sha256:abc","operations":null}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			t.Cleanup(server.Close)
			if _, err := FetchRegistry(context.Background(), server.Client(), server.URL); err == nil {
				t.Fatalf("a missing or null operations key must refuse, not silently report an empty (but genuinely present) registry")
			}
		})
	}
}

// r2 P1 (reproduced, CHAOS-5524 folded in per team-lead ruling): a
// /registry response naming the same operation twice, under CONFLICTING
// document digests, used to collapse last-wins -- whichever entry
// happened to come last silently decided which digest a routing row got
// written with. The Python verb refuses outright
// (`GoPlaneUnavailable ... lists operation 'X' more than once`); this
// must too, and it must refuse regardless of which duplicate would have
// "won" the old collapse.
// Sibling of the catalog-file UTF-8 fix: the SAME /registry endpoint is
// read by go_api_cli.py, whose HTTP client decodes the response as UTF-8
// before json.loads ever runs. A byte Go's json.Unmarshal tolerates but
// Python's client cannot decode must refuse here too.
func TestFetchRegistryRefusesInvalidUTF8(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte("{\"schema_digest\":\"sha256:abc\",\"operations\":[{\"operation\":\"flowMatrix\",\"document_digest\":\"\xff\"}]}"))
	}))
	t.Cleanup(server.Close)
	if _, err := FetchRegistry(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("a /registry response containing invalid UTF-8 must refuse -- python's HTTP client cannot decode it either")
	}
}

// r4 P1 (reproduced), the /registry sibling: the sweep named all three
// decoders explicitly (snapshot.go, registry.go, routing_catalog.go).
func TestFetchRegistryRefusesUnpairedSurrogateEscape(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"schema_digest":"sha256:abc","operations":[{"operation":"flowMatrix","document_digest":"\ud800abc"}]}`))
	}))
	t.Cleanup(server.Close)
	if _, err := FetchRegistry(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("a /registry response carrying an unpaired UTF-16 surrogate escape must refuse, not silently collapse a document_digest to U+FFFD")
	}
}

func TestFetchRegistryRefusesConflictingDuplicateOperations(t *testing.T) {
	for name, order := range map[string][2]string{
		"catalog-matching digest first": {"77c998975b27c6d14f0927c167464edaa01d702a3b1960b7a2f5bfd746f213c2", "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"},
		"catalog-matching digest last":  {"ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff", "77c998975b27c6d14f0927c167464edaa01d702a3b1960b7a2f5bfd746f213c2"},
	} {
		t.Run(name, func(t *testing.T) {
			body, err := json.Marshal(map[string]any{
				"schema_digest": "sha256:abc",
				"operations": []map[string]string{
					{"operation": "flowMatrix", "document_digest": order[0]},
					{"operation": "flowMatrix", "document_digest": order[1]},
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write(body)
			}))
			t.Cleanup(server.Close)
			_, err = FetchRegistry(context.Background(), server.Client(), server.URL)
			if err == nil {
				t.Fatal("a registry naming one operation twice under conflicting digests must refuse -- ordering must not decide which digest wins")
			}
			if !strings.Contains(err.Error(), "flowMatrix") || !strings.Contains(err.Error(), "more than once") {
				t.Fatalf("the refusal must name the duplicated operation, got %v", err)
			}
		})
	}
}

// A duplicate operation naming the IDENTICAL digest twice is still a
// malformed registry -- the guard is on the SHAPE (an operation listed
// more than once), not on whether the two entries happen to agree, so a
// tampered or buggy registry cannot escape the refusal by duplicating
// consistently.
func TestFetchRegistryRefusesAnIdenticalDuplicateOperationToo(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"schema_digest":"sha256:abc","operations":[
			{"operation":"flowMatrix","document_digest":"abc"},
			{"operation":"flowMatrix","document_digest":"abc"}
		]}`))
	}))
	t.Cleanup(server.Close)
	if _, err := FetchRegistry(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("an operation listed twice must refuse even when both entries agree")
	}
}

// r3 P1 (reproduced): the OLD plain-struct decode let a differently-cased
// shadow key win over the exact "operation"/"document_digest" key for
// the SAME field -- {"operation":"flowMatrix","document_digest":"good",
// "Operation":"tampered"} decoded Operation as "tampered", not
// "flowMatrix", regardless of the exact key appearing first. The
// exact-map read (exactStringField) cannot see this: "Operation" and
// "operation" are two distinct map entries, so reading the exact key
// always returns the exact key's own value.
func TestFetchRegistryOperationFieldIsNeverShadowedByADifferentlyCasedKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"schema_digest":"sha256:abc","operations":[
			{"operation":"flowMatrix","document_digest":"good","Operation":"tampered","Document_Digest":"tampered-too"}
		]}`))
	}))
	t.Cleanup(server.Close)
	view, err := FetchRegistry(context.Background(), server.Client(), server.URL)
	if err != nil {
		t.Fatalf("FetchRegistry: %v", err)
	}
	if _, ok := view.DocumentDigest["tampered"]; ok {
		t.Fatalf("the shadow-cased key was read instead of the exact one: %+v", view.DocumentDigest)
	}
	if got := view.DocumentDigest["flowMatrix"]; got != "good" {
		t.Fatalf("DocumentDigest[flowMatrix] = %q, want %q (the exact document_digest key, not the shadow)", got, "good")
	}
}

// r3 P1 (team-lead's decoder sweep): /registry must accept an
// unrecognised top-level or operation-entry key -- Python's dict
// subscript ignores them too, so refusing would be a disagreement in
// the opposite direction from the case-shadow bug.
func TestFetchRegistryAcceptsAnUnknownKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"schema_digest":"sha256:abc","unexpected_future_field":"x","operations":[
			{"operation":"flowMatrix","document_digest":"good","unexpected_future_field":"y"}
		]}`))
	}))
	t.Cleanup(server.Close)
	view, err := FetchRegistry(context.Background(), server.Client(), server.URL)
	if err != nil {
		t.Fatalf("an unrecognised key must not refuse the registry: %v", err)
	}
	if view.DocumentDigest["flowMatrix"] != "good" {
		t.Fatalf("DocumentDigest[flowMatrix] = %q", view.DocumentDigest["flowMatrix"])
	}
}

// r3 P1 (reproduced): "a valid operation followed by {} or null also
// passes Go's whole-response validation and permits a write" -- an empty
// or null registry entry decoded to an empty-string operation/digest
// with NO error, because exactStringField correctly reports the key as
// absent, not malformed, and nothing upstream checked for absence.
// r6 T1 (M31, noted as production-subsumed but untested at this exact
// line): an empty `schema_digest` must refuse HERE, at the raw-decode
// boundary, not only be caught incidentally by a write verb's preflight
// 2 (which compares it against a local digest that is never empty).
// `status` calls FetchRegistry directly too and has no such preflight.
func TestFetchRegistryRefusesAnEmptySchemaDigest(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"schema_digest":"","operations":[{"operation":"flowMatrix","document_digest":"good"}]}`))
	}))
	t.Cleanup(server.Close)
	if _, err := FetchRegistry(context.Background(), server.Client(), server.URL); err == nil {
		t.Fatal("an empty schema_digest must refuse -- it is not a real digest anything computed")
	}
}

func TestFetchRegistryRefusesAnEmptyOrNullOperationsEntry(t *testing.T) {
	for name, body := range map[string]string{
		"empty object entry": `{"schema_digest":"sha256:abc","operations":[
			{"operation":"flowMatrix","document_digest":"good"},
			{}
		]}`,
		"null entry": `{"schema_digest":"sha256:abc","operations":[
			{"operation":"flowMatrix","document_digest":"good"},
			null
		]}`,
	} {
		t.Run(name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				_, _ = w.Write([]byte(body))
			}))
			t.Cleanup(server.Close)
			if _, err := FetchRegistry(context.Background(), server.Client(), server.URL); err == nil {
				t.Fatal("an empty or null operations entry must refuse, not silently decode to an empty operation/digest")
			}
		})
	}
}

// r3 P1 (reproduced, package-wide UTF-8 sweep): LoadDocuments has the
// same silent U+FFFD substitution the other decoders in this package
// close -- an invalid byte anywhere in the file decoded without error
// before this check existed.
func TestLoadDocumentsRefusesInvalidUTF8(t *testing.T) {
	path := filepath.Join(t.TempDir(), "documents.json")
	if err := os.WriteFile(path, []byte("[{\"operation\":\"flowMatrix\",\"document\":\"query \xff\",\"digest\":\"d\"}]"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDocuments(path); err == nil {
		t.Fatal("a documents file containing invalid UTF-8 must refuse")
	}
}

// r4 P1 (reproduced): the third named decoder in the surrogate sweep.
func TestLoadDocumentsRefusesUnpairedSurrogateEscape(t *testing.T) {
	path := filepath.Join(t.TempDir(), "documents.json")
	body := `[{"operation":"flowMatrix","document":"query \ud800abc","digest":"d"}]`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadDocuments(path); err == nil {
		t.Fatal("a documents file carrying an unpaired UTF-16 surrogate escape must refuse, not silently collapse the document text to U+FFFD")
	}
}

// r3 P1 (team-lead's decoder sweep): LoadDocuments was the LAST plain
// struct-tag decode in this file -- the same case-shadow class closed
// for /registry and /buildinfo applied to it too.
func TestLoadDocumentsFieldsAreNeverShadowedByADifferentlyCasedKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "documents.json")
	body := `[{"operation":"flowMatrix","document":"good query text","Document":"tampered"}]`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	documents, err := LoadDocuments(path)
	if err != nil {
		t.Fatalf("LoadDocuments: %v", err)
	}
	if got := documents["flowMatrix"]; got != "good query text" {
		t.Fatalf("documents[flowMatrix] = %q, want the exact \"document\" key's value, not the shadow", got)
	}
}

// Every decoder in this package accepts an unrecognised key rather than
// refusing the whole file on it -- Python's dict subscript ignores extra
// keys too, so accepting them is AGREEMENT, and refusing them would be
// the same disagreement pointed the other way (team-lead's decoder
// sweep: catalog already documents and tests this; the others did not
// have an explicit executed case).
func TestLoadDocumentsAcceptsAnUnknownKey(t *testing.T) {
	path := filepath.Join(t.TempDir(), "documents.json")
	body := `[{"operation":"flowMatrix","document":"good query text","unexpected_future_field":"anything"}]`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	documents, err := LoadDocuments(path)
	if err != nil {
		t.Fatalf("an unrecognised key must not refuse the file: %v", err)
	}
	if got := documents["flowMatrix"]; got != "good query text" {
		t.Fatalf("documents[flowMatrix] = %q", got)
	}
}

// A build that moves DURING a run invalidates every receipt the run wrote,
// because each names the build read before it started.
func TestVerifyBuildStableRefusesAMovedBuild(t *testing.T) {
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		_, _ = w.Write([]byte(`{"commit":"build-after","modified":false}`))
	}))
	t.Cleanup(server.Close)

	err := VerifyBuildStable(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"), "build-before")
	if err == nil {
		t.Fatal("a build that moved mid-run must fail the run")
	}
	if !strings.Contains(err.Error(), "build-before") || !strings.Contains(err.Error(), "build-after") {
		t.Fatalf("the failure must name BOTH builds, got %v", err)
	}
	if calls != 1 {
		t.Fatalf("expected exactly one re-read, got %d", calls)
	}
}

func TestVerifyBuildStableAcceptsAnUnchangedBuild(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"commit":"same-build","modified":false}`))
	}))
	t.Cleanup(server.Close)

	if err := VerifyBuildStable(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"), "same-build"); err != nil {
		t.Fatalf("an unchanged build must pass: %v", err)
	}
}

// A re-read that FAILS is not the same as a stable build -- the run cannot
// show its receipts name the build that served them.
func TestVerifyBuildStableRefusesWhenTheRereadFails(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	t.Cleanup(server.Close)

	if err := VerifyBuildStable(context.Background(), server.Client(), server.URL,
		StaticCredential("Authorization", "test", "Bearer x"), "some-build"); err == nil {
		t.Fatal("a failed re-read must fail the run, not pass silently")
	}
}

// r1 P2: the refusal path built its own prose and dropped the provenance
// the success path carried, so the receipts that most needed context had
// the least. Both paths now use ONE constructor, and the result is a JSON
// object rather than generated text appended to operator text.
func TestBothReceiptPathsCarryTheSameStructuredProvenance(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "canary")
	runner.Config.ReviewEvidence = "CHAOS-5425 first deployed-executed run"

	if _, _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}

	success, err := runner.ReceiptsFor(time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	refusal, err := runner.RefusalReceipts(time.Now().UTC(), "the serving build moved DURING the run")
	if err != nil {
		t.Fatalf("RefusalReceipts: %v", err)
	}
	if len(success) != 1 || len(refusal) != 1 {
		t.Fatalf("expected one receipt on each path, got %d and %d", len(success), len(refusal))
	}

	for name, receipt := range map[string]Receipt{"success": success[0], "refusal": refusal[0]} {
		t.Run(name, func(t *testing.T) {
			var provenance ReceiptProvenance
			if err := json.Unmarshal([]byte(receipt.ReviewEvidence), &provenance); err != nil {
				t.Fatalf("review_evidence is not a JSON object -- a reader cannot tell which half a machine wrote: %q (%v)", receipt.ReviewEvidence, err)
			}
			// The operator's words are kept VERBATIM and in their own key,
			// never concatenated with generated text.
			if provenance.Operator != "CHAOS-5425 first deployed-executed run" {
				t.Fatalf("the operator's own evidence was altered: %q", provenance.Operator)
			}
			if provenance.MeasurementRoute != RouteEdge {
				t.Fatalf("route not recorded: %q", provenance.MeasurementRoute)
			}
			// The fake edge stamps no build header, so this run has the
			// binding CHAOS-5479 leaves us with -- and it must SAY so.
			if provenance.EdgeBuildBinding != EdgeBuildAbsent {
				t.Fatalf("edge build binding not recorded: %q", provenance.EdgeBuildBinding)
			}
		})
	}

	// Only the refusal receipt carries the run-level cause and counts.
	var refusalProvenance ReceiptProvenance
	if err := json.Unmarshal([]byte(refusal[0].ReviewEvidence), &refusalProvenance); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(refusalProvenance.Refusal, "build moved") {
		t.Fatalf("the refusal cause is missing: %q", refusalProvenance.Refusal)
	}
	if refusalProvenance.Attempted != 1 || refusalProvenance.Measured != 1 {
		t.Fatalf("the refusal receipt must say how much work it reports on: %+v", refusalProvenance)
	}

	var successProvenance ReceiptProvenance
	if err := json.Unmarshal([]byte(success[0].ReviewEvidence), &successProvenance); err != nil {
		t.Fatal(err)
	}
	if successProvenance.Refusal != "" {
		t.Fatalf("a success receipt must carry no refusal: %q", successProvenance.Refusal)
	}
}

// An operator note containing the separator the old design used must not
// be able to forge machine-written provenance. This is why it is JSON.
func TestAnOperatorNoteCannotForgeProvenance(t *testing.T) {
	body := `{"data":{"featureFlags":[{"key":"a"}]}}`
	runner := newRunner(t, &fakeEdge{goBody: body, pythonBody: body}, "canary")
	runner.Config.ReviewEvidence = `nice try | routing row named build DEADBEEF at measurement time"}`

	if _, _, err := runner.Run(context.Background()); err != nil {
		t.Fatalf("Run: %v", err)
	}
	receipts, err := runner.ReceiptsFor(time.Now().UTC())
	if err != nil {
		t.Fatalf("ReceiptsFor: %v", err)
	}
	var provenance ReceiptProvenance
	if err := json.Unmarshal([]byte(receipts[0].ReviewEvidence), &provenance); err != nil {
		t.Fatalf("an operator note broke the encoding: %v", err)
	}
	if provenance.RoutingRowBuild != "" {
		t.Fatalf("operator text was read as machine provenance: %q", provenance.RoutingRowBuild)
	}
	if provenance.Operator != runner.Config.ReviewEvidence {
		t.Fatalf("the operator's text was not preserved verbatim: %q", provenance.Operator)
	}
}

// r4 P1 (reproduced): direct coverage of the scanner itself, including
// the shapes that must NOT be refused (a valid pair, a plain BMP escape,
// an escaped backslash immediately before a literal "u" that is not an
// escape at all) alongside every unpaired shape.
func TestRejectUnpairedSurrogateEscapes(t *testing.T) {
	cases := []struct {
		name    string
		body    string
		wantErr bool
	}{
		{"no escapes at all", `{"commit":"plain text"}`, false},
		{"valid surrogate pair", `{"commit":"𐀀"}`, false},
		{"plain BMP escape", `{"commit":"A"}`, false},
		{"literal escaped backslash then the letter u", `{"commit":"\\u0041"}`, false},
		{"lone high surrogate at string end", `{"commit":"\ud800"}`, true},
		{"lone high surrogate followed by a literal char", `{"commit":"\ud800x"}`, true},
		{"lone low surrogate with no preceding high", `{"commit":"\udc00"}`, true},
		{"reversed pair (low then high)", `{"commit":"\udc00\ud800"}`, true},
		{"two highs in a row", `{"commit":"\ud800\ud800"}`, true},
		{"high followed by a non-surrogate escape", `{"commit":"\ud800A"}`, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := rejectUnpairedSurrogateEscapes([]byte(tc.body))
			if tc.wantErr && err == nil {
				t.Fatalf("body %q: want an error, got nil", tc.body)
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("body %q: want no error, got %v", tc.body, err)
			}
		})
	}
}

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

// authorizationNow is 30 seconds before the fixtures' decision expiry, so expiry
// checks do not depend on the date the suite runs.
var authorizationNow = time.Date(2026, 9, 3, 5, 0, 0, 0, time.UTC)

var authorizationSurfaces = []string{
	AuthorizationRequestSurface,
	AuthorizationDecisionSurface,
	AuthorizationBatchRequestSurface,
	AuthorizationBatchResponseSurface,
}

// authorizationDir is the corpus directory of one surface: the schema stem
// without its version, because the strict ajv runner pairs examples/<name>/
// with <name>.v1.schema.json.
// authorizationManifestPaths name each manifest by full repo path: a bare
// file name is ambiguous to the Go path-filter gate.
var authorizationManifestPaths = map[string]string{
	AuthorizationRequestSurface:       "contracts/auth/v1/examples/authorization_request/manifest.json",
	AuthorizationDecisionSurface:      "contracts/auth/v1/examples/authorization_decision/manifest.json",
	AuthorizationBatchRequestSurface:  "contracts/auth/v1/examples/authorization_batch_request/manifest.json",
	AuthorizationBatchResponseSurface: "contracts/auth/v1/examples/authorization_batch_response/manifest.json",
}

var authorizationManifestBasename = filepath.Base(authorizationManifestPaths[AuthorizationRequestSurface])

func authorizationDir(surface string) string {
	return filepath.Join("contracts", "auth", "v1", "examples", strings.TrimSuffix(surface, ".v1"))
}

type authorizationEntry struct {
	File                   string `json:"file"`
	ExpectInstanceLocation string `json:"expect_instance_location"`
	ExpectKeyword          string `json:"expect_keyword"`
	Why                    string `json:"why"`
	Surface                string `json:"-"`
}

// authorizationManifest merges the four per-surface manifests. Request, when
// present (the batch response corpus), is the batch request every fixture in
// that corpus answers.
type authorizationManifest struct {
	Accept         []authorizationEntry
	Reject         []authorizationEntry
	RejectByClient []authorizationEntry
	NotCacheable   []authorizationEntry
	BatchRequest   json.RawMessage
}

// Tokens read off google/jsonschema-go for the keywords this corpus asserts.
var goAuthorizationKeywordTokens = map[string]string{
	"maxItems":  "maxItems:",
	"maxLength": "maxLength:",
	"minItems":  "minItems:",
	"maximum":   "maximum:",
	"not":       "not:",
	"type":      "type:",
}

func authorizationTokenFor(keyword string) (string, bool) {
	if token, ok := goMessageForKeyword[keyword]; ok {
		return token, true
	}
	token, ok := goAuthorizationKeywordTokens[keyword]
	return token, ok
}

func loadAuthorizationManifest(t *testing.T) authorizationManifest {
	t.Helper()
	var merged authorizationManifest
	for _, surface := range authorizationSurfaces {
		raw, err := os.ReadFile(filepath.Join(testRoot(t), authorizationManifestPaths[surface]))
		if err != nil {
			t.Fatalf("reading %s manifest: %v", surface, err)
		}
		var manifest struct {
			Accept         []authorizationEntry `json:"accept"`
			Reject         []authorizationEntry `json:"reject"`
			RejectByClient []authorizationEntry `json:"reject_by_client"`
			NotCacheable   []authorizationEntry `json:"accept_not_cacheable"`
			Request        json.RawMessage      `json:"request"`
		}
		if err := json.Unmarshal(raw, &manifest); err != nil {
			t.Fatalf("parsing %s manifest: %v", surface, err)
		}
		for i := range manifest.Accept {
			manifest.Accept[i].Surface = surface
		}
		for i := range manifest.Reject {
			manifest.Reject[i].Surface = surface
		}
		for i := range manifest.RejectByClient {
			manifest.RejectByClient[i].Surface = surface
		}
		for i := range manifest.NotCacheable {
			manifest.NotCacheable[i].Surface = surface
		}
		merged.Accept = append(merged.Accept, manifest.Accept...)
		merged.Reject = append(merged.Reject, manifest.Reject...)
		merged.RejectByClient = append(merged.RejectByClient, manifest.RejectByClient...)
		merged.NotCacheable = append(merged.NotCacheable, manifest.NotCacheable...)
		if surface == AuthorizationBatchResponseSurface {
			merged.BatchRequest = manifest.Request
		}
	}
	return merged
}

func authorizationBytes(t *testing.T, surface, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), authorizationDir(surface), name))
	if err != nil {
		t.Fatalf("reading fixture %s/%s: %v", surface, name, err)
	}
	return raw
}

func authorizationDocument(t *testing.T, surface, name string) any {
	t.Helper()
	var document any
	if err := json.Unmarshal(authorizationBytes(t, surface, name), &document); err != nil {
		t.Fatalf("fixture %s/%s is not valid JSON: %v", surface, name, err)
	}
	return document
}

func TestAuthorizationCorpusIsNotEmptyInEitherDirection(t *testing.T) {
	manifest := loadAuthorizationManifest(t)
	if len(manifest.Accept) == 0 || len(manifest.Reject) == 0 || len(manifest.RejectByClient) == 0 {
		t.Fatalf("manifest has accept=%d reject=%d reject_by_client=%d; an empty list makes its tests vacuous",
			len(manifest.Accept), len(manifest.Reject), len(manifest.RejectByClient))
	}
	seen := map[string]bool{}
	for _, entry := range manifest.Accept {
		seen[entry.Surface] = true
	}
	for _, surface := range authorizationSurfaces {
		if !seen[surface] {
			t.Errorf("no accept fixture for %s", surface)
		}
	}
}

func TestEveryAuthorizationFixtureOnDiskIsClaimedByTheManifest(t *testing.T) {
	manifest := loadAuthorizationManifest(t)
	claimed := map[string]map[string]bool{}
	for _, group := range [][]authorizationEntry{manifest.Accept, manifest.Reject, manifest.RejectByClient, manifest.NotCacheable} {
		for _, entry := range group {
			if claimed[entry.Surface] == nil {
				claimed[entry.Surface] = map[string]bool{}
			}
			claimed[entry.Surface][entry.File] = true
		}
	}
	for _, surface := range authorizationSurfaces {
		entries, err := os.ReadDir(filepath.Join(testRoot(t), authorizationDir(surface)))
		if err != nil {
			t.Fatalf("reading %s fixture dir: %v", surface, err)
		}
		onDisk := map[string]bool{}
		for _, entry := range entries {
			if entry.IsDir() || entry.Name() == authorizationManifestBasename || !strings.HasSuffix(entry.Name(), ".json") {
				continue
			}
			onDisk[entry.Name()] = true
		}
		var unclaimed, missing []string
		for name := range onDisk {
			if !claimed[surface][name] {
				unclaimed = append(unclaimed, name)
			}
		}
		for name := range claimed[surface] {
			if !onDisk[name] {
				missing = append(missing, name)
			}
		}
		sort.Strings(unclaimed)
		sort.Strings(missing)
		if len(unclaimed) > 0 {
			t.Errorf("%s: fixture files no runner opens: %v", surface, unclaimed)
		}
		if len(missing) > 0 {
			t.Errorf("%s: manifest claims absent files: %v", surface, missing)
		}
	}
}

func TestTheBatchRequestEmbeddedInTheResponseCorpusIsTheValidBatchRequestFixture(t *testing.T) {
	// The response corpus carries its request in its manifest because the ajv
	// runner keeps each corpus self-contained. Two copies of one document are
	// allowed only because this test makes drift detectable.
	manifest := loadAuthorizationManifest(t)
	var embedded, fixture any
	if err := json.Unmarshal(manifest.BatchRequest, &embedded); err != nil || embedded == nil {
		t.Fatalf("response corpus manifest carries no usable `request`: %v", err)
	}
	fixture = authorizationDocument(t, AuthorizationBatchRequestSurface, "batch-request-valid-two-items.json")
	a, _ := json.Marshal(embedded)
	b, _ := json.Marshal(fixture)
	if string(a) != string(b) {
		t.Fatalf("manifest request differs from batch-request-valid-two-items.json")
	}
}

func TestEveryAuthorizationManifestKeywordHasAGoMessageToken(t *testing.T) {
	for _, entry := range loadAuthorizationManifest(t).Reject {
		if _, ok := authorizationTokenFor(entry.ExpectKeyword); !ok {
			t.Errorf("no Go message token for keyword %q (fixture %s)", entry.ExpectKeyword, entry.File)
		}
	}
}

func TestAcceptedAuthorizationFixturesValidate(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadAuthorizationManifest(t).Accept {
		t.Run(entry.File, func(t *testing.T) {
			if err := Validate(root, entry.Surface, authorizationDocument(t, entry.Surface, entry.File)); err != nil {
				t.Fatalf("accept fixture was rejected: %v", err)
			}
		})
	}
}

func TestRejectedAuthorizationFixturesAreRejectedByTheDeclaredRule(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadAuthorizationManifest(t).Reject {
		t.Run(entry.File, func(t *testing.T) {
			err := Validate(root, entry.Surface, authorizationDocument(t, entry.Surface, entry.File))
			if err == nil {
				t.Fatalf("validated cleanly; expected rejection by %q at %q (%s)",
					entry.ExpectKeyword, entry.ExpectInstanceLocation, entry.Why)
			}
			token, ok := authorizationTokenFor(entry.ExpectKeyword)
			if !ok {
				t.Fatalf("no Go message token registered for keyword %q", entry.ExpectKeyword)
			}
			if !strings.Contains(err.Error(), token) {
				t.Fatalf("rejected, but not by %q. token %q absent from: %v", entry.ExpectKeyword, token, err)
			}
		})
	}
}

func TestClientEnforcedAuthorizationFixturesValidateButAreRefused(t *testing.T) {
	root := testRoot(t)
	manifest := loadAuthorizationManifest(t)
	for _, entry := range manifest.RejectByClient {
		t.Run(entry.File, func(t *testing.T) {
			raw := authorizationBytes(t, entry.Surface, entry.File)
			var document any
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatalf("fixture is not valid JSON: %v", err)
			}
			if err := Validate(root, entry.Surface, document); err != nil {
				t.Fatalf("client-enforced fixture must VALIDATE (that is what makes the client check load-bearing), got: %v", err)
			}
			var err error
			switch entry.Surface {
			case AuthorizationDecisionSurface:
				_, err = ParseAuthorizationDecision(root, raw, authorizationNow)
			case AuthorizationBatchResponseSurface:
				_, err = ParseAuthorizationBatchResponse(root, manifest.BatchRequest, raw, authorizationNow)
			default:
				t.Fatalf("no client for surface %s", entry.Surface)
			}
			if err == nil {
				t.Fatalf("client accepted a document it must refuse (%s)", entry.Why)
			}
		})
	}
}

func TestADisagreeingBatchKeepsEachDecisionBoundToItsOwnRequest(t *testing.T) {
	root := testRoot(t)
	decisions, err := ParseAuthorizationBatchResponse(root,
		authorizationBytes(t, AuthorizationBatchRequestSurface, "batch-request-valid-two-items.json"),
		authorizationBytes(t, AuthorizationBatchResponseSurface, "batch-response-valid-items-disagree.json"), authorizationNow)
	if err != nil {
		t.Fatalf("a well-formed batch was refused: %v", err)
	}
	if len(decisions) != 2 || !decisions[0].Allowed || decisions[1].Allowed {
		t.Fatalf("decisions = %+v, want [allow, deny] in request order", decisions)
	}
	if decisions[0].ReasonCode == decisions[1].ReasonCode {
		t.Errorf("both items carry reason %q: the fixture no longer disagrees", decisions[0].ReasonCode)
	}
}

func TestADecisionParsesToItsVerdictAndObligations(t *testing.T) {
	root := testRoot(t)
	allow, err := ParseAuthorizationDecision(root, authorizationBytes(t, AuthorizationDecisionSurface, "decision-valid-allow-with-obligations.json"), authorizationNow)
	if err != nil || !allow.Allowed || allow.Obligations == nil || allow.Obligations.MinimumAssurance == nil || *allow.Obligations.MinimumAssurance != "aal2" {
		t.Fatalf("allow parsed wrong: %+v, %v", allow, err)
	}
	if allow.PolicyRevision != 42 {
		t.Errorf("policy revision = %d, want 42", allow.PolicyRevision)
	}
	deny, err := ParseAuthorizationDecision(root, authorizationBytes(t, AuthorizationDecisionSurface, "decision-valid-deny.json"), authorizationNow)
	if err != nil || deny.Allowed || deny.Obligations != nil {
		t.Fatalf("deny parsed wrong: %+v, %v", deny, err)
	}
}

func TestEveryRejectFixtureIsRefusedByTheClientToo(t *testing.T) {
	// The client path adds checks on top of the schema; it must never turn a
	// schema rejection into an acceptance.
	root := testRoot(t)
	for _, entry := range loadAuthorizationManifest(t).Reject {
		if entry.Surface != AuthorizationDecisionSurface {
			continue
		}
		t.Run(entry.File, func(t *testing.T) {
			if decision, err := ParseAuthorizationDecision(root, authorizationBytes(t, entry.Surface, entry.File), authorizationNow); err == nil {
				t.Fatalf("client accepted a rejected decision: %+v", decision)
			}
		})
	}
}

func TestEveryAuthorizationManifestPathMatchesItsSurfaceDirectory(t *testing.T) {
	for _, surface := range authorizationSurfaces {
		want := filepath.ToSlash(filepath.Join(authorizationDir(surface), authorizationManifestBasename))
		if authorizationManifestPaths[surface] != want {
			t.Errorf("%s: manifest path %q, want %q", surface, authorizationManifestPaths[surface], want)
		}
	}
}

func TestADecisionIsCacheableOnlyStrictlyBeforeItsExpiry(t *testing.T) {
	// expires_at bounds REUSE, never the answer to the request it was issued
	// for: an instant at or after expiry still parses, and the decision says it
	// must not be cached. Single decision (allow and deny) and batch item.
	root := testRoot(t)
	raw := authorizationBytes(t, AuthorizationDecisionSurface, "decision-valid-allow-with-obligations.json")
	expiry := time.Date(2026, 9, 3, 5, 0, 30, 0, time.UTC)
	before, err := ParseAuthorizationDecision(root, raw, expiry.Add(-time.Second))
	if err != nil || !before.Cacheable {
		t.Fatalf("one second before expiry: err=%v decision=%+v, want parsed and Cacheable", err, before)
	}
	for _, now := range []time.Time{expiry, expiry.Add(time.Second), expiry.AddDate(1, 0, 0)} {
		decision, err := ParseAuthorizationDecision(root, raw, now)
		if err != nil {
			t.Fatalf("now=%s: an allow past its expiry was refused: %v", now, err)
		}
		if !decision.Allowed || decision.Cacheable {
			t.Errorf("now=%s: allowed=%v cacheable=%v, want allowed=true cacheable=false", now, decision.Allowed, decision.Cacheable)
		}
	}
	deny := authorizationBytes(t, AuthorizationDecisionSurface, "decision-valid-deny.json")
	denied, err := ParseAuthorizationDecision(root, deny, expiry)
	if err != nil || denied.Allowed || denied.Cacheable {
		t.Errorf("expired deny: err=%v decision=%+v, want parsed, denied, not cacheable", err, denied)
	}
	request := authorizationBytes(t, AuthorizationBatchRequestSurface, "batch-request-valid-two-items.json")
	response := authorizationBytes(t, AuthorizationBatchResponseSurface, "batch-response-valid-items-disagree.json")
	items, err := ParseAuthorizationBatchResponse(root, request, response, expiry.Add(-time.Second))
	if err != nil || !items[0].Cacheable || !items[1].Cacheable {
		t.Fatalf("batch one second before expiry: err=%v items=%+v, want all Cacheable", err, items)
	}
	items, err = ParseAuthorizationBatchResponse(root, request, response, expiry)
	if err != nil {
		t.Fatalf("a batch at expiry was refused: %v", err)
	}
	if items[0].Cacheable || items[1].Cacheable || !items[0].Allowed || items[1].Allowed {
		t.Errorf("batch at expiry: %+v, want verdicts kept and none cacheable", items)
	}
}

func TestNotCacheableFixturesValidateAndParseAsUseOnce(t *testing.T) {
	root := testRoot(t)
	manifest := loadAuthorizationManifest(t)
	if len(manifest.NotCacheable) == 0 {
		t.Fatal("manifest has no accept_not_cacheable entries; the class would be vacuous")
	}
	seen := map[string]bool{}
	for _, entry := range manifest.NotCacheable {
		seen[entry.Surface] = true
		t.Run(entry.File, func(t *testing.T) {
			raw := authorizationBytes(t, entry.Surface, entry.File)
			var document any
			if err := json.Unmarshal(raw, &document); err != nil {
				t.Fatal(err)
			}
			if err := Validate(root, entry.Surface, document); err != nil {
				t.Fatalf("not-cacheable fixture must validate: %v", err)
			}
			switch entry.Surface {
			case AuthorizationDecisionSurface:
				decision, err := ParseAuthorizationDecision(root, raw, authorizationNow)
				if err != nil || decision.Cacheable || !decision.Allowed {
					t.Fatalf("err=%v decision=%+v, want an allow that is not cacheable", err, decision)
				}
			case AuthorizationBatchResponseSurface:
				items, err := ParseAuthorizationBatchResponse(root, manifest.BatchRequest, raw, authorizationNow)
				if err != nil || len(items) != 2 || items[0].Cacheable || !items[0].Allowed {
					t.Fatalf("err=%v items=%+v, want item 0 an allow that is not cacheable", err, items)
				}
			default:
				t.Fatalf("no client for surface %s", entry.Surface)
			}
		})
	}
	for _, surface := range []string{AuthorizationDecisionSurface, AuthorizationBatchResponseSurface} {
		if !seen[surface] {
			t.Errorf("no accept_not_cacheable fixture for %s", surface)
		}
	}
}

func TestAnUnknownReasonCodeIsStillRefusedPastExpiry(t *testing.T) {
	root := testRoot(t)
	raw := authorizationBytes(t, AuthorizationDecisionSurface, "decision-invalid-reason-code-not-in-vocabulary.json")
	if decision, err := ParseAuthorizationDecision(root, raw, authorizationNow.AddDate(1, 0, 0)); err == nil {
		t.Fatalf("an unknown reason code parsed past expiry: %+v", decision)
	}
}

func TestParsedObligationsMarshalBackToWhatWasSent(t *testing.T) {
	// Absent versus empty versus false must survive a parse and re-marshal, for
	// every decision the corpus accepts: an empty allowlist means none, an
	// absent one means unrestricted within the tenant.
	root := testRoot(t)
	checked := 0
	for _, entry := range loadAuthorizationManifest(t).Accept {
		if entry.Surface != AuthorizationDecisionSurface {
			continue
		}
		t.Run(entry.File, func(t *testing.T) {
			raw := authorizationBytes(t, AuthorizationDecisionSurface, entry.File)
			decision, err := ParseAuthorizationDecision(root, raw, authorizationNow)
			if err != nil {
				t.Fatalf("accept fixture was refused: %v", err)
			}
			var sent struct {
				Obligations map[string]any `json:"obligations"`
			}
			if err := json.Unmarshal(raw, &sent); err != nil {
				t.Fatal(err)
			}
			var back map[string]any
			if decision.Obligations != nil {
				out, err := json.Marshal(decision.Obligations)
				if err != nil {
					t.Fatal(err)
				}
				if err := json.Unmarshal(out, &back); err != nil {
					t.Fatal(err)
				}
			}
			a, _ := json.Marshal(sent.Obligations)
			b, _ := json.Marshal(back)
			if string(a) != string(b) {
				t.Fatalf("obligations changed on the way out:\n sent %s\n back %s", a, b)
			}
			checked++
		})
	}
	if checked == 0 {
		t.Fatal("checked no decision fixture")
	}
}

func TestTheTypedParserRefusesAnImpossibleCalendarDate(t *testing.T) {
	// The timestamp pattern checks shape only, and google/jsonschema-go ignores
	// `format`, so Validate alone accepts 2026-02-30 (Python and ajv reject it by
	// format). The typed parser must refuse it regardless of which validator ran.
	root := testRoot(t)
	raw := strings.Replace(string(authorizationBytes(t, AuthorizationDecisionSurface, "decision-valid-deny.json")),
		"2026-09-03T05:00:30Z", "2026-02-30T05:00:30Z", 1)
	if !strings.Contains(raw, "2026-02-30") {
		t.Fatal("fixture no longer carries the expected expiry; the probe would be vacuous")
	}
	if decision, err := ParseAuthorizationDecision(root, []byte(raw), authorizationNow); err == nil {
		t.Fatalf("an impossible expiry date parsed: %+v", decision)
	}
}

// The wire form of a delegated allow of /v1/authorize: tenant_scope,
// minimum_assurance and a one-key repository_allowlist, expiring at the
// decision time. It parses, and it is not cacheable.
func TestParseDelegatedAllowWithObligationsIsUseOnce(t *testing.T) {
	raw := []byte(`{"schema_version":"authorization_decision.v1","decision_id":"dec_ABCDEFGHIJKLMNOPQRSTUVWXYZ","allowed":true,` +
		`"reason_code":"allowed_by_role","principal_id":"prn_t9UvRnQ4QpeQv2DA8zePlg","organization_id":"0f1b6c1e-6a8b-4c35-9a0e-5b0d6a2f7c11",` +
		`"policy_revision":9,"expires_at":"2026-09-03T05:00:00.000000Z","obligations":{"tenant_scope":"0f1b6c1e-6a8b-4c35-9a0e-5b0d6a2f7c11",` +
		`"minimum_assurance":"aal1","repository_allowlist":["full-chaos/dev-health-ops"]}}`)
	d, err := ParseAuthorizationDecision(testRoot(t), raw, authorizationNow)
	if err != nil {
		t.Fatal(err)
	}
	if d.Cacheable || d.Obligations == nil || d.Obligations.MinimumAssurance == nil || *d.Obligations.MinimumAssurance != "aal1" ||
		d.Obligations.RepositoryAllowlist == nil || len(*d.Obligations.RepositoryAllowlist) != 1 || d.Obligations.StepUp != nil {
		t.Fatalf("%+v %+v", d, d.Obligations)
	}
}

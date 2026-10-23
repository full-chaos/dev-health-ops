package tokencorpus

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
)

func mustGenerateWorkload(t *testing.T, seed string) *Corpus {
	t.Helper()
	c, err := GenerateWorkload(newSeededReader(seed))
	if err != nil {
		t.Fatalf("GenerateWorkload: %v", err)
	}
	return c
}

func TestWorkloadEveryGeneratedVectorGetsItsManifestDecision(t *testing.T) {
	if err := VerifyAll(testRoot(t), mustGenerateWorkload(t, "a")); err != nil {
		t.Fatal(err)
	}
}

func TestWorkloadCommittedCorpusGetsItsManifestDecision(t *testing.T) {
	root := testRoot(t)
	c, err := Load(filepath.Join(root, WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	if err := VerifyAll(root, c); err != nil {
		t.Fatal(err)
	}
}

func TestWorkloadGeneratorIsDeterministicExceptForKeys(t *testing.T) {
	first, again := render(t, mustGenerateWorkload(t, "a")), render(t, mustGenerateWorkload(t, "a"))
	if len(first) != len(again) {
		t.Fatalf("file sets differ: %d vs %d", len(first), len(again))
	}
	for name, body := range first {
		if !bytes.Equal(body, again[name]) {
			t.Errorf("%s differs between two runs on the same keys", name)
		}
	}
	other := render(t, mustGenerateWorkload(t, "b"))
	for name, body := range first {
		same := bytes.Equal(body, other[name])
		keyDependent := name == "jwks.json" || name == "manifest.json" || (strings.HasPrefix(name, "vectors"+string(filepath.Separator)) && !strings.Contains(string(body), `"signature_hex": ""`))
		if same && name == "jwks.json" {
			t.Errorf("%s did not change with the keys", name)
		}
		if !same && !keyDependent {
			t.Errorf("%s changed with the keys but is not key material", name)
		}
	}
	var ma, mb Manifest
	if err := json.Unmarshal(first["manifest.json"], &ma); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(other["manifest.json"], &mb); err != nil {
		t.Fatal(err)
	}
	ma.RemovedKeys, mb.RemovedKeys = nil, nil
	if !reflect.DeepEqual(ma, mb) {
		t.Error("manifest.json changed with the keys beyond the removed public key")
	}
	for name := range first {
		if !strings.HasPrefix(name, "vectors"+string(filepath.Separator)) {
			continue
		}
		var a, b Vector
		if err := json.Unmarshal(first[name], &a); err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(other[name], &b); err != nil {
			t.Fatal(err)
		}
		if a.HeaderJSON != b.HeaderJSON || a.PayloadJSON != b.PayloadJSON {
			t.Errorf("%s: header or payload text changed with the keys", name)
		}
	}
}

func TestWorkloadCommittedCorpusIsTheInventoryOfItsDirectory(t *testing.T) {
	root := testRoot(t)
	c, err := Load(filepath.Join(root, WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	claimed := map[string]bool{}
	for _, e := range c.Manifest.Vectors {
		if claimed[e.File] {
			t.Errorf("%s is listed twice", e.File)
		}
		claimed[e.File] = true
	}
	entries, err := os.ReadDir(filepath.Join(root, WorkloadDir, "vectors"))
	if err != nil {
		t.Fatal(err)
	}
	var unclaimed []string
	for _, e := range entries {
		if !claimed[e.Name()] {
			unclaimed = append(unclaimed, e.Name())
		}
		delete(claimed, e.Name())
	}
	var missing []string
	for name := range claimed {
		missing = append(missing, name)
	}
	sort.Strings(unclaimed)
	sort.Strings(missing)
	if len(unclaimed)+len(missing) > 0 {
		t.Fatalf("on disk but unlisted: %v; listed but absent: %v", unclaimed, missing)
	}
	top, err := os.ReadDir(filepath.Join(root, WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range top {
		switch e.Name() {
		case "manifest.json", "jwks.json", "README.md", "vectors":
		default:
			t.Errorf("unexpected entry %s in the corpus directory", e.Name())
		}
	}
}

func TestWorkloadManifestRowsAreWellFormed(t *testing.T) {
	c, err := Load(filepath.Join(testRoot(t), WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	reasons := map[string]bool{}
	for _, r := range Reasons {
		reasons[r] = true
	}
	seen := map[string]int{}
	for _, e := range c.Manifest.Vectors {
		switch e.Expect {
		case Accept:
			if e.Reason != "" || e.Stage != "" {
				t.Errorf("%s: an accept row names a reason", e.File)
			}
		case Reject:
			if !reasons[e.Reason] {
				t.Errorf("%s: reason %q is outside the vocabulary", e.File, e.Reason)
			}
			if (e.Reason == ReasonContract) != (e.Stage != "") {
				t.Errorf("%s: contract_stage must be set exactly for the contract reason", e.File)
			}
			seen[e.Reason]++
		default:
			t.Errorf("%s: expect %q", e.File, e.Expect)
		}
		if e.Why == "" || e.VerifyAs == "" || e.Now == 0 {
			t.Errorf("%s: why, verify_as_audience and now are required", e.File)
		}
	}
	for _, r := range Reasons {
		if seen[r] == 0 {
			t.Errorf("no reject row exercises the reason %q", r)
		}
	}
}

func TestWorkloadCommittedJwksIsPublicOnlyAndValidatesAgainstTheContract(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, WorkloadDir, "jwks.json"))
	if err != nil {
		t.Fatal(err)
	}
	var document any
	if err := json.Unmarshal(raw, &document); err != nil {
		t.Fatal(err)
	}
	if err := contracts.Validate(root, "jwks.v1", document); err != nil {
		t.Fatalf("jwks.json does not satisfy jwks.v1: %v", err)
	}
	for _, private := range []string{`"d"`, `"p"`, `"q"`, `"k"`} {
		if strings.Contains(string(raw), private+":") {
			t.Errorf("jwks.json carries the private member %s", private)
		}
	}
}

func TestWorkloadNoPrivateKeyOrCompactTokenIsCommitted(t *testing.T) {
	root := testRoot(t)
	err := filepath.WalkDir(filepath.Join(root, WorkloadDir), func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(raw)
		if strings.Contains(text, "PRIVATE KEY") || strings.Contains(text, `"d":`) {
			t.Errorf("%s carries private key material", path)
		}
		if compactShape.MatchString(text) {
			t.Errorf("%s carries three dot-joined base64url segments", path)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// Each broken reader below skips one decision; the corpus must catch it. A
// corpus no mutant can fail proves nothing.
func TestWorkloadTheCorpusFailsAReaderThatSkipsADecision(t *testing.T) {
	root := testRoot(t)
	c, err := Load(filepath.Join(root, WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	for name, skip := range map[string]string{
		"signature":    ReasonSignature,
		"kid":          ReasonUnknownKid,
		"issuer":       ReasonIssuer,
		"audience":     ReasonAudience,
		"not before":   ReasonNotYetValid,
		"expiry":       ReasonExpired,
		"the contract": ReasonContract,
	} {
		t.Run(name, func(t *testing.T) {
			if err := verifyAll(root, c, skip); err == nil {
				t.Fatalf("a reader that skips %s passed every vector", skip)
			}
		})
	}
}

// The removed-kid row is run with the removed key in the verifier's key set
// (a stale cache), by the manifest itself, so every consumer executes it.
func TestWorkloadRemovedKidRowRunsWithTheStaleKeyFromTheManifest(t *testing.T) {
	root := testRoot(t)
	c, err := Load(filepath.Join(root, WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	stale := 0
	for _, e := range c.Manifest.Vectors {
		if e.KeySet == KeySetPublishedAndRemoved {
			stale++
			if e.Reason != ReasonUnknownKid {
				t.Errorf("%s: a stale-key row must be refused as %s", e.File, ReasonUnknownKid)
			}
		}
	}
	if stale == 0 {
		t.Fatal("no manifest row runs with the removed key in the key set")
	}
	for _, k := range c.Jwks.Keys {
		for _, removed := range c.Manifest.RemovedKeys {
			if k.Kid == removed.Kid {
				t.Fatalf("removed kid %s is still in jwks.json", k.Kid)
			}
		}
	}
	if err := verifyAll(root, c, skipRemoved); err == nil {
		t.Fatal("a reader that ignores removed kids passed the manifest run")
	}
}

func TestWorkloadTheCorpusFailsAReaderThatChecksIatInsteadOfNbf(t *testing.T) {
	root := testRoot(t)
	c, err := Load(filepath.Join(root, WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	if err := verifyAll(root, c, skipNbfFromIat); err == nil {
		t.Fatal("a reader that reads iat where nbf belongs passed every vector")
	}
}

func TestWorkloadVerifyAllRefusesAManifestRowWhoseExpectContradictsItsReason(t *testing.T) {
	root := testRoot(t)
	c, err := Load(filepath.Join(root, WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	for _, flip := range []struct{ file, expect string }{{"valid-auth-service.json", Reject}, {"invalid-alg-none.json", Accept}, {"valid-auth-service.json", "maybe"}} {
		bad := *c
		bad.Manifest.Vectors = append([]Entry{}, c.Manifest.Vectors...)
		for i := range bad.Manifest.Vectors {
			if bad.Manifest.Vectors[i].File == flip.file {
				bad.Manifest.Vectors[i].Expect = flip.expect
			}
		}
		if err := VerifyAll(root, &bad); err == nil {
			t.Errorf("%s with expect %q passed VerifyAll", flip.file, flip.expect)
		}
	}
}

func TestWorkloadFileNamesCannotEscapeTheCorpusDirectory(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "a", "b")
	c := mustGenerateWorkload(t, "a")
	if err := Write(dir, c); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{"../../outside.json", "../x.json", "sub/x.json", `..\x.json`, "..", ".", "", "x.txt"} {
		bad := *c
		bad.Vectors = map[string]Vector{name: c.Vectors["valid-auth-service.json"]}
		if err := Write(filepath.Join(base, "w"), &bad); err == nil {
			t.Errorf("Write accepted the vector name %q", name)
		}
		loaded := *c
		loaded.Manifest.Vectors = []Entry{{File: name}}
		if err := writeJSON(filepath.Join(dir, "manifest.json"), loaded.Manifest); err != nil {
			t.Fatal(err)
		}
		if _, err := Load(dir); err == nil {
			t.Errorf("Load accepted the manifest file name %q", name)
		}
	}
	if _, err := os.Stat(filepath.Join(base, "outside.json")); err == nil {
		t.Error("a file was written outside the corpus directory")
	}
}

func TestWorkloadWriteRemovesVectorsTheCorpusNoLongerHas(t *testing.T) {
	dir := t.TempDir()
	c := mustGenerateWorkload(t, "a")
	if err := Write(dir, c); err != nil {
		t.Fatal(err)
	}
	stale := filepath.Join(dir, "vectors", "invalid-retired-vector.json")
	if err := os.WriteFile(stale, []byte("{}\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := Write(dir, c); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(stale); err == nil {
		t.Fatal("Write left a vector the corpus no longer has")
	}
	if _, err := Load(dir); err != nil {
		t.Fatal(err)
	}
}

// Each broken reader below swaps two neighbouring decisions. A swap no vector
// can tell apart is a decision order the corpus does not pin. nbf and exp
// never both fail (the contract requires nbf before exp), so that pair cannot
// be told apart by any token.
func TestWorkloadTheCorpusPinsTheDecisionOrder(t *testing.T) {
	root := testRoot(t)
	c, err := Load(filepath.Join(root, WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	order := c.Manifest.ReasonOrder
	if strings.Join(order, ",") != strings.Join(DecisionOrder, ",") {
		t.Fatalf("manifest decision_order %v is not %v", order, DecisionOrder)
	}
	for i := 0; i+1 < len(order); i++ {
		a, b := order[i], order[i+1]
		if a == ReasonNotYetValid && b == ReasonExpired {
			continue
		}
		t.Run(a+" before "+b, func(t *testing.T) {
			swapped := append([]string{}, order...)
			swapped[i], swapped[i+1] = swapped[i+1], swapped[i]
			if err := verifyAllIn(root, c, swapped, ""); err == nil {
				t.Fatalf("a reader that decides %s before %s passed every vector", b, a)
			}
		})
	}
}

func TestWorkloadManifestNamesItsSurface(t *testing.T) {
	c, err := Load(filepath.Join(testRoot(t), WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	if c.Manifest.Surface != SurfaceWorkload {
		t.Fatalf("surface %q, want %q", c.Manifest.Surface, SurfaceWorkload)
	}
}

// Every accepted vector carries exactly the workload typ and no member of a
// user access token; the typ census below names every refusal form the ruling
// lists, so deleting one is red.
func TestWorkloadAcceptedVectorsCarryTheExactTypAndNoUserTokenMember(t *testing.T) {
	c, err := Load(filepath.Join(testRoot(t), WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	accepted := 0
	for _, e := range c.Manifest.Vectors {
		if e.Expect != Accept {
			continue
		}
		accepted++
		v := c.Vectors[e.File]
		if !strings.Contains(v.HeaderJSON, `"typ":"workload+jwt"`) {
			t.Errorf("%s: header %s lacks the exact typ", e.File, v.HeaderJSON)
		}
		for _, member := range []string{`"org_id"`, `"sid"`, `"scope"`} {
			if strings.Contains(v.PayloadJSON, member) {
				t.Errorf("%s: an accepted workload token carries %s", e.File, member)
			}
		}
	}
	if accepted == 0 {
		t.Fatal("no accepted vector")
	}
}

func TestWorkloadTypRefusalFormsAreAllInTheCorpus(t *testing.T) {
	c, err := Load(filepath.Join(testRoot(t), WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	wantTyp := map[string]bool{"at+jwt": false, "JWT": false, "jwt": false, "WORKLOAD+JWT": false, "Workload+JWT": false, "application/workload+jwt": false, "workload+jwt ": false, "": false}
	absent := false
	for _, e := range c.Manifest.Vectors {
		if e.Reason != ReasonContract {
			continue
		}
		var header map[string]any
		if err := json.Unmarshal([]byte(c.Vectors[e.File].HeaderJSON), &header); err != nil {
			continue
		}
		typ, has := header["typ"].(string)
		if !has {
			absent = absent || header["typ"] == nil && !strings.Contains(c.Vectors[e.File].HeaderJSON, `"typ"`)
			continue
		}
		if _, listed := wantTyp[typ]; listed {
			wantTyp[typ] = true
		}
	}
	for typ, seen := range wantTyp {
		if !seen {
			t.Errorf("no contract refusal vector carries typ %q", typ)
		}
	}
	if !absent {
		t.Error("no contract refusal vector omits typ")
	}
}

func TestWorkloadLifetimeRowsStraddleTheCeiling(t *testing.T) {
	c, err := Load(filepath.Join(testRoot(t), WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	var atCeiling, over bool
	for _, e := range c.Manifest.Vectors {
		var claims struct{ Iat, Exp int64 }
		if err := json.Unmarshal([]byte(c.Vectors[e.File].PayloadJSON), &claims); err != nil {
			continue
		}
		life := claims.Exp - claims.Iat
		if e.Expect == Accept && life == WorkloadCeiling {
			atCeiling = true
		}
		if e.Reason == ReasonContract && e.Stage == "client_rule" && life > WorkloadCeiling {
			over = true
		}
		if e.Expect == Accept && life > WorkloadCeiling {
			t.Errorf("%s: an accepted vector lives %d s", e.File, life)
		}
	}
	if !atCeiling || !over {
		t.Fatalf("ceiling rows: at the ceiling %v, over it %v", atCeiling, over)
	}
}

// No vector, key or document of the corpus may look like a real credential.
func TestWorkloadCorpusHoldsNoCredentialShapedValue(t *testing.T) {
	forbidden := []string{"fcacr_", "fcpush_", "svc_acr_", "svc_worker_", "sk_live_", "sk_test_", "whsec_", "ghp_", "ghs_", "github_pat_", "xoxb-", "AKIA", "AIza", "-----BEGIN"}
	err := filepath.WalkDir(filepath.Join(testRoot(t), WorkloadDir), func(path string, e os.DirEntry, err error) error {
		if err != nil || e.IsDir() {
			return err
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, prefix := range forbidden {
			if strings.Contains(string(raw), prefix) {
				t.Errorf("%s contains the credential-shaped prefix %q", path, prefix)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// A workload token and a user access token must never be decided as each other.
func TestWorkloadContractRefusesTheAccessTokenCorpus(t *testing.T) {
	root := testRoot(t)
	access, err := Load(filepath.Join(root, Dir))
	if err != nil {
		t.Fatal(err)
	}
	asWorkload := *access
	asWorkload.Manifest.Surface = SurfaceWorkload
	for _, e := range access.Manifest.Vectors {
		if e.Expect != Accept {
			continue
		}
		got, err := Check(root, &asWorkload, e)
		if err != nil {
			t.Fatal(err)
		}
		if got.Reason != ReasonContract {
			t.Errorf("%s: a valid user access token was decided %q by the workload contract", e.File, got.Reason)
		}
	}
}

// A verifier that validates the schema and none of the client-only rules (no
// duplicate-member refusal, no integer-token rule, no nbf/iat/exp ordering or
// ceiling) must fail the corpus, and the rows that catch it must include the
// decimal-form NumericDate and the nbf-before-iat vectors.
func TestWorkloadCorpusFailsAReaderThatSkipsTheClientRules(t *testing.T) {
	root := testRoot(t)
	c, err := Load(filepath.Join(root, WorkloadDir))
	if err != nil {
		t.Fatal(err)
	}
	err = verifyAll(root, c, skipClientRules)
	if err == nil {
		t.Fatal("a reader that skips every client rule passed every vector")
	}
	for _, row := range []string{"invalid-exp-decimal-form.json", "invalid-nbf-before-iat.json", "invalid-lifetime-over-the-ceiling.json", "invalid-exp-not-after-nbf.json", "invalid-duplicate-claim-aud.json"} {
		if !strings.Contains(err.Error(), row+":") {
			t.Errorf("the client-rule mutant was not caught by %s", row)
		}
	}
}

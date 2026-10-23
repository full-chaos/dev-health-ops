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

// workloadTokenManifest mirrors contracts/auth/v1/examples/workload-token/manifest.json.
type workloadTokenManifest struct {
	Accept []struct {
		File string `json:"file"`
	} `json:"accept"`
	Reject []struct {
		File                   string `json:"file"`
		ExpectInstanceLocation string `json:"expect_instance_location"`
		ExpectKeyword          string `json:"expect_keyword"`
		Why                    string `json:"why"`
	} `json:"reject"`
	RejectByClient []struct {
		File string `json:"file"`
		Why  string `json:"why"`
	} `json:"reject_by_client"`
}

const workloadTokenFixtureDir = "contracts/auth/v1/examples/workload-token"

func loadWorkloadTokenManifest(t *testing.T) workloadTokenManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), workloadTokenFixtureDir, "manifest.json"))
	if err != nil {
		t.Fatalf("reading workload-token manifest: %v", err)
	}
	var manifest workloadTokenManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parsing workload-token manifest: %v", err)
	}
	return manifest
}

func loadWorkloadTokenFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), workloadTokenFixtureDir, name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return raw
}

func decodeWorkloadTokenFixture(t *testing.T, name string) any {
	t.Helper()
	var document any
	if err := json.Unmarshal(loadWorkloadTokenFixture(t, name), &document); err != nil {
		t.Fatalf("fixture %s is not valid JSON: %v", name, err)
	}
	return document
}

func TestTheWorkloadTokenManifestIsNotEmpty(t *testing.T) {
	manifest := loadWorkloadTokenManifest(t)
	if len(manifest.Accept) == 0 || len(manifest.Reject) == 0 || len(manifest.RejectByClient) == 0 {
		t.Fatal("a manifest category is empty; every table-driven test below would pass vacuously")
	}
}

func TestEveryWorkloadTokenFixtureOnDiskIsClaimedByTheManifest(t *testing.T) {
	manifest := loadWorkloadTokenManifest(t)
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
	entries, err := os.ReadDir(filepath.Join(testRoot(t), workloadTokenFixtureDir))
	if err != nil {
		t.Fatalf("reading fixture dir: %v", err)
	}
	var unclaimed, missing []string
	onDisk := map[string]bool{}
	for _, entry := range entries {
		if entry.IsDir() || entry.Name() == "manifest.json" || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		onDisk[entry.Name()] = true
		if !claimed[entry.Name()] {
			unclaimed = append(unclaimed, entry.Name())
		}
	}
	for name := range claimed {
		if !onDisk[name] {
			missing = append(missing, name)
		}
	}
	sort.Strings(unclaimed)
	sort.Strings(missing)
	if len(unclaimed) > 0 || len(missing) > 0 {
		t.Fatalf("unclaimed on disk: %v; claimed but absent: %v", unclaimed, missing)
	}
}

func TestAcceptedWorkloadTokenFixturesValidateAndParse(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadWorkloadTokenManifest(t).Accept {
		t.Run(entry.File, func(t *testing.T) {
			if err := Validate(root, WorkloadTokenSurface, decodeWorkloadTokenFixture(t, entry.File)); err != nil {
				t.Fatalf("accept fixture was rejected by the schema: %v", err)
			}
			token, err := ParseWorkloadToken(root, loadWorkloadTokenFixture(t, entry.File))
			if err != nil {
				t.Fatalf("accept fixture was refused by the client: %v", err)
			}
			if token.Header.Alg != "EdDSA" || token.Header.Typ != WorkloadTokenTyp || token.Claims.Sub == "" || token.Claims.PrincipalType != "workload" {
				t.Fatalf("parsed token lost a member: %+v", token)
			}
		})
	}
}

func TestRejectedWorkloadTokenFixturesFailByTheDeclaredKeyword(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadWorkloadTokenManifest(t).Reject {
		t.Run(entry.File, func(t *testing.T) {
			err := Validate(root, WorkloadTokenSurface, decodeWorkloadTokenFixture(t, entry.File))
			if err == nil {
				t.Fatalf("validated cleanly; expected rejection by %q (%s)", entry.ExpectKeyword, entry.Why)
			}
			token, ok := errorTokenFor(entry.ExpectKeyword)
			if !ok {
				t.Fatalf("no Go message token registered for keyword %q", entry.ExpectKeyword)
			}
			if !strings.Contains(err.Error(), token) {
				t.Fatalf("rejected, but not by %q. token %q absent from: %v", entry.ExpectKeyword, token, err)
			}
			if _, err := ParseWorkloadToken(root, loadWorkloadTokenFixture(t, entry.File)); err == nil {
				t.Fatal("the schema refused the fixture but ParseWorkloadToken accepted it")
			}
		})
	}
}

// workloadTokenClientRefusals is the refusal each reject_by_client fixture must
// draw from ParseWorkloadToken, so a fixture refused for another reason (a
// schema violation among them) fails instead of passing.
var workloadTokenClientRefusals = map[string]string{
	"client-rejected-lifetime-over-the-ceiling.json": "refused at client_rule lifetime_over_ceiling",
	"client-rejected-lifetime-one-hour.json":         "refused at client_rule lifetime_over_ceiling",
	"client-rejected-exp-equals-nbf.json":            "refused at client_rule exp_not_after_nbf",
	"client-rejected-nbf-before-iat.json":            "refused at client_rule nbf_before_iat",
	"client-rejected-duplicate-claim-aud.json":       "refused at duplicate_member",
	"client-rejected-duplicate-header-typ.json":      "refused at duplicate_member",
	"client-rejected-exp-decimal-form.json":          "refused at decode",
}

func TestClientEnforcedWorkloadTokenFixturesValidateButTheClientRefusesThemForTheirRule(t *testing.T) {
	root := testRoot(t)
	entries := loadWorkloadTokenManifest(t).RejectByClient
	claimed := map[string]bool{}
	for _, entry := range entries {
		claimed[entry.File] = true
		t.Run(entry.File, func(t *testing.T) {
			want, ok := workloadTokenClientRefusals[entry.File]
			if !ok {
				t.Fatal("no expected refusal registered for this fixture")
			}
			if err := Validate(root, WorkloadTokenSurface, decodeWorkloadTokenFixture(t, entry.File)); err != nil {
				t.Fatalf("this fixture is meant to be VALID against the schema; schema said: %v", err)
			}
			_, err := ParseWorkloadToken(root, loadWorkloadTokenFixture(t, entry.File))
			if err == nil {
				t.Fatalf("the client accepted a document only it can refuse (%s)", entry.Why)
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("refused, but not for its rule: want %q in %v", want, err)
			}
		})
	}
	for file := range workloadTokenClientRefusals {
		if !claimed[file] {
			t.Errorf("refusal registered for %s, which the manifest does not list", file)
		}
	}
}

func TestTheWorkloadTokenLifetimeCeilingIs300Seconds(t *testing.T) {
	if WorkloadTokenLifetimeCeiling != 300*time.Second {
		t.Fatalf("WorkloadTokenLifetimeCeiling = %s, want 300s", WorkloadTokenLifetimeCeiling)
	}
	root := testRoot(t)
	wt, err := ParseWorkloadToken(root, loadWorkloadTokenFixture(t, "valid-lifetime-at-the-ceiling.json"))
	if err != nil {
		t.Fatalf("a token at the ceiling was refused: %v", err)
	}
	if got := time.Duration(wt.Claims.Exp-wt.Claims.Iat) * time.Second; got != WorkloadTokenLifetimeCeiling {
		t.Fatalf("the at-ceiling fixture lives %s, want %s", got, WorkloadTokenLifetimeCeiling)
	}
	if _, err := ParseWorkloadToken(root, loadWorkloadTokenFixture(t, "client-rejected-lifetime-over-the-ceiling.json")); err == nil ||
		!strings.Contains(err.Error(), "lifetime_over_ceiling") {
		t.Fatalf("one second over the ceiling: err = %v, want a ceiling refusal", err)
	}
	if got := WorkloadTokenLifetimeCeiling; got >= AccessTokenLifetimeCeiling {
		t.Fatalf("a workload token must live shorter than a user access token (%s vs %s)", got, AccessTokenLifetimeCeiling)
	}
}

func TestTheWorkloadTokenTypIsExactAndDiffersFromTheUserAccessTokens(t *testing.T) {
	if WorkloadTokenTyp != "workload+jwt" {
		t.Fatalf("WorkloadTokenTyp = %q", WorkloadTokenTyp)
	}
	root := testRoot(t)
	base := decodeWorkloadTokenFixture(t, "valid-auth-service.json").(map[string]any)
	header := base["header"].(map[string]any)
	for _, typ := range []string{"at+jwt", "JWT", "jwt", "WORKLOAD+JWT", "Workload+Jwt", "workload+jwt ", " workload+jwt", "workload+jwt\n", "application/workload+jwt", "workload", "workload+jwt+", ""} {
		document := map[string]any{"header": withMember(header, "typ", typ), "claims": base["claims"]}
		if err := Validate(root, WorkloadTokenSurface, document); err == nil {
			t.Errorf("typ %q validated", typ)
		}
	}
	userToken := decodeAccessTokenFixture(t, "valid-auth-service.json").(map[string]any)
	if err := Validate(root, WorkloadTokenSurface, userToken); err == nil {
		t.Error("a user access token validated as a workload token")
	}
	if err := Validate(root, AccessTokenSurface, base); err == nil {
		t.Error("a workload token validated as a user access token")
	}
}

func TestAWorkloadTokenWithTrailingContentIsRefused(t *testing.T) {
	valid := loadWorkloadTokenFixture(t, "valid-auth-service.json")
	for name, trailing := range map[string]string{"null": "\nnull", "second-doc": "\n{}"} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseWorkloadToken(testRoot(t), append(append([]byte{}, valid...), trailing...)); err == nil {
				t.Fatal("trailing content was ignored")
			}
		})
	}
}

// workloadTokenAudiences mirrors contracts/auth/v1/vocabulary/audiences.json.
type workloadTokenAudiences struct {
	Audiences []struct {
		Audience string   `json:"audience"`
		Classes  []string `json:"classes"`
	} `json:"audiences"`
}

func TestTheWorkloadTokenAudienceEnumIsTheWorkloadRowsOfTheVocabulary(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "vocabulary", "audiences.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vocabulary workloadTokenAudiences
	if err := json.Unmarshal(raw, &vocabulary); err != nil {
		t.Fatal(err)
	}
	var workloadAudiences, otherAudiences []string
	for _, row := range vocabulary.Audiences {
		carried := false
		for _, class := range row.Classes {
			carried = carried || class == WorkloadTokenClass
		}
		if carried {
			workloadAudiences = append(workloadAudiences, row.Audience)
		} else {
			otherAudiences = append(otherAudiences, row.Audience)
		}
	}
	schemaRaw, err := os.ReadFile(SchemaPath(root, WorkloadTokenSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Claims struct {
				Properties struct {
					Aud struct {
						Enum []string `json:"enum"`
					} `json:"aud"`
				} `json:"properties"`
			} `json:"claims"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(schemaRaw, &schema); err != nil {
		t.Fatal(err)
	}
	enum := append([]string{}, schema.Properties.Claims.Properties.Aud.Enum...)
	sort.Strings(enum)
	sort.Strings(workloadAudiences)
	if len(enum) == 0 || strings.Join(enum, ",") != strings.Join(workloadAudiences, ",") {
		t.Fatalf("schema aud enum %v, vocabulary workload_token audiences %v", enum, workloadAudiences)
	}
	if len(otherAudiences) == 0 {
		t.Fatal("the vocabulary names an audience no workload token carries; the refusal below would pass vacuously otherwise")
	}
	base := decodeWorkloadTokenFixture(t, "valid-auth-service.json").(map[string]any)
	for _, audience := range otherAudiences {
		document := map[string]any{"header": base["header"], "claims": withMember(base["claims"].(map[string]any), "aud", audience)}
		if err := Validate(root, WorkloadTokenSurface, document); err == nil {
			t.Errorf("audience %q that no workload token carries validated", audience)
		}
	}
}

func TestAWorkloadTokenHasNoOrganizationSessionOrScope(t *testing.T) {
	root := testRoot(t)
	base := decodeWorkloadTokenFixture(t, "valid-auth-service.json").(map[string]any)
	claims := base["claims"].(map[string]any)
	for member, value := range map[string]any{"org_id": "00000000-0000-4000-8000-000000000001", "sid": "ses_AAAAAAAAAAAAAAAAAAAAAQ", "scope": "api:access"} {
		document := map[string]any{"header": base["header"], "claims": withMember(claims, member, value)}
		if err := Validate(root, WorkloadTokenSurface, document); err == nil {
			t.Errorf("a workload token with %s validated", member)
		}
	}
}

// TestAWorkloadTokenRefusalCarriesNoMemberValue: a refusal is logged by its
// caller, and the members of a token are credential material, so no error of
// ParseWorkloadToken may repeat a header or claims value, even a duplicate's.
func TestAWorkloadTokenRefusalCarriesNoMemberValue(t *testing.T) {
	root := testRoot(t)
	manifest := loadWorkloadTokenManifest(t)
	var files []string
	for _, r := range manifest.Reject {
		files = append(files, r.File)
	}
	for _, r := range manifest.RejectByClient {
		files = append(files, r.File)
	}
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			raw := loadWorkloadTokenFixture(t, file)
			_, err := ParseWorkloadToken(root, raw)
			if err == nil {
				t.Fatal("a refused fixture was accepted")
			}
			// The surface name opens every message; a value such as "access" is
			// part of it, not a repeat.
			message := strings.TrimPrefix(err.Error(), WorkloadTokenSurface+": ")
			for _, value := range memberValues(t, raw) {
				if strings.Contains(message, value) {
					t.Fatalf("the refusal repeats the member value %q: %v", value, err)
				}
			}
		})
	}
}

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

// serviceAccountTokenManifest mirrors contracts/auth/v1/examples/service-account-token/manifest.json.
type serviceAccountTokenManifest struct {
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

const serviceAccountTokenFixtureDir = "contracts/auth/v1/examples/service-account-token"

func loadServiceAccountTokenManifest(t *testing.T) serviceAccountTokenManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), serviceAccountTokenFixtureDir, "manifest.json"))
	if err != nil {
		t.Fatalf("reading service-account-token manifest: %v", err)
	}
	var manifest serviceAccountTokenManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parsing service-account-token manifest: %v", err)
	}
	return manifest
}

func loadServiceAccountTokenFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), serviceAccountTokenFixtureDir, name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return raw
}

func decodeServiceAccountTokenFixture(t *testing.T, name string) any {
	t.Helper()
	var document any
	if err := json.Unmarshal(loadServiceAccountTokenFixture(t, name), &document); err != nil {
		t.Fatalf("fixture %s is not valid JSON: %v", name, err)
	}
	return document
}

func TestTheServiceAccountTokenManifestIsNotEmpty(t *testing.T) {
	manifest := loadServiceAccountTokenManifest(t)
	if len(manifest.Accept) == 0 || len(manifest.Reject) == 0 || len(manifest.RejectByClient) == 0 {
		t.Fatal("a manifest category is empty; every table-driven test below would pass vacuously")
	}
}

func TestEveryServiceAccountTokenFixtureOnDiskIsClaimedByTheManifest(t *testing.T) {
	manifest := loadServiceAccountTokenManifest(t)
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
	entries, err := os.ReadDir(filepath.Join(testRoot(t), serviceAccountTokenFixtureDir))
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

func TestAcceptedServiceAccountTokenFixturesValidateAndParse(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadServiceAccountTokenManifest(t).Accept {
		t.Run(entry.File, func(t *testing.T) {
			if err := Validate(root, ServiceAccountTokenSurface, decodeServiceAccountTokenFixture(t, entry.File)); err != nil {
				t.Fatalf("accept fixture was rejected by the schema: %v", err)
			}
			token, err := ParseServiceAccountToken(root, loadServiceAccountTokenFixture(t, entry.File))
			if err != nil {
				t.Fatalf("accept fixture was refused by the client: %v", err)
			}
			if token.Header.Alg != "EdDSA" || token.Header.Typ != ServiceAccountTokenTyp || token.Claims.Sub == "" || token.Claims.PrincipalType != ServiceAccountTokenPrincipalType || token.Claims.OrgID == "" || token.Claims.Cid == "" || token.Claims.Aud != ServiceAccountTokenAudience {
				t.Fatalf("parsed token lost a member: %+v", token)
			}
		})
	}
}

func TestRejectedServiceAccountTokenFixturesFailByTheDeclaredKeyword(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadServiceAccountTokenManifest(t).Reject {
		t.Run(entry.File, func(t *testing.T) {
			err := Validate(root, ServiceAccountTokenSurface, decodeServiceAccountTokenFixture(t, entry.File))
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
			if _, err := ParseServiceAccountToken(root, loadServiceAccountTokenFixture(t, entry.File)); err == nil {
				t.Fatal("the schema refused the fixture but ParseServiceAccountToken accepted it")
			}
		})
	}
}

// serviceAccountTokenClientRefusals is the refusal each reject_by_client fixture must
// draw from ParseServiceAccountToken, so a fixture refused for another reason (a
// schema violation among them) fails instead of passing.
var serviceAccountTokenClientRefusals = map[string]string{
	"client-rejected-lifetime-over-the-ceiling.json": "refused at client_rule lifetime_over_ceiling",
	"client-rejected-lifetime-one-hour.json":         "refused at client_rule lifetime_over_ceiling",
	"client-rejected-exp-equals-nbf.json":            "refused at client_rule exp_not_after_nbf",
	"client-rejected-nbf-before-iat.json":            "refused at client_rule nbf_before_iat",
	"client-rejected-duplicate-claim-aud.json":       "refused at duplicate_member",
	"client-rejected-duplicate-header-typ.json":      "refused at duplicate_member",
	"client-rejected-exp-decimal-form.json":          "refused at decode",
}

func TestClientEnforcedServiceAccountTokenFixturesValidateButTheClientRefusesThemForTheirRule(t *testing.T) {
	root := testRoot(t)
	entries := loadServiceAccountTokenManifest(t).RejectByClient
	claimed := map[string]bool{}
	for _, entry := range entries {
		claimed[entry.File] = true
		t.Run(entry.File, func(t *testing.T) {
			want, ok := serviceAccountTokenClientRefusals[entry.File]
			if !ok {
				t.Fatal("no expected refusal registered for this fixture")
			}
			if err := Validate(root, ServiceAccountTokenSurface, decodeServiceAccountTokenFixture(t, entry.File)); err != nil {
				t.Fatalf("this fixture is meant to be VALID against the schema; schema said: %v", err)
			}
			_, err := ParseServiceAccountToken(root, loadServiceAccountTokenFixture(t, entry.File))
			if err == nil {
				t.Fatalf("the client accepted a document only it can refuse (%s)", entry.Why)
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("refused, but not for its rule: want %q in %v", want, err)
			}
		})
	}
	for file := range serviceAccountTokenClientRefusals {
		if !claimed[file] {
			t.Errorf("refusal registered for %s, which the manifest does not list", file)
		}
	}
}

func TestTheServiceAccountTokenLifetimeCeilingIs300Seconds(t *testing.T) {
	if ServiceAccountTokenLifetimeCeiling != 300*time.Second {
		t.Fatalf("ServiceAccountTokenLifetimeCeiling = %s, want 300s", ServiceAccountTokenLifetimeCeiling)
	}
	root := testRoot(t)
	wt, err := ParseServiceAccountToken(root, loadServiceAccountTokenFixture(t, "valid-lifetime-at-the-ceiling.json"))
	if err != nil {
		t.Fatalf("a token at the ceiling was refused: %v", err)
	}
	if got := time.Duration(wt.Claims.Exp-wt.Claims.Iat) * time.Second; got != ServiceAccountTokenLifetimeCeiling {
		t.Fatalf("the at-ceiling fixture lives %s, want %s", got, ServiceAccountTokenLifetimeCeiling)
	}
	if _, err := ParseServiceAccountToken(root, loadServiceAccountTokenFixture(t, "client-rejected-lifetime-over-the-ceiling.json")); err == nil ||
		!strings.Contains(err.Error(), "lifetime_over_ceiling") {
		t.Fatalf("one second over the ceiling: err = %v, want a ceiling refusal", err)
	}
	if got := ServiceAccountTokenLifetimeCeiling; got >= AccessTokenLifetimeCeiling {
		t.Fatalf("a service account token must live shorter than a user access token (%s vs %s)", got, AccessTokenLifetimeCeiling)
	}
}

func TestTheServiceAccountTokenTypIsExactAndDiffersFromTheUserAccessTokens(t *testing.T) {
	if ServiceAccountTokenTyp != "sa+jwt" {
		t.Fatalf("ServiceAccountTokenTyp = %q", ServiceAccountTokenTyp)
	}
	root := testRoot(t)
	base := decodeServiceAccountTokenFixture(t, "valid-dev-health-api.json").(map[string]any)
	header := base["header"].(map[string]any)
	for _, typ := range []string{"at+jwt", "JWT", "jwt", "SA+JWT", "Sa+Jwt", "sa+jwt ", " sa+jwt", "sa+jwt\n", "application/sa+jwt", "workload+jwt", "sa", "sa+jwt+", ""} {
		document := map[string]any{"header": withMember(header, "typ", typ), "claims": base["claims"]}
		if err := Validate(root, ServiceAccountTokenSurface, document); err == nil {
			t.Errorf("typ %q validated", typ)
		}
	}
	userToken := decodeAccessTokenFixture(t, "valid-auth-service.json").(map[string]any)
	if err := Validate(root, ServiceAccountTokenSurface, userToken); err == nil {
		t.Error("a user access token validated as a service account token")
	}
	if err := Validate(root, AccessTokenSurface, base); err == nil {
		t.Error("a service account token validated as a user access token")
	}
}

func TestAServiceAccountTokenWithTrailingContentIsRefused(t *testing.T) {
	valid := loadServiceAccountTokenFixture(t, "valid-dev-health-api.json")
	for name, trailing := range map[string]string{"null": "\nnull", "second-doc": "\n{}"} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseServiceAccountToken(testRoot(t), append(append([]byte{}, valid...), trailing...)); err == nil {
				t.Fatal("trailing content was ignored")
			}
		})
	}
}

// serviceAccountTokenAudiences mirrors contracts/auth/v1/vocabulary/audiences.json.
type serviceAccountTokenAudiences struct {
	Audiences []struct {
		Audience string   `json:"audience"`
		Classes  []string `json:"classes"`
	} `json:"audiences"`
}

func TestTheServiceAccountTokenAudienceIsTheOneRowOfTheVocabularyThatCarriesTheClass(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "vocabulary", "audiences.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vocabulary serviceAccountTokenAudiences
	if err := json.Unmarshal(raw, &vocabulary); err != nil {
		t.Fatal(err)
	}
	var carriedAudiences, otherAudiences []string
	for _, row := range vocabulary.Audiences {
		carried := false
		for _, class := range row.Classes {
			carried = carried || class == ServiceAccountTokenClass
		}
		if carried {
			carriedAudiences = append(carriedAudiences, row.Audience)
		} else {
			otherAudiences = append(otherAudiences, row.Audience)
		}
	}
	schemaRaw, err := os.ReadFile(SchemaPath(root, ServiceAccountTokenSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			Claims struct {
				Properties struct {
					Aud struct {
						Const string `json:"const"`
					} `json:"aud"`
				} `json:"properties"`
			} `json:"claims"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(schemaRaw, &schema); err != nil {
		t.Fatal(err)
	}
	sort.Strings(carriedAudiences)
	if schema.Properties.Claims.Properties.Aud.Const != ServiceAccountTokenAudience || strings.Join(carriedAudiences, ",") != ServiceAccountTokenAudience {
		t.Fatalf("schema aud const %q, constant %q, vocabulary service_account_token audiences %v", schema.Properties.Claims.Properties.Aud.Const, ServiceAccountTokenAudience, carriedAudiences)
	}
	if len(otherAudiences) == 0 {
		t.Fatal("the vocabulary names an audience no service account token carries; the refusal below would pass vacuously otherwise")
	}
	base := decodeServiceAccountTokenFixture(t, "valid-dev-health-api.json").(map[string]any)
	for _, audience := range otherAudiences {
		document := map[string]any{"header": base["header"], "claims": withMember(base["claims"].(map[string]any), "aud", audience)}
		if err := Validate(root, ServiceAccountTokenSurface, document); err == nil {
			t.Errorf("audience %q that no service account token carries validated", audience)
		}
	}
}

func TestAServiceAccountTokenNamesItsOrganizationAndKeyAndHasNoSessionOrScope(t *testing.T) {
	root := testRoot(t)
	base := decodeServiceAccountTokenFixture(t, "valid-dev-health-api.json").(map[string]any)
	claims := base["claims"].(map[string]any)
	for member, value := range map[string]any{"sid": "ses_AAAAAAAAAAAAAAAAAAAAAQ", "scope": "api:access", "act": map[string]any{"sub": "prn_AAAAAAAAAAAAAAAAAAAAAA"}} {
		document := map[string]any{"header": base["header"], "claims": withMember(claims, member, value)}
		if err := Validate(root, ServiceAccountTokenSurface, document); err == nil {
			t.Errorf("a service account token with %s validated", member)
		}
	}
	for _, member := range []string{"org_id", "cid", "sub", "jti", "nbf", "principal_type"} {
		without := map[string]any{}
		for k, v := range claims {
			if k != member {
				without[k] = v
			}
		}
		if err := Validate(root, ServiceAccountTokenSurface, map[string]any{"header": base["header"], "claims": without}); err == nil {
			t.Errorf("a service account token without %s validated", member)
		}
	}
}

// TestAServiceAccountTokenRefusalCarriesNoMemberValue: a refusal is logged by its
// caller, and the members of a token are credential material, so no error of
// ParseServiceAccountToken may repeat a header or claims value, even a duplicate's.
func TestAServiceAccountTokenRefusalCarriesNoMemberValue(t *testing.T) {
	root := testRoot(t)
	manifest := loadServiceAccountTokenManifest(t)
	var files []string
	for _, r := range manifest.Reject {
		files = append(files, r.File)
	}
	for _, r := range manifest.RejectByClient {
		files = append(files, r.File)
	}
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			raw := loadServiceAccountTokenFixture(t, file)
			_, err := ParseServiceAccountToken(root, raw)
			if err == nil {
				t.Fatal("a refused fixture was accepted")
			}
			// The surface name opens every message; a value such as "access" is
			// part of it, not a repeat.
			message := strings.TrimPrefix(err.Error(), ServiceAccountTokenSurface+": ")
			for _, value := range memberValues(t, raw) {
				if strings.Contains(message, value) {
					t.Fatalf("the refusal repeats the member value %q: %v", value, err)
				}
			}
		})
	}
}

func TestTheServiceAccountAudienceOfTheVocabularyValidatesAndAnArrayStillDoesNot(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "vocabulary", "audiences.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vocabulary serviceAccountTokenAudiences
	if err := json.Unmarshal(raw, &vocabulary); err != nil {
		t.Fatal(err)
	}
	base := decodeServiceAccountTokenFixture(t, "valid-dev-health-api.json").(map[string]any)
	seen := map[string]bool{}
	for _, row := range vocabulary.Audiences {
		for _, class := range row.Classes {
			if class != ServiceAccountTokenClass {
				continue
			}
			seen[row.Audience] = true
			document := map[string]any{"header": base["header"], "claims": withMember(base["claims"].(map[string]any), "aud", row.Audience)}
			if err := Validate(root, ServiceAccountTokenSurface, document); err != nil {
				t.Errorf("service account audience %q was refused: %v", row.Audience, err)
			}
			array := map[string]any{"header": base["header"], "claims": withMember(base["claims"].(map[string]any), "aud", []any{row.Audience})}
			if err := Validate(root, ServiceAccountTokenSurface, array); err == nil {
				t.Errorf("an array holding %q validated", row.Audience)
			}
		}
	}
	if len(seen) != 1 || !seen[ServiceAccountTokenAudience] {
		t.Fatalf("service account token audiences in the vocabulary: %v", seen)
	}
}

func TestAServiceAccountTokenAndAWorkloadTokenAreNeverDecidedAsEachOther(t *testing.T) {
	root := testRoot(t)
	workload := decodeWorkloadTokenFixture(t, "valid-dev-health-api.json")
	if err := Validate(root, ServiceAccountTokenSurface, workload); err == nil {
		t.Error("a workload token validated as a service account token")
	}
	if _, err := ParseServiceAccountToken(root, loadWorkloadTokenFixture(t, "valid-dev-health-api.json")); err == nil {
		t.Error("ParseServiceAccountToken accepted a workload token")
	}
	serviceAccount := decodeServiceAccountTokenFixture(t, "valid-dev-health-api.json")
	if err := Validate(root, WorkloadTokenSurface, serviceAccount); err == nil {
		t.Error("a service account token validated as a workload token")
	}
	if _, err := ParseWorkloadToken(root, loadServiceAccountTokenFixture(t, "valid-dev-health-api.json")); err == nil {
		t.Error("ParseWorkloadToken accepted a service account token")
	}
}

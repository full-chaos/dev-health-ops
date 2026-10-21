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

// accessTokenManifest mirrors contracts/auth/v1/examples/access-token/manifest.json.
type accessTokenManifest struct {
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

const accessTokenFixtureDir = "contracts/auth/v1/examples/access-token"

func loadAccessTokenManifest(t *testing.T) accessTokenManifest {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), accessTokenFixtureDir, "manifest.json"))
	if err != nil {
		t.Fatalf("reading access-token manifest: %v", err)
	}
	var manifest accessTokenManifest
	if err := json.Unmarshal(raw, &manifest); err != nil {
		t.Fatalf("parsing access-token manifest: %v", err)
	}
	return manifest
}

func loadAccessTokenFixture(t *testing.T, name string) []byte {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(testRoot(t), accessTokenFixtureDir, name))
	if err != nil {
		t.Fatalf("reading fixture %s: %v", name, err)
	}
	return raw
}

func decodeAccessTokenFixture(t *testing.T, name string) any {
	t.Helper()
	var document any
	if err := json.Unmarshal(loadAccessTokenFixture(t, name), &document); err != nil {
		t.Fatalf("fixture %s is not valid JSON: %v", name, err)
	}
	return document
}

func TestTheAccessTokenManifestIsNotEmpty(t *testing.T) {
	manifest := loadAccessTokenManifest(t)
	if len(manifest.Accept) == 0 || len(manifest.Reject) == 0 || len(manifest.RejectByClient) == 0 {
		t.Fatal("a manifest category is empty; every table-driven test below would pass vacuously")
	}
}

func TestEveryAccessTokenFixtureOnDiskIsClaimedByTheManifest(t *testing.T) {
	manifest := loadAccessTokenManifest(t)
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
	entries, err := os.ReadDir(filepath.Join(testRoot(t), accessTokenFixtureDir))
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

func TestAcceptedAccessTokenFixturesValidateAndParse(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadAccessTokenManifest(t).Accept {
		t.Run(entry.File, func(t *testing.T) {
			if err := Validate(root, AccessTokenSurface, decodeAccessTokenFixture(t, entry.File)); err != nil {
				t.Fatalf("accept fixture was rejected by the schema: %v", err)
			}
			token, err := ParseAccessToken(root, loadAccessTokenFixture(t, entry.File))
			if err != nil {
				t.Fatalf("accept fixture was refused by the client: %v", err)
			}
			if token.Header.Alg != "EdDSA" || token.Header.Typ != "at+jwt" || token.Claims.Sub == "" || token.Claims.Sid == "" {
				t.Fatalf("parsed token lost a member: %+v", token)
			}
		})
	}
}

func TestRejectedAccessTokenFixturesFailByTheDeclaredKeyword(t *testing.T) {
	root := testRoot(t)
	for _, entry := range loadAccessTokenManifest(t).Reject {
		t.Run(entry.File, func(t *testing.T) {
			err := Validate(root, AccessTokenSurface, decodeAccessTokenFixture(t, entry.File))
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
			if _, err := ParseAccessToken(root, loadAccessTokenFixture(t, entry.File)); err == nil {
				t.Fatal("the schema refused the fixture but ParseAccessToken accepted it")
			}
		})
	}
}

// accessTokenClientRefusals is the refusal each reject_by_client fixture must
// draw from ParseAccessToken, so a fixture refused for another reason (a
// schema violation among them) fails instead of passing.
var accessTokenClientRefusals = map[string]string{
	"client-rejected-lifetime-over-the-ceiling.json": "refused at client_rule lifetime_over_ceiling",
	"client-rejected-exp-equals-nbf.json":            "refused at client_rule exp_not_after_nbf",
	"client-rejected-nbf-before-iat.json":            "refused at client_rule nbf_before_iat",
	"client-rejected-exp-before-iat.json":            "refused at client_rule exp_not_after_nbf",
	"client-rejected-duplicate-claim-aud.json":       "refused at duplicate_member",
	"client-rejected-duplicate-header-alg.json":      "refused at duplicate_member",
	"client-rejected-exp-decimal-form.json":          "refused at decode",
}

func TestClientEnforcedAccessTokenFixturesValidateButTheClientRefusesThemForTheirRule(t *testing.T) {
	root := testRoot(t)
	entries := loadAccessTokenManifest(t).RejectByClient
	claimed := map[string]bool{}
	for _, entry := range entries {
		claimed[entry.File] = true
		t.Run(entry.File, func(t *testing.T) {
			want, ok := accessTokenClientRefusals[entry.File]
			if !ok {
				t.Fatal("no expected refusal registered for this fixture")
			}
			if err := Validate(root, AccessTokenSurface, decodeAccessTokenFixture(t, entry.File)); err != nil {
				t.Fatalf("this fixture is meant to be VALID against the schema; schema said: %v", err)
			}
			_, err := ParseAccessToken(root, loadAccessTokenFixture(t, entry.File))
			if err == nil {
				t.Fatalf("the client accepted a document only it can refuse (%s)", entry.Why)
			}
			if !strings.Contains(err.Error(), want) {
				t.Fatalf("refused, but not for its rule: want %q in %v", want, err)
			}
		})
	}
	for file := range accessTokenClientRefusals {
		if !claimed[file] {
			t.Errorf("refusal registered for %s, which the manifest does not list", file)
		}
	}
}

func TestTheLifetimeCeilingIsTheHS256CompatCeiling(t *testing.T) {
	if AccessTokenLifetimeCeiling != time.Hour {
		t.Fatalf("AccessTokenLifetimeCeiling = %s, want the 60 min HS256-compat ceiling", AccessTokenLifetimeCeiling)
	}
	root := testRoot(t)
	at, err := ParseAccessToken(root, loadAccessTokenFixture(t, "valid-lifetime-at-the-ceiling.json"))
	if err != nil {
		t.Fatalf("a token at the ceiling was refused: %v", err)
	}
	if got := time.Duration(at.Claims.Exp-at.Claims.Iat) * time.Second; got != AccessTokenLifetimeCeiling {
		t.Fatalf("the at-ceiling fixture lives %s, want %s", got, AccessTokenLifetimeCeiling)
	}
	if _, err := ParseAccessToken(root, loadAccessTokenFixture(t, "client-rejected-lifetime-over-the-ceiling.json")); err == nil ||
		!strings.Contains(err.Error(), "ceiling") {
		t.Fatalf("one second over the ceiling: err = %v, want a ceiling refusal", err)
	}
}

func TestAnAccessTokenWithTrailingContentIsRefused(t *testing.T) {
	valid := loadAccessTokenFixture(t, "valid-api.json")
	for name, trailing := range map[string]string{"null": "\nnull", "second-doc": "\n{}"} {
		t.Run(name, func(t *testing.T) {
			if _, err := ParseAccessToken(testRoot(t), append(append([]byte{}, valid...), trailing...)); err == nil {
				t.Fatal("trailing content was ignored")
			}
		})
	}
}

// accessTokenAudiences mirrors contracts/auth/v1/vocabulary/audiences.json.
type accessTokenAudiences struct {
	Audiences []struct {
		Audience string   `json:"audience"`
		Classes  []string `json:"classes"`
	} `json:"audiences"`
}

func TestTheAccessTokenAudienceEnumIsTheUserTokenRowsOfTheVocabulary(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(ContractsDir(root), "vocabulary", "audiences.json"))
	if err != nil {
		t.Fatal(err)
	}
	var vocabulary accessTokenAudiences
	if err := json.Unmarshal(raw, &vocabulary); err != nil {
		t.Fatal(err)
	}
	var userAudiences, otherAudiences []string
	for _, row := range vocabulary.Audiences {
		user := false
		for _, class := range row.Classes {
			user = user || class == UserAccessTokenClass
		}
		if user {
			userAudiences = append(userAudiences, row.Audience)
		} else {
			otherAudiences = append(otherAudiences, row.Audience)
		}
	}
	schemaRaw, err := os.ReadFile(SchemaPath(root, AccessTokenSurface))
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
	sort.Strings(userAudiences)
	if strings.Join(enum, ",") != strings.Join(userAudiences, ",") {
		t.Fatalf("schema aud enum %v, vocabulary user_access_token audiences %v", enum, userAudiences)
	}
	if len(otherAudiences) == 0 {
		t.Fatal("the vocabulary names no audience of another class; the refusal below would pass vacuously")
	}
	base := decodeAccessTokenFixture(t, "valid-api.json").(map[string]any)
	for _, audience := range otherAudiences {
		document := map[string]any{"header": base["header"], "claims": withMember(base["claims"].(map[string]any), "aud", audience)}
		if err := Validate(root, AccessTokenSurface, document); err == nil {
			t.Errorf("audience %q of another class validated in a user access token", audience)
		}
	}
}

func withMember(object map[string]any, key string, value any) map[string]any {
	out := make(map[string]any, len(object))
	for k, v := range object {
		out[k] = v
	}
	out[key] = value
	return out
}

// TestAnAccessTokenRefusalCarriesNoMemberValue: a refusal is logged by its
// caller, and the members of a token are credential material, so no error of
// ParseAccessToken may repeat a header or claims value, even a duplicate's.
func TestAnAccessTokenRefusalCarriesNoMemberValue(t *testing.T) {
	root := testRoot(t)
	manifest := loadAccessTokenManifest(t)
	var files []string
	for _, r := range manifest.Reject {
		files = append(files, r.File)
	}
	for _, r := range manifest.RejectByClient {
		files = append(files, r.File)
	}
	for _, file := range files {
		t.Run(file, func(t *testing.T) {
			raw := loadAccessTokenFixture(t, file)
			_, err := ParseAccessToken(root, raw)
			if err == nil {
				t.Fatal("a refused fixture was accepted")
			}
			// The surface name opens every message; a value such as "access" is
			// part of it, not a repeat.
			message := strings.TrimPrefix(err.Error(), AccessTokenSurface+": ")
			for _, value := range memberValues(t, raw) {
				if strings.Contains(message, value) {
					t.Fatalf("the refusal repeats the member value %q: %v", value, err)
				}
			}
		})
	}
}

// memberValues returns every scalar value token of raw (never a member name),
// as its literal text, duplicates included. Values under four characters are
// skipped: they match ordinary words of any message.
func memberValues(t *testing.T, raw []byte) []string {
	t.Helper()
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	dec.UseNumber()
	var values []string
	type frame struct{ object, expectKey bool }
	var stack []frame
	for {
		tok, err := dec.Token()
		if err != nil {
			break
		}
		isKey := len(stack) > 0 && stack[len(stack)-1].object && stack[len(stack)-1].expectKey
		if len(stack) > 0 && stack[len(stack)-1].object {
			stack[len(stack)-1].expectKey = !stack[len(stack)-1].expectKey
		}
		switch v := tok.(type) {
		case json.Delim:
			switch v {
			case '{':
				stack = append(stack, frame{object: true, expectKey: true})
			case '[':
				stack = append(stack, frame{})
			default:
				stack = stack[:len(stack)-1]
				if len(stack) > 0 && stack[len(stack)-1].object {
					stack[len(stack)-1].expectKey = true
				}
			}
		case string:
			if !isKey && len(strings.TrimSpace(v)) >= 4 {
				values = append(values, strings.TrimSpace(v))
			}
		case json.Number:
			if len(v.String()) >= 4 {
				values = append(values, v.String())
			}
		}
	}
	if len(values) == 0 {
		t.Fatal("no member value found; the check would pass vacuously")
	}
	return values
}

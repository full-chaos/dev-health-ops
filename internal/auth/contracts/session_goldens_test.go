package contracts

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

// Built from a full repo path so the Go path-filter resolver sees a real file.
var sessionManifestName = filepath.Base("contracts/auth/v1/examples/session/manifest.json")

type sessionManifest struct {
	Accept []struct {
		File string `json:"file"`
	} `json:"accept"`
	Reject []struct {
		File                   string `json:"file"`
		ExpectInstanceLocation string `json:"expect_instance_location"`
		ExpectKeyword          string `json:"expect_keyword"`
	} `json:"reject"`
	RejectByClient []struct {
		File string `json:"file"`
	} `json:"reject_by_client"`
}

var sessionKeywordTokens = map[string]string{
	"additionalProperties": "unexpected additional properties",
	"const":                "const:",
	"enum":                 "enum:",
	"maximum":              "maximum:",
	"minItems":             "minItems:",
	"minimum":              "minimum:",
	"not":                  "not:",
	"pattern":              "pattern:",
	"required":             "required:",
	"type":                 "type:",
	"uniqueItems":          "uniqueItems:",
}

type sessionSurface struct {
	name  string
	dir   string
	parse func(root string, raw []byte) error
}

func sessionSurfaces() []sessionSurface {
	return []sessionSurface{
		{SessionSurface, "session", func(r string, b []byte) error { _, err := ParseSession(r, b); return err }},
		{RefreshRequestSurface, "refresh-request", func(r string, b []byte) error { _, err := ParseRefreshRequest(r, b); return err }},
		{RefreshResponseSurface, "refresh-response", func(r string, b []byte) error { _, err := ParseRefreshResponse(r, b); return err }},
		{RefreshResponseV2Surface, "refresh-response.v2", func(r string, b []byte) error { _, err := ParseRefreshResponse(r, b); return err }},
		{AuthOutcomeSurface, "auth-outcome", func(r string, b []byte) error { _, err := ParseAuthOutcome(r, b); return err }},
	}
}

func loadSessionCorpus(t *testing.T, s sessionSurface) (string, sessionManifest) {
	t.Helper()
	dir := filepath.Join(testRoot(t), "contracts/auth/v1/examples", s.dir)
	raw, err := os.ReadFile(filepath.Join(dir, sessionManifestName))
	if err != nil {
		t.Fatal(err)
	}
	var m sessionManifest
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatal(err)
	}
	return dir, m
}

func TestSessionSurfaceCorporaAreNonEmptyAndFullyClaimed(t *testing.T) {
	for _, s := range sessionSurfaces() {
		t.Run(s.name, func(t *testing.T) {
			dir, m := loadSessionCorpus(t, s)
			if len(m.Accept) == 0 || len(m.Reject) == 0 || len(m.RejectByClient) == 0 {
				t.Fatalf("empty group: accept=%d reject=%d client=%d", len(m.Accept), len(m.Reject), len(m.RejectByClient))
			}
			claimed := map[string]bool{}
			for _, e := range m.Accept {
				claimed[e.File] = true
			}
			for _, e := range m.Reject {
				claimed[e.File] = true
			}
			for _, e := range m.RejectByClient {
				claimed[e.File] = true
			}
			entries, err := os.ReadDir(dir)
			if err != nil {
				t.Fatal(err)
			}
			var stray []string
			for _, e := range entries {
				if e.Name() != sessionManifestName && !claimed[e.Name()] {
					stray = append(stray, e.Name())
				}
			}
			if len(stray) > 0 {
				t.Fatalf("unclaimed fixtures: %v", stray)
			}
			for f := range claimed {
				if _, err := os.Stat(filepath.Join(dir, f)); err != nil {
					t.Errorf("claimed but absent: %s", f)
				}
			}
		})
	}
}

func TestSessionSurfaceAcceptFixturesValidateAndParse(t *testing.T) {
	root := testRoot(t)
	for _, s := range sessionSurfaces() {
		dir, m := loadSessionCorpus(t, s)
		for _, e := range m.Accept {
			t.Run(s.dir+"/"+e.File, func(t *testing.T) {
				raw, err := os.ReadFile(filepath.Join(dir, e.File))
				if err != nil {
					t.Fatal(err)
				}
				if err := s.parse(root, raw); err != nil {
					t.Fatalf("accept fixture refused: %v", err)
				}
			})
		}
	}
}

func TestSessionSurfaceRejectFixturesFailByTheDeclaredKeyword(t *testing.T) {
	root := testRoot(t)
	for _, s := range sessionSurfaces() {
		dir, m := loadSessionCorpus(t, s)
		for _, e := range m.Reject {
			t.Run(s.dir+"/"+e.File, func(t *testing.T) {
				token, ok := sessionKeywordTokens[e.ExpectKeyword]
				if !ok {
					t.Fatalf("no Go message token for keyword %q", e.ExpectKeyword)
				}
				raw, err := os.ReadFile(filepath.Join(dir, e.File))
				if err != nil {
					t.Fatal(err)
				}
				var doc any
				if err := json.Unmarshal(raw, &doc); err != nil {
					t.Fatal(err)
				}
				err = Validate(root, s.name, doc)
				if err == nil {
					t.Fatalf("validated cleanly; want %s at %s", e.ExpectKeyword, e.ExpectInstanceLocation)
				}
				if !strings.Contains(err.Error(), token) {
					t.Fatalf("not rejected by %q (token %q): %v", e.ExpectKeyword, token, err)
				}
				if perr := s.parse(root, raw); perr == nil {
					t.Fatal("Validate refused but the parser accepted")
				}
			})
		}
	}
}

func TestSessionSurfaceClientRejectedFixturesPassTheSchemaButAreRefused(t *testing.T) {
	root := testRoot(t)
	for _, s := range sessionSurfaces() {
		dir, m := loadSessionCorpus(t, s)
		for _, e := range m.RejectByClient {
			t.Run(s.dir+"/"+e.File, func(t *testing.T) {
				raw, err := os.ReadFile(filepath.Join(dir, e.File))
				if err != nil {
					t.Fatal(err)
				}
				var doc any
				if err := json.Unmarshal(raw, &doc); err != nil {
					t.Fatalf("fixture is not valid JSON: %v", err)
				}
				if err := Validate(root, s.name, doc); err != nil {
					t.Fatalf("client-rejected fixture must pass the schema, got: %v", err)
				}
				if err := s.parse(root, raw); err == nil {
					t.Fatal("client accepted a document it must refuse")
				}
			})
		}
	}
}

func schemaEnum(t *testing.T, surface string, path ...string) []string {
	t.Helper()
	raw, err := os.ReadFile(SchemaPath(testRoot(t), surface))
	if err != nil {
		t.Fatal(err)
	}
	var node any
	if err := json.Unmarshal(raw, &node); err != nil {
		t.Fatal(err)
	}
	for _, p := range path {
		node = node.(map[string]any)[p]
	}
	var out []string
	for _, v := range node.([]any) {
		out = append(out, v.(string))
	}
	return out
}

func TestAuthOutcomeVocabularyMatchesTheSchema(t *testing.T) {
	got := schemaEnum(t, AuthOutcomeSurface, "properties", "reason_code", "enum")
	want := append([]string(nil), AuthOutcomeReasonCodes...)
	sort.Strings(got)
	sort.Strings(want)
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("schema %v != Go %v", got, want)
	}
	succ := schemaEnum(t, AuthOutcomeSurface, "then", "properties", "reason_code", "enum")
	if !reflect.DeepEqual(succ, AuthOutcomeSuccessReasons) {
		t.Fatalf("schema success reasons %v != Go %v", succ, AuthOutcomeSuccessReasons)
	}
}

func TestEveryAuthOutcomeReasonFitsTheErrorEnvelopeReasonPattern(t *testing.T) {
	raw, err := os.ReadFile(SchemaPath(testRoot(t), ErrorSurface))
	if err != nil {
		t.Fatal(err)
	}
	var schema struct {
		Properties struct {
			ReasonCode struct {
				Pattern string `json:"pattern"`
			} `json:"reason_code"`
		} `json:"properties"`
	}
	if err := json.Unmarshal(raw, &schema); err != nil {
		t.Fatal(err)
	}
	pattern := schema.Properties.ReasonCode.Pattern
	if pattern == "" {
		t.Fatal("error.v1 declares no reason_code pattern")
	}
	for _, code := range AuthOutcomeReasonCodes {
		doc := map[string]any{
			"schema_version": "error.v1", "status": 403, "reason_code": code,
			"request_id": "req_EXAMPLE0000000000000000001", "occurred_at": "2026-10-18T10:00:00Z",
		}
		if err := Validate(testRoot(t), ErrorSurface, doc); err != nil {
			t.Errorf("reason %q is not a valid error.v1 reason_code (%s): %v", code, pattern, err)
		}
	}
}

func TestEdgetokenReasonsAreAllInTheVocabulary(t *testing.T) {
	for _, r := range []string{"malformed", "bad_signature", "audience", "issuer", "expired", "not_yet_valid", "missing_sub", "missing_type", "type_mismatch"} {
		found := false
		for _, c := range AuthOutcomeReasonCodes {
			found = found || c == r
		}
		if !found {
			t.Errorf("edgetoken reason %q missing from the vocabulary", r)
		}
	}
}

func TestSessionLive(t *testing.T) {
	root := testRoot(t)
	dir := filepath.Join(root, "contracts/auth/v1/examples/session")
	for file, wantLive := range map[string]bool{"valid-live.json": true, "valid-revoked.json": false} {
		raw, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatal(err)
		}
		s, err := ParseSession(root, raw)
		if err != nil {
			t.Fatal(err)
		}
		if got := s.Live(s.CreatedAt.Add(1)); got != wantLive {
			t.Errorf("%s: Live = %v, want %v", file, got, wantLive)
		}
		if s.Live(s.ExpiresAt.Time) {
			t.Errorf("%s: live at expiry", file)
		}
	}
}

func TestSecretStringRevealsOnlyThroughReveal(t *testing.T) {
	root := testRoot(t)
	raw, err := os.ReadFile(filepath.Join(root, "contracts/auth/v1/examples/refresh-request/valid-minimal.json"))
	if err != nil {
		t.Fatal(err)
	}
	req, err := ParseRefreshRequest(root, raw)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(req.RefreshCredential.Reveal(), "EXAMPLEREFRESH") {
		t.Fatalf("Reveal lost the wire value: %q", req.RefreshCredential.Reveal())
	}
}

package sso

import (
	"bytes"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// TestValidateAbsoluteURIScheme pins D2761 (team-lead's correction on
// D2759): sp_entity_id is exempt from validateHTTPSOrLoopback -- it is a
// bare identifier, never something a browser or IdP is sent a request to
// -- so any absolute URI (any scheme at all) is accepted; only an empty
// or relative/schemeless value is refused.
func TestValidateAbsoluteURIScheme(t *testing.T) {
	cases := []struct {
		name      string
		candidate string
		wantErr   bool
	}{
		{"a URN entityID is accepted", "urn:example:sp", false},
		{"an https URL is accepted", "https://host/metadata", false},
		{"an http URL is ALSO accepted here (unlike sp_acs_url)", "http://host/metadata", false},
		{"empty is refused", "", true},
		{"a relative, schemeless value is refused", "sp", true},
		{"a schemeless //host value is refused", "//host/path", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateAbsoluteURIScheme(c.candidate)
			if c.wantErr && err == nil {
				t.Fatalf("candidate=%q: want an error, got nil", c.candidate)
			}
			if !c.wantErr && err != nil {
				t.Fatalf("candidate=%q: want no error, got %v", c.candidate, err)
			}
		})
	}
}

const samlTestProviderID = "0b6f1c1e-5a1d-4a43-9d57-3f6c2b1a7e90"

func TestValidateSAMLACSOverridePath(t *testing.T) {
	cases := []struct {
		name     string
		override string
		wantErr  bool
	}{
		{"no override", "", false},
		{"exact route", "https://sp.test/saml/" + samlTestProviderID + "/acs", false},
		{"api route", "https://sp.test/api/v1/auth/saml/" + samlTestProviderID + "/acs", false},
		{"proxy prefix", "https://sp.test/proxy/saml/" + samlTestProviderID + "/acs", false},
		{"query after the path", "https://sp.test/saml/" + samlTestProviderID + "/acs?x=1", false},
		{"bare acs path", "https://sp.test/acs", true},
		{"another provider id", "https://sp.test/saml/6d0c7a3e-1111-4a43-9d57-3f6c2b1a7e90/acs", true},
		{"upper case id", "https://sp.test/saml/" + strings.ToUpper(samlTestProviderID) + "/acs", true},
		{"trailing slash", "https://sp.test/saml/" + samlTestProviderID + "/acs/", true},
		{"id in the query only", "https://sp.test/acs?p=/saml/" + samlTestProviderID + "/acs", true},
		{"metadata route", "https://sp.test/saml/" + samlTestProviderID + "/metadata", true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			err := validateSAMLACSOverridePath(samlConfigValues{SPACSURL: c.override}, samlTestProviderID)
			if c.wantErr != (err != nil) {
				t.Fatalf("override=%q: wantErr=%v got %v", c.override, c.wantErr, err)
			}
		})
	}
}

func TestDecodeSAMLConfigRefusesAnOverrideOfAnotherPath(t *testing.T) {
	raw := `{"sp_acs_url":"https://sp.test/acs"}`
	if _, err := decodeSAMLConfig(&raw, samlTestProviderID); err == nil {
		t.Fatal("want an error for an override that is not this provider's ACS path")
	}
	ok := `{"sp_acs_url":"https://sp.test/saml/` + samlTestProviderID + `/acs"}`
	if _, err := decodeSAMLConfig(&ok, samlTestProviderID); err != nil {
		t.Fatalf("exact route refused: %v", err)
	}
}

// The path refusal reaches the admin surface and the log exactly as the
// https refusal does (h.fail: bare 500), naming the provider id and the rule
// and never the configured URL.
func TestSAMLACSOverridePathRefusalParityAndLog(t *testing.T) {
	const secretHost = "override-host.invalid"
	pathRaw := `{"sp_acs_url":"https://` + secretHost + `/acs?token=abc"}`
	httpsRaw := `{"sp_acs_url":"http://` + secretHost + `/acs?token=abc"}`
	var results [2]struct {
		code int
		body string
		log  string
	}
	for i, raw := range []string{pathRaw, httpsRaw} {
		_, err := decodeSAMLConfig(&raw, samlTestProviderID)
		if err == nil {
			t.Fatalf("case %d: want an error", i)
		}
		var logs bytes.Buffer
		h := handlers{Deps{Logger: slog.New(slog.NewTextHandler(&logs, nil))}}
		rec := httptest.NewRecorder()
		h.fail(rec, httptest.NewRequest(http.MethodPost, "/api/v1/auth/saml/"+samlTestProviderID+"/initiate", nil), "decode saml config", err)
		results[i].code, results[i].body, results[i].log = rec.Code, rec.Body.String(), logs.String()
	}
	if results[0].code != results[1].code || results[0].body != results[1].body {
		t.Fatalf("path refusal %d %q differs from https refusal %d %q", results[0].code, results[0].body, results[1].code, results[1].body)
	}
	log := results[0].log
	if !strings.Contains(log, samlTestProviderID) || !strings.Contains(log, "sp_acs_url must end with /saml/") {
		t.Fatalf("log lacks the provider id or the rule: %s", log)
	}
	for _, leak := range []string{secretHost, "token=abc"} {
		if strings.Contains(log, leak) || strings.Contains(results[0].body, leak) {
			t.Fatalf("log or body carries the override value %q: %s", leak, log)
		}
	}
}

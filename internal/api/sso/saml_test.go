package sso

import "testing"

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

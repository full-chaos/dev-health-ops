package sessionissue

import "testing"

const stateDocument = `"principal_id":"11111111-1111-4111-8111-111111111111","organization_id":null,"revoked":false,"expired":false,"legacy_token_version":3`

// The state document has five required members and a closed list of optional
// names (CHAOS-12398): client_class, the class of a live OAuth grant on the
// session, comes only with such a grant, so an answer with no grant is the
// five members exactly as before. go-api reads it before auth-service emits it
// (D6123). An unknown name stays refused.
func TestStateReadsAnOptionalClientClassAndRefusesEveryOtherExtraMember(t *testing.T) {
	five, err := parseState([]byte(`{` + stateDocument + `}`))
	if err != nil {
		t.Fatalf("the five-member document: %v", err)
	}
	if five.ClientClass != "" {
		t.Fatalf("no grant reads as class %q", five.ClientClass)
	}
	for _, class := range []string{"browser", "mcp"} {
		state, err := parseState([]byte(`{` + stateDocument + `,"client_class":"` + class + `"}`))
		if err != nil {
			t.Fatalf("client_class %s: %v", class, err)
		}
		if state.ClientClass != class {
			t.Fatalf("client_class %s read as %q", class, state.ClientClass)
		}
	}
	for name, doc := range map[string]string{
		"an unknown extra member":       `,"extra":1`,
		"an unknown string member":      `,"class":"mcp"`,
		"a repeated client_class":       `,"client_class":"mcp","client_class":"browser"`,
		"an unknown class":              `,"client_class":"web"`,
		"an empty class":                `,"client_class":""`,
		"a null class":                  `,"client_class":null`,
		"a class of the wrong type":     `,"client_class":1`,
		"client_class plus unknown":     `,"client_class":"mcp","extra":1`,
		"a class with a trailing space": `,"client_class":"mcp "`,
		"a class in another case":       `,"client_class":"MCP"`,
	} {
		if _, err := parseState([]byte(`{` + stateDocument + doc + `}`)); err == nil {
			t.Errorf("%s was accepted", name)
		}
	}
	// A required member cannot be replaced by the optional one.
	if _, err := parseState([]byte(`{"principal_id":"11111111-1111-4111-8111-111111111111","organization_id":null,"revoked":false,"expired":false,"client_class":"mcp"}`)); err == nil {
		t.Error("client_class stood in for a required member")
	}
}

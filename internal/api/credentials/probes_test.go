package credentials

import "testing"

func TestBuildSafeURL(t *testing.T) {
	cases := map[[2]string]string{
		{"https://api.github.com", "user"}:                     "https://api.github.com/user",
		{"https://ghe.example.test/api/v3/", "/user"}:          "https://ghe.example.test/api/v3/user",
		{"https://host.example.test/x?y=1#z", "user"}:          "https://host.example.test/x/user",
		{"http://host.example.test:8080", "rest/api/3/myself"}: "http://host.example.test:8080/rest/api/3/myself",
		{"https://host.example.test//", "user"}:                "https://host.example.test/user",
	}
	for input, want := range cases {
		if got := buildSafeURL(input[0], input[1]); got != want {
			t.Errorf("buildSafeURL(%q, %q) = %q, want %q", input[0], input[1], got, want)
		}
	}
}

func TestJSONObjectErrorText(t *testing.T) {
	cases := map[string]string{
		"":                "Expecting value: line 1 column 1 (char 0)",
		"{":               "Expecting property name enclosed in double quotes: line 1 column 2 (char 1)",
		"  \n  oops":      "Expecting value: line 2 column 3 (char 5)",
		`{"a":1,}`:        "Illegal trailing comma before end of object: line 1 column 7 (char 6)",
		`{"a" 1}`:         "Expecting ':' delimiter: line 1 column 6 (char 5)",
		`{"a":1} x`:       "Extra data: line 1 column 9 (char 8)",
		`{"a":"b`:         "Unterminated string starting at: line 1 column 6 (char 5)",
		"{\"a\":\"\\q\"}": "Invalid \\escape: line 1 column 7 (char 6)",
	}
	for body, want := range cases {
		_, err := response{status: 200, body: []byte(body)}.jsonObject()
		if err == nil || err.Error() != want {
			t.Errorf("jsonObject(%q) error = %v, want %q", body, err, want)
		}
	}
}

func TestGitHubFromMapping(t *testing.T) {
	cases := []struct {
		name, creds string
		ok, app     bool
		token, base string
	}{
		{"token", `{"token":"t"}`, true, false, "t", ""},
		{"empty", `{}`, false, false, "", ""},
		{"nulls only", `{"token":null}`, false, false, "", ""},
		{"empty token", `{"token":""}`, false, false, "", ""},
		{"token and app fields", `{"token":"t","app_id":"1"}`, false, false, "", ""},
		{"incomplete app", `{"app_id":"1","private_key":"k"}`, false, false, "", ""},
		{"app camelCase", `{"appId":"1","privateKey":"k","installationId":"2"}`, true, true, "", ""},
		{"base url alias", `{"token":"t","baseUrl":"https://h"}`, true, false, "t", "https://h"},
		{"unknown keys ignored", `{"token":"t","other":1}`, true, false, "t", ""},
		{"numeric token", `{"token":12}`, true, false, "12", ""},
		{"missing key path", `{"app_id":"1","installation_id":"2","private_key_path":"/nonexistent/key.pem"}`, false, false, "", ""},
	}
	for _, c := range cases {
		got := githubFromMapping(decodeObject(t, c.creds))
		if (got != nil) != c.ok {
			t.Errorf("%s: ok=%v, want %v", c.name, got != nil, c.ok)
			continue
		}
		if got != nil && (got.app != c.app || got.token != c.token || got.baseURL != c.base) {
			t.Errorf("%s: %+v", c.name, got)
		}
	}
}

func TestOrChainUsesTruthiness(t *testing.T) {
	creds := decodeObject(t, `{"a":"","b":0,"c":"x","d":"y","e":false,"f":5}`)
	if got := orChain(creds, "a", "b", "c", "d"); got != "x" {
		t.Errorf("first truthy value wins, got %q", got)
	}
	if got := orChain(creds, "a", "b", "e"); got != "" {
		t.Errorf("no truthy value is empty, got %q", got)
	}
	if got := orChain(creds, "b", "f"); got != "5" {
		t.Errorf("a number is its str(), got %q", got)
	}
}

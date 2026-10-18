package teamsidentity

import "testing"

const linkNextURL = "https://api.github.com/orgs/acme/teams?page=2"

func TestGithubNextLinkPresentParamVariants(t *testing.T) {
	cases := []struct {
		name     string
		header   string
		wantURL  string
		wantNext bool
	}{
		{"quoted", `<` + linkNextURL + `>; rel="next"`, linkNextURL, true},
		{"unquoted", `<` + linkNextURL + `>; rel=next`, linkNextURL, true},
		{"upper-case key", `<` + linkNextURL + `>; REL="next"`, linkNextURL, true},
		{"upper-case value", `<` + linkNextURL + `>; rel="NEXT"`, linkNextURL, true},
		{"spaces around equals", `<` + linkNextURL + `>; rel = "next"`, linkNextURL, true},
		{"param before rel", `<` + linkNextURL + `>; title="x"; rel="next"`, linkNextURL, true},
		{"next after last", `<https://api.github.com/orgs/acme/teams?page=9>; rel="last", <` + linkNextURL + `>; REL = "next"`, linkNextURL, true},
		{"rel list is not next", `<` + linkNextURL + `>; rel="next last"`, "", false},
		{"single quotes are not next", `<` + linkNextURL + `>; rel='next'`, "", false},
		{"last only", `<` + linkNextURL + `>; rel="last"`, "", false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotURL, gotNext := githubNextLinkPresent(tc.header)
			if gotNext != tc.wantNext || gotURL != tc.wantURL {
				t.Fatalf("githubNextLinkPresent(%q) = (%q, %v), want (%q, %v)", tc.header, gotURL, gotNext, tc.wantURL, tc.wantNext)
			}
		})
	}
}

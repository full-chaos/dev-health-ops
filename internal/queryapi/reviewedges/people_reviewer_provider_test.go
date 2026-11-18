package reviewedges

import (
	"context"
	"errors"
	"testing"
)

const (
	rpUUIDA = "00000000-0000-0000-0000-00000000000a"
	rpUUIDB = "00000000-0000-0000-0000-00000000000b"
)

func reviewerOn(t *testing.T, repoProvider, stored string, identities ...[]any) (name string, rawKey bool, key string) {
	t.Helper()
	client := &fakeClient{
		response:      &fakeRowScanner{rows: [][]any{edgeRow(stored, "drive-by")}},
		identities:    identities,
		repoProviders: [][]any{{"repo-a", repoProvider}},
	}
	edge := resolvePeople(t, client, "org-1")[0]
	return nameOf(edge.ReviewerName), edge.ReviewerKey == personKey("org-1", stored, nil), edge.ReviewerKey
}

// CHAOS-14040: on a github or gitlab repository the stored reviewer is that provider's login, so a display
// name that equals it is not a match.
func TestReviewerProvider_ALoginIsNotMatchedToADisplayName(t *testing.T) {
	for _, tc := range []struct{ provider, login string }{{"github", "samuel-a"}, {"gitlab", "gl-sam"}} {
		t.Run(tc.provider, func(t *testing.T) {
			a := identityRow(rpUUIDA, "a", "", "Sam", `{"`+tc.provider+`": ["`+tc.login+`"]}`)
			name, raw, _ := reviewerOn(t, tc.provider, "sam", a)
			if name != "sam" || !raw {
				t.Errorf("reviewer sam on a %s repo: name %q rawKey %v, want the stored login with its raw key", tc.provider, name, raw)
			}
		})
	}
}

func TestReviewerProvider_ARealLoginOfThatProviderStillMatches(t *testing.T) {
	for _, tc := range []struct{ provider, login string }{{"github", "samuel-a"}, {"gitlab", "gl-sam"}} {
		t.Run(tc.provider, func(t *testing.T) {
			a := identityRow(rpUUIDA, "a", "", "Sam", `{"`+tc.provider+`": ["`+tc.login+`"]}`)
			name, raw, _ := reviewerOn(t, tc.provider, tc.login, a)
			if name != "Sam" || raw {
				t.Errorf("name %q rawKey %v, want Sam with the identity key", name, raw)
			}
		})
	}
}

func TestReviewerProvider_ALoginHeldUnderAnotherProviderDoesNotMatch(t *testing.T) {
	b := identityRow(rpUUIDB, "b", "", "Bea", `{"gitlab": ["sam"]}`)
	name, raw, _ := reviewerOn(t, "github", "sam", b)
	if name != "sam" || !raw {
		t.Errorf("a gitlab login on a github repo: name %q rawKey %v, want no match", name, raw)
	}
}

func TestReviewerProvider_ACanonicalIdOrEmailIsNotALogin(t *testing.T) {
	for _, tc := range []struct{ name, stored, want string }{{"canonical id", "sam", "sam"}, {"e-mail", "sam@corp.example", "<nil>"}} {
		t.Run(tc.name, func(t *testing.T) {
			b := identityRow(rpUUIDB, "sam", "sam@corp.example", "Bea", `{}`)
			name, raw, _ := reviewerOn(t, "github", tc.stored, b)
			if name != tc.want || !raw {
				t.Errorf("name %q rawKey %v, want no match (name %s)", name, raw, tc.want)
			}
		})
	}
}

func TestReviewerProvider_AProviderKeyIsComparedCaseInsensitively(t *testing.T) {
	a := identityRow(rpUUIDA, "a", "", "Sam", `{"GitHub": ["Samuel-A"]}`)
	name, raw, _ := reviewerOn(t, "github", "samuel-a", a)
	if name != "Sam" || raw {
		t.Errorf("key GitHub on a github repo: name %q rawKey %v, want Sam", name, raw)
	}
}

func TestReviewerProvider_AKeyOutsideTheKnownProvidersIsUnscoped(t *testing.T) {
	a := identityRow(rpUUIDA, "a", "", "Sam", `{"bitbucket": ["samuel-a"]}`)
	for _, provider := range []string{"github", "gitlab"} {
		t.Run(provider, func(t *testing.T) {
			name, raw, _ := reviewerOn(t, provider, "samuel-a", a)
			if name != "Sam" || raw {
				t.Errorf("unscoped key on a %s repo: name %q rawKey %v, want Sam", provider, name, raw)
			}
		})
	}
}

func TestReviewerProvider_ALoginTwoIdentitiesHoldFailsClosed(t *testing.T) {
	a := identityRow(rpUUIDA, "a", "", "Ann", `{"github": ["dup"]}`)
	b := identityRow(rpUUIDB, "b", "", "Bea", `{"github": ["dup"]}`)
	name, raw, _ := reviewerOn(t, "github", "dup", a, b)
	if name != "dup" || !raw {
		t.Errorf("shared login: name %q rawKey %v, want no match", name, raw)
	}
	// Scoped by one identity and unscoped by another is a shared value too.
	c := identityRow(rpUUIDB, "b", "", "Bea", `{"bitbucket": ["dup"]}`)
	name, raw, _ = reviewerOn(t, "github", "dup", a, c)
	if name != "dup" || !raw {
		t.Errorf("scoped + unscoped holders: name %q rawKey %v, want no match", name, raw)
	}
}

// A repository with another provider, or with no repos row, keeps the CHAOS-14032 order, display name last.
func TestReviewerProvider_ANonLoginRepoKeepsTheDisplayNameFallback(t *testing.T) {
	a := identityRow(rpUUIDA, "a", "", "Sam", `{"github": ["samuel-a"]}`)
	for _, provider := range []string{"unknown", "external", ""} {
		t.Run("provider="+provider, func(t *testing.T) {
			name, raw, _ := reviewerOn(t, provider, "sam", a)
			if name != "Sam" || raw {
				t.Errorf("name %q rawKey %v, want the display-name match Sam", name, raw)
			}
		})
	}
	t.Run("no repos row", func(t *testing.T) {
		client := &fakeClient{
			response:   &fakeRowScanner{rows: [][]any{edgeRow("sam", "drive-by")}},
			identities: [][]any{a},
		}
		if got := nameOf(resolvePeople(t, client, "org-1")[0].ReviewerName); got != "Sam" {
			t.Errorf("name %q, want Sam", got)
		}
	})
}

// The author side does not change: an author stored by name still matches a display name on a github repo.
func TestReviewerProvider_TheAuthorSideIsUnchanged(t *testing.T) {
	a := identityRow(rpUUIDA, "a", "", "Sam", `{"github": ["samuel-a"]}`)
	client := &fakeClient{
		response:      &fakeRowScanner{rows: [][]any{edgeRow("drive-by", "sam")}},
		identities:    [][]any{a},
		repoProviders: [][]any{{"repo-a", "github"}},
	}
	edge := resolvePeople(t, client, "org-1")[0]
	if got := nameOf(edge.AuthorName); got != "Sam" {
		t.Errorf("author name %q, want Sam", got)
	}
}

func TestReviewerProvider_AFailedRepoProviderReadFailsTheRequest(t *testing.T) {
	client := &fakeClient{
		response:         &fakeRowScanner{rows: [][]any{edgeRow("sam", "drive-by")}},
		repoProvidersErr: errors.New("boom"),
	}
	if _, err := ResolveScoped(context.Background(), client, "org-1", mustDate(t, "2026-08-01"), mustDate(t, "2026-08-31"), Scope{}, 100); err == nil {
		t.Fatal("a failed repo provider read must fail the request")
	}
}

// A provider key is lower-cased, never trimmed: a padded "GitLab " is outside the known providers, so it is
// unscoped, and it counts as a holder beside a github holder of the same login.
func TestReviewerProvider_APaddedProviderKeyIsUnscopedNotTrimmed(t *testing.T) {
	a := identityRow(rpUUIDA, "a", "", "Alice", `{" GitLab": ["sam"]}`)
	b := identityRow(rpUUIDB, "b", "", "Bea", `{"github": ["sam"]}`)
	name, raw, _ := reviewerOn(t, "github", "sam", a, b)
	if name != "sam" || !raw {
		t.Errorf("padded key beside a github holder: name %q rawKey %v, want fail closed (two holders)", name, raw)
	}
	name, raw, _ = reviewerOn(t, "github", "sam", a)
	if name != "Alice" || raw {
		t.Errorf("padded key as sole holder: name %q rawKey %v, want Alice (unscoped)", name, raw)
	}
}

// A padded repository provider label is not github or gitlab: the CHAOS-14032 order applies.
func TestReviewerProvider_APaddedRepoProviderKeepsTheLegacyOrder(t *testing.T) {
	a := identityRow(rpUUIDA, "a", "", "Sam", `{"github": ["samuel-a"]}`)
	name, raw, _ := reviewerOn(t, " GitHub", "sam", a)
	if name != "Sam" || raw {
		t.Errorf("padded repo provider: name %q rawKey %v, want the legacy display-name match Sam", name, raw)
	}
}

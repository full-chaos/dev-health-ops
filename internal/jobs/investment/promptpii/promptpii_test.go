package promptpii

import (
	"strings"
	"testing"
)

func TestTextReplacesEachClassAndKeepsTheRest(t *testing.T) {
	cases := []struct {
		name    string
		in      string
		gone    string
		want    string
		counted func(Counts) int
	}{
		{"email", "ask jane.doe+ops@corp.example about the outage", "jane.doe", "ask [email-1] about the outage", func(c Counts) int { return c.Emails }},
		{"github mention", "cc @jane-doe for review", "jane-doe", "cc [user-1] for review", func(c Counts) int { return c.Mentions }},
		{"team mention", "cc @acme/platform please", "acme/platform", "cc [user-1] please", func(c Counts) int { return c.Mentions }},
		{"jira mention", "assigned to [~jdoe] today", "jdoe", "assigned to [user-1] today", func(c Counts) int { return c.Mentions }},
		{"jira account mention", "ping [~accountid:5b10ac8d82e05b22cc7d4ef5] now", "5b10ac8d", "ping [user-1] now", func(c Counts) int { return c.Mentions }},
		{"national phone", "call (415) 555-0132 tonight", "555-0132", "call [phone-1] tonight", func(c Counts) int { return c.Phones }},
		{"dotted phone", "call 415.555.0132 tonight", "555.0132", "call [phone-1] tonight", func(c Counts) int { return c.Phones }},
		{"international phone", "call +44 20 7946 0958 tonight", "7946", "call [phone-1] tonight", func(c Counts) int { return c.Phones }},
		{"url userinfo", "see https://deploy:hunter2pass@git.corp.example/repo for logs", "hunter2pass", "see [REDACTED]@git.corp.example/repo for logs", func(c Counts) int { return c.Credentials }},
		{"provider token", "the token is ghp_abcdefghijklmnopqrstuvwxyz0123456789 ok", "ghp_abcdefghijklmnopqrstuvwxyz0123456789", "", func(c Counts) int { return c.Credentials }},
		{"dotless host email", "mail alice@localhost about it", "alice@localhost", "mail [email-1] about it", func(c Counts) int { return c.Emails }},
		{"non-ascii email", "contact δοκιμή@example.gr today", "δοκιμή@", "contact [email-1] today", func(c Counts) int { return c.Emails }},
		{"parenthesised area code phone", "call (02) 9374 4000 today", "9374", "call [phone-1] today", func(c Counts) int { return c.Phones }},
		{"plain ten digit phone", "call 4155550132 tonight", "4155550132", "call [phone-1] tonight", func(c Counts) int { return c.Phones }},
		{"plain ten digit phone in punctuation", "phone:4155550132, thanks", "4155550132", "phone:[phone-1], thanks", func(c Counts) int { return c.Phones }},
		{"french phone", "appelez 01 23 45 67 89 demain", "45 67", "appelez [phone-1] demain", func(c Counts) int { return c.Phones }},
		{"labeled secret equals", "set token=abcdefgh and retry", "abcdefgh", "set token=[REDACTED] and retry", func(c Counts) int { return c.Credentials }},
		{"labeled password quoted", `config password: "correct horse" ok`, "correct horse", `config password: [REDACTED] ok`, func(c Counts) int { return c.Credentials }},
		{"authorization header", "sent header Authorization: Bearer abc123xyz to the gateway", "abc123xyz", "Authorization: Bearer [REDACTED]", func(c Counts) int { return c.Credentials }},
		{"bearer in prose", "send bearer abc123xyz to the gateway", "abc123xyz", "send bearer [REDACTED] to the gateway", func(c Counts) int { return c.Credentials }},
		{"password in prose", "password: Tr0ub4dor3xx was pasted here", "Tr0ub4dor", "", func(c Counts) int { return c.Credentials }},
		{"plus-address before a tag-like host", "mail jane.doe+ops@latest today", "jane.doe", "mail [email-1] today", func(c Counts) int { return c.Emails }},
		{"scoped pin next to a real mention", "upgrade @scope/pkg@latest and ask @bob", "bob", "upgrade @scope/pkg@latest and ask [user-1]", func(c Counts) int { return c.Mentions }},
		{"real email next to a package pin", "mail bob@corp.example about lodash@latest", "bob@", "mail [email-1] about lodash@latest", func(c Counts) int { return c.Emails }},
		{"dist-tag name inside a real address", "mail ann@beta.example.com today", "ann@", "mail [email-1] today", func(c Counts) int { return c.Emails }},
		{"v-version host inside a real address", "mail ann@v2.example.com today", "ann@", "mail [email-1] today", func(c Counts) int { return c.Emails }},
		{"dotless address that is not a tag", "mail alice@internal today", "alice@", "mail [email-1] today", func(c Counts) int { return c.Emails }},
		{"dotless address named like a digest", "mail alice@sha256 today", "alice@", "mail [email-1] today", func(c Counts) int { return c.Emails }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := NewFilter()
			got := f.Text(tc.in)
			if strings.Contains(got, tc.gone) {
				t.Fatalf("value survived the filter: %q", got)
			}
			if tc.want != "" && !strings.Contains(got, tc.want) {
				t.Fatalf("got %q, want it to contain %q", got, tc.want)
			}
			if n := tc.counted(f.Counts()); n != 1 {
				t.Fatalf("class count = %d, want 1 (counts %+v)", n, f.Counts())
			}
		})
	}
}

func TestTextKeepsPlainEngineeringProse(t *testing.T) {
	for _, in := range []string{
		"Rotate the token expiry handling in the OAuth refresh path",
		"Fix PROJ-12345: timeout on 2026-10-13 12:30:45 UTC in v1.2.3",
		"Upgrade 192.168.100.200 gateway; build 20261013123045",
		"Rows 123456789 and 12345678901 differ in length from a phone number",
		"Secret rotation for the signing key is overdue",
		"See https://example.com/docs/a%20b and 100%25 coverage; the /usr/token/refresh path is slow",
		"Pin react@18.3.1 and lodash@4.17.21 before the 2026-10-13 release; call 555-0132 maybe",
		"The token is invalid and the password expired",
		"The bearer of bad news and an authorization flow review",
		"Upgrade lodash@latest, react@next, vite@beta.3, esbuild@canary-1042 and typescript@rc",
		"Pin pkg@v18.3.1 and tool@v2 and @scope/pkg@v18.3.1+build9 before release",
		"Upgrade @scope/pkg@latest, @types/node@18.3.1 and @acme/ui@next-1042 today",
		"",
	} {
		f := NewFilter()
		if got := f.Text(in); got != in {
			t.Errorf("changed plain text:\n in: %q\nout: %q", in, got)
		}
		if f.Counts().Total() != 0 {
			t.Errorf("counted %+v on plain text %q", f.Counts(), in)
		}
	}
}

func TestPlaceholdersAreStablePerValueAndDeterministic(t *testing.T) {
	in := "From a@x.example to b@x.example, again A@X.example; @bob then @alice then @bob"
	first := NewFilter().Text(in)
	second := NewFilter().Text(in)
	if first != second {
		t.Fatalf("not deterministic:\n%q\n%q", first, second)
	}
	want := "From [email-1] to [email-2], again [email-1]; [user-1] then [user-2] then [user-1]"
	if first != want {
		t.Fatalf("got %q\nwant %q", first, want)
	}
}

func TestTextIsIdempotent(t *testing.T) {
	inputs := []string{
		"mail a.b@corp.example, cc @jane-doe and [~jdoe]; call (415) 555-0132 or +44 20 7946 0958",
		"see https://deploy:hunter2pass@git.corp.example/x password: Tr0ub4dor3xx ghp_abcdefghijklmnopqrstuvwxyz0123456789",
		"mail alice@localhost, δοκιμή@example.gr; call (02) 9374 4000 or 01 23 45 67 89; set token=abcdefgh, password: \"correct horse\"",
		"plain text with nothing to hide",
		"lodash@latest and @scope/pkg@beta.3; mail bob@corp.example",
	}
	for _, in := range inputs {
		once := NewFilter().Text(in)
		f := NewFilter()
		twice := f.Text(once)
		if once != twice {
			t.Errorf("not idempotent:\nonce:  %q\ntwice: %q", once, twice)
		}
		if f.Counts().Total() != 0 {
			t.Errorf("second pass counted %+v over already filtered text", f.Counts())
		}
	}
}

func TestCountsNeverHoldValues(t *testing.T) {
	f := NewFilter()
	f.Text("a@b.example @c")
	c := f.Counts()
	if c != (Counts{Emails: 1, Mentions: 1}) {
		t.Fatalf("counts = %+v", c)
	}
	if got := c.Add(c).Total(); got != 4 {
		t.Fatalf("Add/Total = %d, want 4", got)
	}
}

func TestPackagePinsAreKeptAndCountNoEmail(t *testing.T) {
	for _, in := range []string{
		"bump lodash@latest",
		"try react@next and vite@beta.3",
		"esbuild@canary-1042 broke the build",
		"pin pkg@v18.3.1-rc.2 for now",
		"pin @scope/pkg@latest for now",
	} {
		f := NewFilter()
		if got := f.Text(in); got != in {
			t.Errorf("package pin changed:\n in: %q\nout: %q", in, got)
		}
		if f.Counts().Total() != 0 {
			t.Errorf("counted %+v in %q", f.Counts(), in)
		}
	}
}

func TestScopedPinKeepsTheNextMentionNumbering(t *testing.T) {
	f := NewFilter()
	got := f.Text("@amy then @scope/pkg@beta.3 then @bob then @amy")
	want := "[user-1] then @scope/pkg@beta.3 then [user-2] then [user-1]"
	if got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if f.Counts().Mentions != 3 {
		t.Fatalf("counts %+v", f.Counts())
	}
}

func TestNamedLimitsOfThePinRule(t *testing.T) {
	if in := "mail alice@latest"; NewFilter().Text(in) != in {
		t.Fatal("alice@latest cannot be told from lodash@latest")
	}
	if got := NewFilter().Text("deploy name@release-branch"); got != "deploy [email-1]" {
		t.Fatalf("a tag outside the channel list is read as an address; got %q", got)
	}
}

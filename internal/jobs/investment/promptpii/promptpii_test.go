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
		{"url userinfo", "see https://deploy:hunter2pass@git.corp.example/repo for logs", "hunter2pass", "see [REDACTED] for logs", func(c Counts) int { return c.Credentials }},
		{"provider token", "the token is ghp_abcdefghijklmnopqrstuvwxyz0123456789 ok", "ghp_abcdefghijklmnopqrstuvwxyz0123456789", "", func(c Counts) int { return c.Credentials }},
		{"password in prose", "password: Tr0ub4dor3xx was pasted here", "Tr0ub4dor", "", func(c Counts) int { return c.Credentials }},
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
		"Secret rotation for the signing key is overdue",
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
		"plain text with nothing to hide",
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

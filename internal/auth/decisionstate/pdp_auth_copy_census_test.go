package decisionstate

import (
	"os"
	"regexp"
	"strings"
	"testing"
)

// D6651: public.* is the authority for users, organizations and memberships; the auth.principals, auth.organizations
// and auth.memberships rows are copies. No PDP decision input may read a value from a copy.
//
// scope is a file and, when funcs is set, only the bodies of those functions. allow holds the lines that name a copy
// table and are not a value read of a decision input, each with the reason and the lock-order step.
type censusScope struct {
	file  string
	funcs []string
	allow map[string]string
}

// authRule is the auth.principals form of the platform role rule; a decision input uses the Public form.
var authRule = regexp.MustCompile(`\b(rule|platformrole)\.(HoldsSQL|ActiveRule)\(`)

// copyTable matches a schema-qualified table name (".principals", "`+s.q+`.organizations") and t.table("memberships").
var copyTable = regexp.MustCompile(`\.(principals|organizations|memberships)\b|table\("(principals|organizations|memberships)"\)`)

func TestNoPDPDecisionInputReadsAnAuthCopy(t *testing.T) {
	scopes := []censusScope{
		{file: "decisionstate.go", allow: map[string]string{
			"`SELECT kind, disabled_at IS NULL FROM `+t.table(\"principals\")+` WHERE id = $1`": "the principal kind (a non-user kind decides before a users row) and a service account's disabled mark: auth-native, no public twin",
		}},
		{file: "../membership/effective_on.go"},
		{file: "../platformrole/rule/rule.go", funcs: []string{"PublicActiveRule", "PublicHoldsSQL"}},
		{file: "../platformrole/store.go", funcs: []string{"ResolveOn", "resolvePublicOn"}},
		{file: "../authapi/users.go"},
		{file: "../session/delegation.go", allow: map[string]string{
			"`SELECT disabled_at IS NULL FROM `+s.q+`.organizations WHERE id = $1 FOR SHARE`":                            "pending PR-2 (step 3)",
			"`SELECT 1 FROM `+s.q+`.memberships WHERE organization_id = $1 AND principal_id = $2 ORDER BY id FOR SHARE`": "pending PR-2 (lock only, step 6)",
			"`SELECT kind = 'user' AND disabled_at IS NULL FROM `+s.q+`.principals WHERE id = $1 FOR SHARE`":             "pending PR-2 (step 1)",
			"JOIN `+s.q+`.principals p ON p.id = r.principal_id":                                                         "pending PR-2 (checkAuthority)",
		}},
	}
	for _, sc := range scopes {
		raw, err := os.ReadFile(sc.file)
		if err != nil {
			t.Fatal(err)
		}
		text := string(raw)
		if len(sc.funcs) > 0 {
			var b strings.Builder
			for _, fn := range sc.funcs {
				body, ok := funcBody(text, fn)
				if !ok {
					t.Errorf("%s: no func %s", sc.file, fn)
				}
				b.WriteString(body)
			}
			text = b.String()
		}
		if sc.file != "../session/delegation.go" && authRule.MatchString(text) {
			t.Errorf("%s calls the auth.principals platform role rule: %s", sc.file, authRule.FindString(text))
		}
		used := map[string]bool{}
		for _, line := range strings.Split(text, "\n") {
			code := line
			if i := strings.Index(code, "//"); i >= 0 {
				code = code[:i]
			}
			for _, m := range copyTable.FindAllStringIndex(code, -1) {
				if strings.HasSuffix(code[:m[0]], "public") {
					continue
				}
				reason := ""
				for frag, why := range sc.allow {
					if strings.Contains(code, frag) {
						reason, used[frag] = why, true
					}
				}
				if reason == "" {
					t.Errorf("%s reads an auth copy table: %s", sc.file, strings.TrimSpace(line))
				}
			}
		}
		for frag := range sc.allow {
			if !used[frag] {
				t.Errorf("%s: allowlist entry matches nothing: %s", sc.file, frag)
			}
		}
	}
}

func funcBody(text, name string) (string, bool) {
	re := regexp.MustCompile(`(?m)^func (\([^)]*\) )?` + regexp.QuoteMeta(name) + `\(`)
	loc := re.FindStringIndex(text)
	if loc == nil {
		return "", false
	}
	body := text[loc[0]:]
	if end := strings.Index(body[1:], "\nfunc "); end > 0 {
		body = body[:end+1]
	}
	return body, true
}

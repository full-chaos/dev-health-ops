package authrows

import "regexp"

var (
	// A failed sign-in's golden row carries the attempted address as {"email": "..."}; the target's holds {}.
	attemptedEmail = regexp.MustCompile(`^(.*login_failedsession.*?)\{"email": "[^"]*"\}(\{.*)$`)
	// A users row's last-login flag is the second to last bool: "... <token_version> <last login> <updated>".
	lastLoginSet = regexp.MustCompile(`^(.* [0-9]+ )true( (?:true|false)\$.*)$`)
	// A sign-in audit row with no organization: the organization column is NULL.
	loginNoOrg = regexp.MustCompile(`^<nil>.*loginsessionuser:.*User logged in\{\}.*success<null>$`)
	// Alice's two successful sign-in audit rows: go-api signed her in to organization B, auth-service to A.
	aliceLoginOrgB = regexp.MustCompile(`^Org B(alice@example\.comloginsessionuser:alice@example\.comUser logged in\{\}.*success<null>)$`)
)

// aliceRefreshRowsInOrgA maps each of alice's refresh_tokens rows in the golden (grouped by organization and
// revocation) to the row auth-service holds: organization A, and one more revoked credential.
var aliceRefreshRowsInOrgA = map[string]string{
	"alice@example.comorg-bfalse 1": "alice@example.comorg-afalse 1",
	"alice@example.comorg-btrue 1":  "alice@example.comorg-atrue 2",
}

// Quirks is the table every ACP-target row difference is classified in. Each entry covers exact lines of the
// golden's rows; the golden is frozen, so an entry cannot drift without the golden's text changing.
var Quirks = []Quirk{
	{
		ID: "audit-login-failed-attempted-email-not-recorded", Kind: Deliberate, Table: TableAuditLogs, Needs: []Aggregate{LocalLogin},
		Reason: "auth-service records a refused sign-in as a reason code and the principal, never the address that was typed; the golden's row also holds that address in its changes",
		Rewrite: func(line string) (string, bool) {
			return replaceOnce(attemptedEmail, line, "${1}{}${2}")
		},
	},
	{
		ID: "audit-login-without-organization-recorded", Kind: Deliberate, Table: TableAuditLogs, Needs: []Aggregate{LocalLogin},
		Reason: "auth-service audits every sign-in, with a NULL organization when the user has none; go-api writes an audit row only for a sign-in that resolves an organization",
		Max:    1,
		Extra: func(line string) bool {
			return loginNoOrg.MatchString(line)
		},
	},
	{
		ID: "login-organization-chosen-without-activity", Kind: Deliberate, Table: TableRefreshTokens, Needs: []Aggregate{LocalLogin},
		Reason: "alice belongs to organizations A and B; go-api prefers the one with ClickHouse activity (B), auth-service reads no ClickHouse and takes her earliest membership (A) (CHAOS-10228, authapi chooseMembership)",
		Rewrite: func(line string) (string, bool) {
			rewritten, ok := aliceRefreshRowsInOrgA[line]
			return rewritten, ok
		},
	},
	{
		ID: "audit-login-organization-chosen-without-activity", Kind: Deliberate, Table: TableAuditLogs, Needs: []Aggregate{LocalLogin},
		Reason: "the same organization choice as login-organization-chosen-without-activity: alice's two sign-in audit rows name organization A where the golden's name B (CHAOS-10228, authapi chooseMembership)",
		Rewrite: func(line string) (string, bool) {
			return replaceOnce(aliceLoginOrgB, line, "Org A${1}")
		},
	},
	{
		ID: "users-last-login-not-stamped", Kind: Gap, Table: TableUsers, Needs: []Aggregate{LocalLogin},
		Tracked: "CHAOS-10630",
		Reason:  "a sign-in through auth-service stamps no last login on public.users or auth.users until the users-owned switch lands, so the row reads as never signed in",
		Rewrite: func(line string) (string, bool) {
			return replaceOnce(lastLoginSet, line, "${1}false${2}")
		},
	},
}

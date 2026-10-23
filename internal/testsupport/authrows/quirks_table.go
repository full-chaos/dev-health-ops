package authrows

import "regexp"

var (
	// A failed sign-in's golden row carries the attempted address as {"email": "..."}; the target's holds {}.
	attemptedEmail = regexp.MustCompile(`^(.*login_failedsession.*?)\{"email": "[^"]*"\}(\{.*)$`)
	// A users row's last-login flag is the second to last bool: "... <token_version> <last login> <updated>".
	lastLoginSet = regexp.MustCompile(`^(.* [0-9]+ )true( (?:true|false)\$.*)$`)
	// A sign-in audit row with no organization: the organization column is NULL.
	loginNoOrg = regexp.MustCompile(`^<nil>.*loginsessionuser:.*User logged in\{\}.*success<null>$`)
)

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
		ID: "users-last-login-not-stamped", Kind: Gap, Table: TableUsers, Needs: []Aggregate{LocalLogin},
		Tracked: "CHAOS-10630",
		Reason:  "a sign-in through auth-service stamps no last login on public.users or auth.users until the users-owned switch lands, so the row reads as never signed in",
		Rewrite: func(line string) (string, bool) {
			return replaceOnce(lastLoginSet, line, "${1}false${2}")
		},
	},
}

package authrows

import (
	"regexp"
	"strings"
)

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

var (
	// A registration's golden audit row: the new organization's name first, the registered address in the resource
	// and again in the changes next to the organization id.
	registeredWithChanges = regexp.MustCompile(`^.*?createuseruser:([^{]*?)User registered\{"email": "[^"]*", "organization_id": "<uuid>"\}(\{.*)$`)
	// A registration or password reset audit row with no organization: the organization column is NULL.
	registeredNoOrg = regexp.MustCompile(`^<nil>.*createuseruser:.*User registered\{\}.*success<null>$`)
	verifiedNoOrg   = regexp.MustCompile(`^<nil>.*email_verifieduseruser:.*Email verified\{\}.*success<null>$`)
	resetNoOrg      = regexp.MustCompile(`^<nil>.*password_resetuseruser:.*User reset password\{\}.*success<null>$`)
	// Alice's password reset: go-api audits it in her first membership's organization, auth-service in none.
	aliceResetOrgA = regexp.MustCompile(`^Org A(alice@example\.compassword_resetuseruser:alice@example\.comUser reset password\{\}.*success<null>)$`)
	resetAsked     = regexp.MustCompile(`password_reset_requesteduser`)
)

// registeredWithoutOrgOrChanges is a registration's golden audit row as auth-service records it: no organization
// and an empty changes object.
func registeredWithoutOrgOrChanges(line string) (string, bool) {
	m := registeredWithChanges.FindStringSubmatch(line)
	if m == nil {
		return "", false
	}
	email := strings.TrimSpace(m[1])
	return "<nil>" + email + "createuseruser:" + m[1] + "User registered{}" + m[2], true
}

// aliceRefreshRowsInOrgA maps each of alice's refresh_tokens rows in the golden (grouped by organization and
// revocation) to the same row in organization A: the organization changes, the count never does.
var aliceRefreshRowsInOrgA = map[string]string{
	"alice@example.comorg-bfalse 1": "alice@example.comorg-afalse 1",
	"alice@example.comorg-btrue 1":  "alice@example.comorg-atrue 1",
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
	{
		ID: "audit-register-organization-and-changes-not-recorded", Kind: Deliberate, Table: TableAuditLogs, Needs: []Aggregate{Credentials},
		Reason:  "auth-service audits a registration as a principal event with no organization, no changes object and no description column; go-api's row names the new organization and holds the address and organization id in its changes",
		Tracked: "",
		Rewrite: registeredWithoutOrgOrChanges,
	},
	{
		ID: "audit-register-without-organization-recorded", Kind: Deliberate, Table: TableAuditLogs, Needs: []Aggregate{Credentials},
		Reason: "auth-service audits every registration; go-api writes an audit row only for a registration that created an organization (the scenario's solo registration, auto-org off)",
		Max:    1, Exact: true,
		Extra: func(line string) bool { return registeredNoOrg.MatchString(line) },
	},
	{
		ID: "audit-email-verified-recorded", Kind: Deliberate, Table: TableAuditLogs, Needs: []Aggregate{Credentials},
		Reason: "auth-service audits a verification twice (the link redeemed, the address marked verified); go-api writes no audit row for it. The scenario verifies two links (nina, uma)",
		Max:    verifiedAuditRows, Exact: true,
		Extra: func(line string) bool { return verifiedNoOrg.MatchString(line) },
	},
	{
		ID: "audit-password-reset-organization-not-recorded", Kind: Deliberate, Table: TableAuditLogs, Needs: []Aggregate{Credentials},
		Reason: "alice's password reset: go-api audits it in her first membership's organization, auth-service's event holds none",
		Rewrite: func(line string) (string, bool) {
			return replaceOnce(aliceResetOrgA, line, "<nil>${1}")
		},
	},
	{
		ID: "audit-password-reset-recorded-without-membership", Kind: Deliberate, Table: TableAuditLogs, Needs: []Aggregate{Credentials},
		Reason: "auth-service audits every password reset; go-api audits one only for a user with a membership (pia has none)",
		Max:    1, Exact: true,
		Extra: func(line string) bool { return resetNoOrg.MatchString(line) },
	},
	{
		ID: "audit-password-reset-requested-not-recorded", Kind: Deliberate, Table: TableAuditLogs, Needs: []Aggregate{Credentials},
		Reason: "go-api audits a password reset request for a user with a membership (alice); auth-service's issue of a link credential writes no audit event",
		Max:    1, Exact: true,
		Missing: func(line string) bool { return resetAsked.MatchString(line) },
	},
}

// verifiedAuditRows is the email-verified audit rows of the scenario: two verification links followed with a live
// token (nina's, uma's; nina's second visit is refused), each audited by two events.
const verifiedAuditRows = 2 * 2

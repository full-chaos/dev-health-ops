package authrows

import "regexp"

var (
	// A users row's last-login flag is the second to last bool: "... <token_version> <last login> <updated>".
	lastLoginSet = regexp.MustCompile(`^(.* [0-9]+ )true( (?:true|false)\$.*)$`)
	// Alice's two successful sign-in audit rows: go-api signed her in to organization B, auth-service to A.
	aliceLoginOrgB = regexp.MustCompile(`^Org B(alice@example\.comloginsessionuser:alice@example\.comUser logged in\{\}.*success<null>)$`)
	// Uma's seeded expired verification token as the public table holds it: expired, a 64-digit hash, a 24-hour lifetime.
	legacyUmaVerificationRow = regexp.MustCompile(`^uma@example\.comtrue false 64 1440$`)
	// A link mail whose token is go-api's format.
	linkTokenGoAPIRow = regexp.MustCompile(`^(.* link token )` + LinkTokenGoAPI + `$`)
)

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
		ID: "users-last-login-not-stamped", Kind: Gap, Table: TableUsers, Needs: []Aggregate{LocalLogin}, Owned: NotOwned,
		Tracked: "CHAOS-10630",
		Reason:  "a sign-in through auth-service stamps no last login on public.users or auth.users until the users-owned switch lands, so the row reads as never signed in; on a leg that owns users (AUTH_USERS_OWNED=true) the sign-in stamps public.users.last_login_at (CHAOS-11502) and this difference is not expected",
		Rewrite: func(line string) (string, bool) {
			return replaceOnce(lastLoginSet, line, "${1}false${2}")
		},
	},
	{
		ID: "email-verification-legacy-row-not-purged-owned", Kind: Gap, Table: TableEmailVerificationTokens, Needs: []Aggregate{Credentials}, Owned: OwnedOnly,
		Tracked: "CHAOS-11506", Max: 1, Exact: true,
		Reason: "go-api replaces a user's verification token on issue; auth-service issues into auth.link_credentials and never touches public.email_verification_tokens, so uma's seeded expired legacy row stays. No reader of it is reachable while users are owned (go-api's verify and reset refuse and are proxied: session/verify.go:86,:237, authproxy.go:155-158; startup guard usersowned.go:15-25; auth-service reads auth.link_credentials only); its purge is CHAOS-11506, the resend and forgot guard gap CHAOS-11507. Max is that one row",
		Extra:  func(line string) bool { return legacyUmaVerificationRow.MatchString(line) },
	},
	{
		ID: "link-token-format-auth-service", Kind: Deliberate, Table: TableLinkMails, Needs: []Aggregate{Credentials}, Owned: OwnedOnly,
		Max: 8, Exact: true,
		Reason: "with users owned by auth-service its credentials flows mail an opaque 43-character link token where go-api and Python mail a signed one, and verify and reset accept only the token of their own plane: a link mailed before the switch lapses after it, and the reverse after a rollback (CHAOS-10688; runbook lines on CHAOS-10625). Max is the golden's eight link mails",
		Rewrite: func(line string) (string, bool) {
			return replaceOnce(linkTokenGoAPIRow, line, "${1}"+LinkTokenOpaque)
		},
	},
}

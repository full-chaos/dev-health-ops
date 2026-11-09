package authrows

import "regexp"

var (
	// Alice's two successful sign-in audit rows: go-api signed her in to organization B, auth-service to A.
	aliceLoginOrgB = regexp.MustCompile(`^Org B(alice@example\.comloginsessionuser:alice@example\.comUser logged in\{\}.*success<null>)$`)
	// Judy's seeded expired reset token as the public table holds it: expired, a 64-digit hash, a 60-minute lifetime.
	legacyJudyResetRow = regexp.MustCompile(`^judy@example\.comtrue false 64 60$`)
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
		ID: "legacy-reset-row-purged-at-switch", Kind: Deliberate, Table: TablePasswordResetTokens, Needs: []Aggregate{Credentials}, Owned: OwnedOnly,
		Max: 1, Exact: true,
		Reason:  "the users-owned switch purges go-api's legacy link-token rows (dho auth legacy-link-purge, run by the corpus's owned hand-off as by a deployment's flip): judy's seeded expired reset row, which the golden keeps because nothing touched it, is gone. It could not be redeemed (expired, and go-api's reset refuses while users are owned). Max is that one row",
		Missing: func(line string) bool { return legacyJudyResetRow.MatchString(line) },
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

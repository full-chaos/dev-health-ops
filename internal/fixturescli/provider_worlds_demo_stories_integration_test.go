//go:build integration

package fixturescli

import "strings"

// The demo world's work: each team's stories (a tracker item and the pull or merge requests that carry
// it), in days before the world's instant.

// demoChange is one pull or merge request of a story.
type demoChange struct {
	Repo string // GitHub repository name or GitLab project path
	// CreatedAgo/MergedAgo are days before the instant; MergedAgo < 0 keeps it open.
	CreatedAgo, MergedAgo int
	// Text names the story's key in the title (an explicit-text link) instead of a native link;
	// Heuristic names nothing (GitHub and GitLab issues only: a link by time and repository).
	Text, Heuristic bool
	// Changes: the reviewer asked for changes once before approving.
	Changes bool
	// CIFailed failed CI runs came before the passing one.
	CIFailed int
	Files    []string
	// Deploy: the merge is deployed to production; DeployFailed: that deployment failed.
	Deploy, DeployFailed bool
}

type demoStory struct {
	Title string
	// Kind: feature, bug, support, chore, security.
	Kind       string
	CreatedAgo int
	DoneAgo    int // < 0: open
	Changes    []demoChange
	// Blocks are indexes of the team's stories this one blocks.
	Blocks []int
}

func feature(title string, created, done int, changes ...demoChange) demoStory {
	return demoStory{Title: title, Kind: "feature", CreatedAgo: created, DoneAgo: done, Changes: changes}
}

func bug(title string, created, done int, changes ...demoChange) demoStory {
	return demoStory{Title: title, Kind: "bug", CreatedAgo: created, DoneAgo: done, Changes: changes}
}

func support(title string, created, done int, changes ...demoChange) demoStory {
	return demoStory{Title: title, Kind: "support", CreatedAgo: created, DoneAgo: done, Changes: changes}
}

func chore(title string, created, done int, changes ...demoChange) demoStory {
	return demoStory{Title: title, Kind: "chore", CreatedAgo: created, DoneAgo: done, Changes: changes}
}

func security(title string, created, done int, changes ...demoChange) demoStory {
	return demoStory{Title: title, Kind: "security", CreatedAgo: created, DoneAgo: done, Changes: changes}
}

func change(repo string, created, merged int) demoChange {
	return demoChange{Repo: repo, CreatedAgo: created, MergedAgo: merged}
}

func (c demoChange) text() demoChange      { c.Text = true; return c }
func (c demoChange) heuristic() demoChange { c.Heuristic = true; return c }
func (c demoChange) changes() demoChange   { c.Changes = true; return c }
func (c demoChange) ci(failed int) demoChange {
	c.CIFailed = failed
	return c
}
func (c demoChange) files(paths ...string) demoChange { c.Files = paths; return c }
func (c demoChange) deploy() demoChange               { c.Deploy = true; return c }
func (c demoChange) deployFails() demoChange          { c.Deploy, c.DeployFailed = true, true; return c }

func (s demoStory) blocks(indexes ...int) demoStory { s.Blocks = indexes; return s }

// Payments (Jira PAY): steady delivery, three open blocker chains, two blockers already done. Its
// stories also land four pull requests in shared-api.
var demoPaymentsStories = []demoStory{
	feature("Card tokenization for saved cards", 88, 74, change("payments-gateway", 82, 75).deploy(), change("shared-api", 80, 76)),
	bug("Duplicate capture on gateway timeout", 85, 80, change("payments-gateway", 83, 81).ci(1).deploy()),
	feature("Multi-currency ledger balances", 79, 60, change("payments-ledger", 72, 62), change("shared-api", 70, 63).text()),
	chore("Upgrade the PSP client library", 70, 66, change("payments-gateway", 68, 66).deploy()),
	feature("Partial refunds", 66, 48, change("payments-gateway", 60, 50).changes().deploy(), change("shared-api", 58, 51)),
	bug("Ledger rounding on FX conversion", 58, 55, change("payments-ledger", 57, 55).ci(1)),
	feature("Payout scheduling API", 54, 40, change("payments-ledger", 50, 41), change("shared-api", 47, 42)),
	// Done blockers: a closed chain.
	chore("Freeze the settlement file layout", 50, 44, change("payments-ledger", 48, 45).text()).blocks(8),
	feature("Settlement file importer", 46, 30, change("payments-ledger", 40, 31).deploy()),
	chore("Retire the v1 refund endpoint", 38, 33, change("payments-gateway", 36, 34).text().deploy()).blocks(10),
	feature("Refund status webhooks", 34, 20, change("payments-gateway", 28, 21).deploy()),
	// Open chain 1: settlement v2 blocks reconciliation blocks payout reporting.
	feature("Settlement file format v2", 26, -1, change("payments-ledger", 22, -1)).blocks(12),
	feature("Bank reconciliation job", 24, -1, change("payments-ledger", 18, -1)).blocks(13),
	feature("Payout reporting for finance", 21, -1, change("payments-gateway", 12, -1).text()),
	// Open chain 2: the PSP contract blocks the 3DS2 rollout.
	support("PSP contract renewal and new credentials", 19, -1, change("payments-gateway", 17, -1)).blocks(15),
	feature("3DS2 challenge flow rollout", 16, -1, change("payments-gateway", 11, -1).changes()),
	// Open chain 3: the ledger schema migration blocks refund entries blocks chargebacks.
	chore("Ledger schema migration to event rows", 14, -1, change("payments-ledger", 13, -1).ci(2)).blocks(17),
	feature("Refund ledger entries", 10, -1, change("payments-ledger", 7, -1)).blocks(18),
	feature("Chargeback automation", 6, -1, change("payments-gateway", 4, -1).text()),
}

// Checkout (Linear CHK): features until a month ago, then mostly regressions and hotfixes.
var demoCheckoutStories = []demoStory{
	feature("One-page checkout", 89, 70, change("checkout-web", 84, 72).deploy(), change("checkout-api", 82, 71).deploy()),
	feature("Address autocomplete", 86, 68, change("checkout-web", 76, 69)),
	feature("Saved payment methods at checkout", 80, 60, change("checkout-api", 70, 61).deploy(), change("shared-api", 68, 62)),
	feature("Gift card redemption", 75, 55, change("checkout-api", 64, 56).deploy()),
	feature("Express checkout buttons", 70, 50, change("checkout-web", 58, 51).text()),
	feature("Order summary redesign", 64, 45, change("checkout-web", 50, 46)),
	feature("Delivery slot picker", 60, 40, change("checkout-api", 47, 41).deploy(), change("checkout-web", 45, 42)),
	feature("Promo code stacking rules", 52, 35, change("checkout-api", 40, 36).deploy()),
	feature("Guest checkout email capture", 45, 32, change("checkout-web", 36, 33)),
	bug("Cart total wrong after promo removal", 28, 26, change("checkout-api", 27, 26).ci(2).deployFails(), change("checkout-api", 26, 25).deploy()),
	bug("Hotfix: payment step spinner never ends", 25, 24, change("checkout-web", 25, 24).ci(1)),
	bug("Address autocomplete drops apartment numbers", 22, 19, change("checkout-web", 21, 19).text()),
	bug("Regression: delivery slots show past times", 18, 16, change("checkout-api", 17, 16).ci(1).deploy()),
	support("Investigate abandoned carts spike", 15, 11, change("checkout-api", 13, 11).deployFails()),
	bug("Hotfix: gift cards double-applied", 10, 9, change("checkout-api", 10, 9).ci(2).deploy()),
	bug("Revert express checkout on Safari", 7, 6, change("checkout-web", 7, 6)),
	feature("Split shipments", 20, -1, change("checkout-api", 14, -1)),
}

// Support Tools (Jira SUP): mostly bugs, escalations and support requests.
var demoSupportStories = []demoStory{
	support("Escalation: agents cannot merge tickets", 87, 85, change("support-console", 86, 85).ci(1).deploy()),
	bug("Macro variables render blank", 83, 79, change("support-macros", 81, 79)),
	support("Bulk close tool for the spam wave", 78, 76, change("support-console", 77, 76).deploy()),
	bug("Search times out on large inboxes", 72, 65, change("support-console", 70, 66).changes().deploy()),
	feature("Canned responses in two languages", 68, 52, change("support-macros", 60, 53)),
	support("Escalation: SLA timer pauses on weekends", 62, 59, change("support-console", 61, 59).ci(1).deploy()),
	bug("Attachment preview breaks on PDF", 55, 50, change("support-console", 53, 50)),
	support("Export a customer's ticket history", 49, 47, change("support-console", 48, 47).text()),
	bug("Macro import drops tags", 43, 40, change("support-macros", 42, 40)),
	support("Escalation: duplicate notifications to customers", 37, 35, change("support-console", 36, 35).ci(2).deploy()),
	bug("Ticket assignment loops between two queues", 31, 27, change("support-console", 29, 27).deploy()),
	support("Data fix: reopen tickets closed by the bulk tool", 24, 23, change("support-console", 24, 23).text()),
	feature("Agent workload view", 40, 18, change("support-console", 25, 19).deploy()),
	bug("Inbox counter off by one", 16, 14, change("support-console", 15, 14)),
	support("Escalation: VIP tickets not flagged", 9, 7, change("support-macros", 8, 7).ci(1)),
	support("Restore deleted macros for the EU team", 4, -1, change("support-macros", 3, -1)),
}

// Identity (GitHub issues, team grant): security work and security alerts on its repositories.
var demoIdentityStories = []demoStory{
	feature("Passkey sign-in", 86, 60, change("identity-service", 75, 62).deploy(), change("identity-sdk", 70, 61)),
	security("Rotate the token signing keys", 80, 77, change("identity-service", 79, 77).text().deploy()),
	bug("Refresh tokens survive a password reset", 66, 63, change("identity-service", 65, 63).ci(1).deploy()),
	feature("SDK: silent token renewal", 58, 41, change("identity-sdk", 48, 42)),
	security("Pin the JWT library after the advisory", 45, 44, change("identity-service", 45, 44).deploy()),
	feature("Org-level session policies", 38, 22, change("identity-service", 30, 23).changes().deploy()),
	security("Rate-limit the password reset endpoint", 26, 25, change("identity-service", 26, 25).text().deploy()),
	bug("SDK leaks the refresh token to logs", 15, 13, change("identity-sdk", 14, 13).ci(1)),
	security("Upgrade the vulnerable XML parser", 8, -1, change("identity-service", 6, -1).heuristic()),
	feature("SCIM provisioning", 12, -1, change("identity-service", 9, -1).heuristic()),
}

// Mobile (Linear MOB): steady delivery, every review by the same person.
var demoMobileStories = []demoStory{
	feature("Biometric unlock", 88, 70, change("mobile-app", 80, 71), change("mobile-bff", 78, 72).deploy()),
	feature("Offline order history", 82, 62, change("mobile-app", 70, 63).changes()),
	bug("Push notifications open the wrong screen", 74, 71, change("mobile-app", 73, 71)),
	feature("In-app returns", 69, 48, change("mobile-bff", 58, 50).deploy(), change("mobile-app", 55, 49)),
	feature("Dark mode", 60, 39, change("mobile-app", 47, 40).changes()),
	bug("Crash on Android 12 camera permission", 50, 47, change("mobile-app", 49, 47).ci(1)),
	feature("Wishlist sync", 44, 27, change("mobile-bff", 35, 29).deploy(), change("mobile-app", 32, 28).text()),
	chore("Upgrade the React Native runtime", 36, 30, change("mobile-app", 34, 30).ci(2)),
	feature("Order tracking map", 30, 12, change("mobile-bff", 22, 14).deploy(), change("mobile-app", 18, 13).changes()),
	bug("Cart badge count stale after logout", 14, 11, change("mobile-app", 13, 11)),
	feature("Product reviews with photos", 12, -1, change("mobile-app", 8, -1)),
	feature("Store pickup", 6, -1, change("mobile-bff", 4, -1)),
}

// Delivery (GitLab root group): the deploy pipeline breaks often; shared-infra changes.
var demoDeliveryStories = []demoStory{
	chore("Move the deploy pipeline to environments", 88, 80, change("chaos-gl/deploy-pipeline", 86, 81).files("ci/deploy.gitlab-ci.yml", "deploy/rollout.yaml").deploy()),
	feature("Canary stage in the deploy pipeline", 82, 66, change("chaos-gl/deploy-pipeline", 75, 67).ci(1).files("ci/deploy.gitlab-ci.yml", "deploy/canary.yaml").deploy()),
	bug("Rollback job does not restore the config map", 70, 64, change("chaos-gl/deploy-pipeline", 66, 64).ci(2).files("deploy/rollback.sh").deploy()),
	chore("Shared runner pool for all projects", 68, 58, change("chaos-gl/shared-infra", 63, 59).files("runners/pool.tf")),
	feature("Release notes generator", 64, 45, change("chaos-gl/release-tools", 52, 46)),
	chore("Terraform state backend migration", 57, 50, change("chaos-gl/shared-infra", 54, 51).ci(1).files("terraform/backend.tf")),
	bug("Deploy pipeline hangs on the smoke test", 46, 41, change("chaos-gl/deploy-pipeline", 44, 41).ci(2).files("ci/deploy.gitlab-ci.yml", "deploy/smoke.sh").deploy()),
	chore("Harden the cluster network policies", 40, 34, change("chaos-gl/shared-infra", 38, 35).files("k8s/network-policies.yaml")),
	bug("Pipeline cache poisoning after the runner upgrade", 30, 27, change("chaos-gl/deploy-pipeline", 29, 27).ci(3).files("ci/deploy.gitlab-ci.yml").deployFails()),
	support("Incident follow-up: add a deploy freeze switch", 24, 20, change("chaos-gl/deploy-pipeline", 23, 20).ci(1).files("deploy/freeze.sh").deploy()),
	bug("Blue-green switch leaves old pods serving", 18, 15, change("chaos-gl/deploy-pipeline", 17, 15).ci(2).files("deploy/rollout.yaml").deployFails()),
	support("Incident follow-up: page on failed rollouts", 12, 10, change("chaos-gl/deploy-pipeline", 11, 10).files("deploy/alerts.yaml").deploy()),
	chore("Pin base images in the shared infra", 9, -1, change("chaos-gl/shared-infra", 7, -1).files("images/base.lock")),
	bug("Deploy pipeline retries mask real failures", 5, -1, change("chaos-gl/deploy-pipeline", 4, -1).ci(2).files("ci/deploy.gitlab-ci.yml")),
}

// Data (GitLab subgroup data): a healthy, steady flow; two of its issues are closed by shared-infra
// merge requests.
var demoDataStories = []demoStory{
	feature("Incremental loads for the orders table", 85, 72, change("chaos-gl/data/warehouse-etl", 80, 73).deploy()),
	feature("Metrics store retention policies", 78, 64, change("chaos-gl/data/metrics-store", 70, 65)),
	chore("Warehouse cluster autoscaling", 70, 61, change("chaos-gl/shared-infra", 66, 62).files("terraform/warehouse.tf")),
	feature("Late-arriving event handling", 64, 50, change("chaos-gl/data/warehouse-etl", 56, 51).deploy()),
	bug("Duplicate rows after a backfill", 52, 49, change("chaos-gl/data/warehouse-etl", 51, 49)),
	feature("Downsampling for old metrics", 47, 33, change("chaos-gl/data/metrics-store", 38, 34)),
	chore("Object storage lifecycle rules", 40, 29, change("chaos-gl/shared-infra", 33, 30).files("terraform/storage.tf")),
	feature("Data quality checks on ingest", 35, 19, change("chaos-gl/data/warehouse-etl", 24, 20).deploy()),
	feature("Query cache for dashboards", 26, 12, change("chaos-gl/data/metrics-store", 16, 13)),
	feature("Customer cohort tables", 14, 5, change("chaos-gl/data/warehouse-etl", 9, 6).deploy()),
}

// Web (GitLab subgroup web): little recorded work, none in the last month.
var demoWebStories = []demoStory{
	feature("Marketing site pricing page", 80, 66, change("chaos-gl/web/marketing-site", 72, 67)),
	chore("Design system color tokens", 62, 50, change("chaos-gl/web/design-system", 55, 51)),
	bug("Footer links broken on mobile", 48, -1),
}

// Delivery's incidents on the deploy pipeline (GitLab incident issues).
type demoIncident struct {
	Title, Severity string
	StartedAgo      int
	Hour            int
	ResolvedAfter   int // hours; < 0: open
}

var demoDeliveryIncidents = []demoIncident{
	{"Deploy to production stuck at 40%", "HIGH", 84, 10, 3},
	{"Config map not restored after rollback", "MEDIUM", 69, 14, 6},
	{"Canary stage promoted a failing build", "HIGH", 61, 9, 2},
	{"Smoke test timeout blocked all deploys", "MEDIUM", 45, 16, 5},
	{"Release tag pushed without artifacts", "LOW", 39, 11, 20},
	{"Runner pool exhausted during peak deploys", "HIGH", 31, 8, 4},
	{"Poisoned pipeline cache shipped stale assets", "CRITICAL", 28, 13, 7},
	{"Rollout left two versions serving checkout", "CRITICAL", 17, 15, 9},
	{"Deploy freeze switch ignored by hotfix lane", "MEDIUM", 12, 10, 3},
	{"Failed rollout not paged for 40 minutes", "HIGH", 9, 7, 2},
	{"Retries masked a broken database migration", "CRITICAL", 4, 9, -1},
	{"Canary metrics missing for the payments service", "LOW", 2, 11, -1},
}

// demoIssueText is a story's tracker description and demoChangeText its pull or merge request body: a
// few sentences in the voice of the work's kind, so a unit's text says what kind of work it is. They name
// no issue key, number or URL: a link stays the link the change declares.
func demoIssueText(story demoStory) string {
	subject := strings.ToLower(story.Title[:1]) + story.Title[1:]
	switch story.Kind {
	case "feature":
		return "Customers and the product team asked for " + subject + ". Today people work around the gap by hand, which slows them down and keeps coming up in sales and support calls. This story delivers the capability end to end: the API change, the user-facing screens, the documentation, and a staged rollout behind a feature flag so we can measure adoption before turning it on for everyone."
	case "bug":
		return "Users report a defect: " + subject + ". It started after a recent release and breaks a core flow for a subset of accounts, and support has linked several customer tickets to it. We need a fix with a regression test, a short root-cause note, and a check that no stored data was corrupted while the defect was live in production."
	case "support":
		return "Unplanned request from the support team: " + subject + ". A customer escalation reached the on-call engineer and the queue is waiting on us, so this interrupts the planned sprint work. Scope is limited to unblocking the affected customers quickly, posting status in the support channel, and writing down what would prevent the same escalation next time."
	case "security":
		return "Security work: " + subject + ". A scanner finding and an internal review flagged a weakness that could expose customer accounts or tokens if it stays in place. The goal is to remove the vulnerable path, rotate any secrets that may be affected, add a test that pins the fix, and record the change for the next compliance review with the security team."
	default:
		return "Maintenance work: " + subject + ". The current setup is outdated, adds friction to every change, and raises the risk of slow builds and surprises during upgrades. This pays down technical debt without changing user-facing behaviour: update the dependency or configuration, remove dead code, keep the tests green, and document anything operators must know."
	}
}

func demoChangeText(story demoStory) string {
	subject := strings.ToLower(story.Title[:1]) + story.Title[1:]
	switch story.Kind {
	case "feature":
		return "Implements " + subject + ". Adds the new endpoint and the user interface changes behind a feature flag, with unit and integration tests for the main paths and the edge cases found in review. Rollout plan: enable it for internal accounts first, watch error rates and latency on the dashboard for a day, then enable it for all customers and remove the flag."
	case "bug":
		return "Fixes the defect: " + subject + ". Root cause: an unchecked edge case in the request handling path that only appears for some accounts. This change corrects the logic, adds a regression test that fails without the fix, and adds logging so we can confirm in production that the error rate drops back to its normal level."
	case "support":
		return "Unblocks the support escalation: " + subject + ". An unplanned change made on request of the on-call engineer; it is intentionally small and safe to revert. It touches only the affected path, includes a test, and adds a note to the support runbook so the team can handle the same case without engineering help next time."
	case "security":
		return "Security fix: " + subject + ". Removes the vulnerable code path, upgrades the affected library to the patched version, and adds a test that fails on the old behaviour. Tokens and secrets that could have been exposed are rotated as part of the rollout, and the change is recorded for the compliance review with the security team."
	default:
		return "Maintenance: " + subject + ". Updates the outdated dependency and configuration, removes code that is no longer used, and keeps behaviour unchanged for users. Build and test times should improve slightly. No migration is needed; operators only need to note the new configuration default described in the documentation change included here."
	}
}

//go:build integration

package fixturescli

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

// The mixed-provider demo world: one organization whose eight teams come from four providers and own
// distinct projects and repositories, written by the same production producers as the provider worlds.
// Each team has a shape a delivery question can tell apart: blocker chains (payments), an incident-heavy
// service with a failing deploy pipeline (delivery), a feature-delivery drop in the last 30 days
// (checkout), unplanned work (support tools), a healthy flow (data), security alerts (identity), review
// concentration (mobile) and a sparse history (web). Two repositories are shared: shared-api (payments
// leads, checkout co-owns) and chaos-gl/shared-infra (delivery leads, data co-owns), by unequal link counts.

var demoInstant = time.Date(2026, 10, 12, 12, 0, 0, 0, time.UTC)

const demoDepthDays = 91

// demoAt is an instant daysAgo whole days before the world's instant, at hour:minute UTC (hour < 12 on
// day 0 keeps it before the instant).
func demoAt(daysAgo, hour, minute int) string {
	day := demoInstant.AddDate(0, 0, -daysAgo)
	return time.Date(day.Year(), day.Month(), day.Day(), hour, minute, 0, 0, time.UTC).Format(time.RFC3339)
}

func demoSHA(parts ...any) string {
	sum := sha256.Sum256([]byte(fmt.Sprint(parts...)))
	return hex.EncodeToString(sum[:20])
}

// ---- people ----

var (
	// GitHub accounts (pull request authors and reviewers).
	dmPia   = githubUser{"pia-dm", 7301, "Pia Example", "pia@demo.example"}
	dmQuin  = githubUser{"quin-dm", 7302, "Quin Example", "quin@demo.example"}
	dmRaf   = githubUser{"raf-dm", 7303, "Raf Example", "raf@demo.example"}
	dmSol   = githubUser{"sol-dm", 7311, "Sol Example", "sol@demo.example"}
	dmTeo   = githubUser{"teo-dm", 7312, "Teo Example", "teo@demo.example"}
	dmUma   = githubUser{"uma-dm", 7313, "Uma Example", "uma@demo.example"}
	dmVic   = githubUser{"vic-dm", 7321, "Vic Example", "vic@demo.example"}
	dmWen   = githubUser{"wen-dm", 7322, "Wen Example", "wen@demo.example"}
	dmXan   = githubUser{"xan-dm", 7331, "Xan Example", "xan@demo.example"}
	dmYara  = githubUser{"yara-dm", 7332, "Yara Example", "yara@demo.example"}
	dmZed   = githubUser{"zed-dm", 7333, "Zed Example", "zed@demo.example"}
	dmAbe   = githubUser{"abe-dm", 7341, "Abe Example", "abe@demo.example"}
	dmBea   = githubUser{"bea-dm", 7342, "Bea Example", "bea@demo.example"}
	dmCyd   = githubUser{"cyd-dm", 7343, "Cyd Example", "cyd@demo.example"}
	dmMoLed = githubUser{"moe-dm", 7344, "Moe Example", "moe@demo.example"}

	// GitLab accounts.
	dmDax = gitlabUser{8301, "dax-dm", "Dax Example", "dax@demo.example"}
	dmEva = gitlabUser{8302, "eva-dm", "Eva Example", "eva@demo.example"}
	dmFin = gitlabUser{8303, "fin-dm", "Fin Example", "fin@demo.example"}
	dmGia = gitlabUser{8311, "gia-dm", "Gia Example", "gia@demo.example"}
	dmHal = gitlabUser{8312, "hal-dm", "Hal Example", "hal@demo.example"}
	dmIda = gitlabUser{8313, "ida-dm", "Ida Example", "ida@demo.example"}
	dmJon = gitlabUser{8321, "jon-dm", "Jon Example", "jon@demo.example"}
	dmKit = gitlabUser{8322, "kit-dm", "Kit Example", "kit@demo.example"}

	// Jira accounts.
	dmJPia  = jiraUser{"712020:dm-pia", "Pia Example", "pia@demo.example"}
	dmJQuin = jiraUser{"712020:dm-quin", "Quin Example", "quin@demo.example"}
	dmJRaf  = jiraUser{"712020:dm-raf", "Raf Example", "raf@demo.example"}
	dmJVic  = jiraUser{"712020:dm-vic", "Vic Example", "vic@demo.example"}
	dmJWen  = jiraUser{"712020:dm-wen", "Wen Example", "wen@demo.example"}

	// Linear accounts.
	dmLSol = linearUser{"5e1d0000-0000-4000-8000-0000000db011", "Sol Example", "sol@demo.example"}
	dmLTeo = linearUser{"5e1d0000-0000-4000-8000-0000000db012", "Teo Example", "teo@demo.example"}
	dmLUma = linearUser{"5e1d0000-0000-4000-8000-0000000db013", "Uma Example", "uma@demo.example"}
	dmLAbe = linearUser{"5e1d0000-0000-4000-8000-0000000db041", "Abe Example", "abe@demo.example"}
	dmLBea = linearUser{"5e1d0000-0000-4000-8000-0000000db042", "Bea Example", "bea@demo.example"}
)

// ---- teams, projects and repositories ----

// GitHub: the identity team holds its two repositories by grant; every other GitHub repository is
// reached through the Jira and Linear issues linked to its pull requests.
var demoGitHubTeams = []githubTeam{
	{131, "identity", "Identity", "Owns sign-in and tokens", []string{"identity-service", "identity-sdk"}, []githubUser{dmXan, dmYara, dmZed}},
}

var demoGitHubRepoNames = []string{
	"payments-gateway", "payments-ledger", "shared-api",
	"checkout-api", "checkout-web",
	"support-console", "support-macros",
	"identity-service", "identity-sdk",
	"mobile-app", "mobile-bff",
}

// Atlassian Teams connected to the PAY and SUP spaces.
var (
	demoJiraPAY = jiraProject{"10101", "PAY", "Payments"}
	demoJiraSUP = jiraProject{"10102", "SUP", "Support Tools"}
)

var demoAtlassianTeams = []atlassianTeam{
	{"9a1f0c3e-0000-4000-8000-0000000d0001", "Payments", "ACTIVE", []jiraUser{dmJPia, dmJQuin, dmJRaf}, []jiraProject{demoJiraPAY}},
	{"9a1f0c3e-0000-4000-8000-0000000d0002", "Support Tools", "ACTIVE", []jiraUser{dmJVic, dmJWen}, []jiraProject{demoJiraSUP}},
}

// Linear teams and their projects (the lookups in the linear world read these too).
var demoLinearTeams = []linearTeam{
	{"7e40c0de-0000-4000-8000-0000000dc001", "CHK", "Checkout", []linearUser{dmLSol, dmLTeo, dmLUma}},
	{"7e40c0de-0000-4000-8000-0000000dc002", "MOB", "Mobile", []linearUser{dmLAbe, dmLBea}},
}

var demoLinearProjects = []linearProject{
	{"9f00d0e1-0000-4000-8000-0000000dd001", "Checkout Experience", []string{"CHK"}, "2026-03-02T09:00:00Z"},
	{"9f00d0e1-0000-4000-8000-0000000dd002", "Mobile App", []string{"MOB"}, "2026-03-09T09:00:00Z"},
}

// GitLab: the root group is the delivery team; data and web are its subgroups.
var demoGitLabGroups = []gitlabGroup{
	{3000, "chaos-gl", "Delivery", []string{"chaos-gl/deploy-pipeline", "chaos-gl/release-tools", "chaos-gl/shared-infra"}, []gitlabUser{dmDax, dmEva, dmFin}},
	{3001, "chaos-gl/data", "Data", []string{"chaos-gl/data/warehouse-etl", "chaos-gl/data/metrics-store"}, []gitlabUser{dmGia, dmHal, dmIda}},
	{3002, "chaos-gl/web", "Web", []string{"chaos-gl/web/marketing-site", "chaos-gl/web/design-system"}, []gitlabUser{dmJon, dmKit}},
}

var demoGitLabProjectPaths = []string{
	"chaos-gl/deploy-pipeline", "chaos-gl/release-tools", "chaos-gl/shared-infra",
	"chaos-gl/data/warehouse-etl", "chaos-gl/data/metrics-store",
	"chaos-gl/web/marketing-site", "chaos-gl/web/design-system",
}

func demoGitLabProjectID(index int) int { return 51001 + index }

func init() {
	for index, name := range demoGitHubRepoNames {
		githubRepos[name] = githubRepo{9301 + index, name, strings.ReplaceAll(name, "-", " "), "2025-02-03T09:00:00Z"}
	}
	for index, path := range demoGitLabProjectPaths {
		gitlabProjects[path] = gitlabProject{demoGitLabProjectID(index), path, path[strings.LastIndex(path, "/")+1:], "2025-02-10T09:00:00Z"}
	}
}

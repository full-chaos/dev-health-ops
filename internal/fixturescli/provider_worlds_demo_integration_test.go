//go:build integration

package fixturescli

import (
	"crypto/sha256"
	"encoding/hex"
	"flag"
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

// The world has no date of its own: every time in it is so many days before its as-of day, which a run
// is given. A freeze takes any day (-demo-as-of); the committed world was frozen at demoFrozenAsOf, the
// default, so its freshness run writes it again byte for byte. A load moves the world by whole days so
// its as-of day is the load day (FrozenWorld.WholeDays), so the day it was frozen at says nothing about
// the days a loaded organization holds.
const demoFrozenAsOf = "2026-10-12"

var demoAsOf = flag.String("demo-as-of", demoFrozenAsOf, "the day (YYYY-MM-DD) the demo world ends: every time in it is relative to it")

// demoInstant is this run's instant: noon UTC of its as-of day.
func demoInstant() time.Time { return demoInstantOf(*demoAsOf) }

// demoInstantOf is the world's instant on an as-of day: noon UTC.
func demoInstantOf(asOf string) time.Time {
	day, err := time.Parse(time.DateOnly, asOf)
	if err != nil {
		panic(fmt.Sprintf("-demo-as-of %q: %v", asOf, err))
	}
	return day.Add(12 * time.Hour)
}

// demoDepthDays is the world's history: 180 days before its as-of day, and the as-of day.
const demoDepthDays = 181

// demoAt is an instant daysAgo whole days before the world's instant, at hour:minute UTC (hour < 12 on
// day 0 keeps it before the instant).
func demoAt(daysAgo, hour, minute int) string {
	day := demoInstant().AddDate(0, 0, -daysAgo)
	return time.Date(day.Year(), day.Month(), day.Day(), hour, minute, 0, 0, time.UTC).Format(time.RFC3339)
}

// demoDay is the day daysAgo whole days before the world's as-of day (YYYY-MM-DD).
func demoDay(daysAgo int) string {
	return demoInstant().AddDate(0, 0, -daysAgo).Format(time.DateOnly)
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
	{ID: "9f00d0e1-0000-4000-8000-0000000dd001", Name: "Checkout Experience", Teams: []string{"CHK"}},
	{ID: "9f00d0e1-0000-4000-8000-0000000dd002", Name: "Mobile App", Teams: []string{"MOB"}},
}

// demoLinearProjectsDated are the Linear projects with their dates, relative to the as-of day: both
// started before the world's history and are committed to a target date in the coming weeks, Checkout
// the sooner one.
func demoLinearProjectsDated() []linearProject {
	dated := append([]linearProject{}, demoLinearProjects...)
	for index, plan := range []struct{ created, target int }{{230, -24}, {223, -52}} {
		dated[index].CreatedAt = demoAt(plan.created, 9, 0)
		dated[index].StartDate = demoDay(plan.created)
		dated[index].TargetDate = demoDay(plan.target)
		dated[index].UpdatedAt = demoAt(1, 9, 0)
	}
	return dated
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
	registerDemoRepositories()
}

// registerDemoRepositories names the demo repositories and projects in the provider worlds' catalogs,
// dated relative to the as-of day: created long before the world's history, last pushed the day before
// it ends. It runs again when the world's answers are built, after the flags are parsed.
func registerDemoRepositories() {
	for index, name := range demoGitHubRepoNames {
		githubRepos[name] = githubRepo{ID: 9301 + index, Name: name, Description: strings.ReplaceAll(name, "-", " "),
			CreatedAt: demoAt(600, 9, 0), PushedAt: demoAt(1, 8, 0)}
	}
	for index, path := range demoGitLabProjectPaths {
		gitlabProjects[path] = gitlabProject{ID: demoGitLabProjectID(index), Path: path, Name: path[strings.LastIndex(path, "/")+1:],
			CreatedAt: demoAt(593, 9, 0), LastActivityAt: demoAt(1, 8, 0)}
	}
}

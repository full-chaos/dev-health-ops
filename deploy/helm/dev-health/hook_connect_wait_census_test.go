package devhealth_test

import (
	"fmt"
	"path"
	"sort"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// CHAOS-12666: since the release-wide NetworkPolicies, a new pod's first TCP
// connect can be refused or dropped while its policy rules are programmed. A
// hook Job, a one-shot Job and an init container start a new pod and connect at
// once, so each entrypoint they run must wait for its first store connect
// (bounded, logged, fail closed: postgres.FirstConnect, or auth-migrate's
// newDatabaseWait) or connect to no store. This census renders the chart,
// lists every Job container and every init container, and fails on an
// entrypoint this table does not know, and on a table row no render shows.
var connectWaits = map[string]string{
	"dho workers routes apply":         "wait: internal/workersctl/main.go configureRuntime, postgres.OpenRuntimePoolsWaiting",
	"dho migrate upgrade":              "wait: internal/pgmigrate/command.go run, postgres.ConnectWaiting; internal/chmigrate/command.go run, clickhouse.OpenWith",
	"dho migrate postgres build-index": "wait: internal/pgmigrate/indexbuild.go buildIndex, postgres.ConnectWaiting",
	"dho migrate roles":                "wait: internal/rivermigrate/roles.go ExecuteRoles, postgres.OpenWaiting",
	"dho migrate river":                "wait: internal/rivermigrate/rivermigrate.go Execute, postgres.OpenWaiting",
	"dho auth users-backfill":          "wait: internal/auth/usersbackfill/command.go connect, FirstConnect.Do on Ping",
	"dho auth users-reconcile":         "wait: internal/auth/usersbackfill/command.go connect, FirstConnect.Do on Ping",
	"dho auth orgs-backfill":           "wait: internal/auth/usersbackfill/command.go connect, FirstConnect.Do on Ping",
	"dho auth legacy-link-purge":       "wait: internal/auth/usersbackfill/command.go connect, FirstConnect.Do on Ping",
	"auth-migrate":                     "wait: cmd/auth-migrate/main.go, newDatabaseWait",
	"auth-migrate grants":              "wait: cmd/auth-migrate/main.go, newDatabaseWait",
	"dho version":                      "none: prints the build, connects to no store",
	"sh -ec until pg_isready":          "wait: the until-loop itself (local bundled PostgreSQL only), bounded by the Job's activeDeadlineSeconds",
	"install -m 0400":                  "none: copies the signing key file",
	"sh -c for n":                      "none: links the users.d files",
}

// entrypointBinaries maps an image repository (its last path segment) to the
// binary its ENTRYPOINT runs, for a container with no command.
var entrypointBinaries = map[string]string{
	"dho":                        "dho",
	"dev-health-go-dho":          "dho",
	"dev-health-go-operator":     "dho",
	"dev-health-go-auth-migrate": "auth-migrate",
}

type censusEntry struct {
	key, where string
}

func stringList(v any) []string {
	var out []string
	for _, item := range asList(v) {
		s, _ := item.(string)
		out = append(out, s)
	}
	return out
}

func imageRepositoryName(image string) string {
	if i := strings.Index(image, "@"); i >= 0 {
		image = image[:i]
	}
	name := path.Base(image)
	if i := strings.Index(name, ":"); i >= 0 {
		name = name[:i]
	}
	return name
}

// entrypointKey names what a container runs: its command's first word, flag
// and two script words, or its image's binary and the leading verb words of
// its args (up to the first flag). ok false: an image with no command whose
// binary is unknown.
func entrypointKey(container map[string]any) (string, bool) {
	if command := stringList(container["command"]); len(command) > 0 {
		words := command[:1]
		if len(command) > 1 {
			words = append(words, command[1])
		}
		if len(command) > 2 {
			script := strings.Fields(command[2])
			words = append(words, script[:min(2, len(script))]...)
		}
		return strings.Join(words, " "), true
	}
	image, _ := container["image"].(string)
	binary, known := entrypointBinaries[imageRepositoryName(image)]
	if !known {
		return "image " + image, false
	}
	words := []string{binary}
	for _, arg := range stringList(container["args"]) {
		if strings.HasPrefix(arg, "-") {
			break
		}
		words = append(words, arg)
	}
	return strings.Join(words, " "), true
}

// censusEntries lists every Job container and every init container of the
// rendered documents.
func censusEntries(docs []map[string]any) []censusEntry {
	var entries []censusEntry
	for _, doc := range docs {
		kind, _ := doc["kind"].(string)
		name, _ := at(doc, "metadata", "name").(string)
		var spec map[string]any
		switch kind {
		case "Job", "Deployment", "StatefulSet", "DaemonSet", "ReplicaSet":
			spec, _ = at(doc, "spec", "template", "spec").(map[string]any)
		case "CronJob":
			spec, _ = at(doc, "spec", "jobTemplate", "spec", "template", "spec").(map[string]any)
		case "Pod":
			spec, _ = doc["spec"].(map[string]any)
		}
		if spec == nil {
			continue
		}
		groups := []string{"initContainers"}
		if kind == "Job" || kind == "CronJob" || kind == "Pod" {
			groups = append(groups, "containers")
		}
		for _, group := range groups {
			for _, item := range asList(spec[group]) {
				container, _ := item.(map[string]any)
				key, _ := entrypointKey(container)
				entries = append(entries, censusEntry{key: key, where: fmt.Sprintf("%s/%s %s %v", kind, name, group, container["name"])})
			}
		}
	}
	return entries
}

// censusViolations names each entry whose entrypoint the table does not know.
func censusViolations(entries []censusEntry) []string {
	var violations []string
	for _, entry := range entries {
		if _, known := connectWaits[entry.key]; !known {
			violations = append(violations, fmt.Sprintf("%s runs %q: no first-connect wait is known for it; give it a bounded wait (postgres.FirstConnect) and add it to connectWaits, or add it as \"none: <why>\"", entry.where, entry.key))
		}
	}
	return violations
}

func censusRenders() map[string][]string {
	hooks := append(append([]string{}, authBase...),
		"--set", "migrations.hook.provisionRoles.enabled=true",
		"--set", "migrations.hook.riverMigrate.enabled=true",
		"--set", "migrations.hook.riverMigrate.image="+pinnedOperatorImage,
		"--set", "migrations.hook.routeActivate.image="+pinnedOperatorImage,
		"--set", "migrations.hook.openUnitsIndex.enabled=true",
		"--set", "migrations.hook.streakIndex.enabled=true",
		"--set", "secrets.create=true",
	)
	local := append(append([]string{}, hooks...),
		"--set", "postgresql.enabled=true",
		"--set", "migrations.hook.localBundledPostgres=true",
		"--set-json", `migrations.hook.events=["post-install","pre-upgrade"]`,
		"--set", "clickhouse.enabled=true",
		"--set-json", `clickhouse.usersd={"a.xml":"<clickhouse/>"}`,
		"--set", "goWorkers.enabled=true",
	)
	renders := map[string][]string{
		"defaults":               nil,
		"all hooks, external db": hooks,
		"all hooks, bundled db":  local,
	}
	for _, verb := range []string{"users-backfill", "users-reconcile", "orgs-backfill"} {
		renders["auth backfill "+verb] = append(append([]string{}, hooks...),
			"--set", "authService.backfill.enabled=true",
			"--set", "authService.backfill.verb="+verb,
			"--set", "authService.backfill.runId=census")
	}
	// legacy-link-purge renders only where users are owned (phase 4).
	purge := withSets(append(append([]string{}, backfillOn...), phase3Secrets...),
		"authService.backfill.verb=legacy-link-purge", "authService.backfill.runId=census",
		"authService.phase=4", "authService.usersOwnedStage=owned", "goApi.enabled=true")
	renders["auth backfill legacy-link-purge"] = append(append(append(purge, sessionReaders...), sessionSuccessor...), authTrusted...)
	return renders
}

func TestHookConnectWaitCensus(t *testing.T) {
	seen := map[string]bool{}
	renders := censusRenders()
	names := make([]string, 0, len(renders))
	for name := range renders {
		names = append(names, name)
	}
	sort.Strings(names)
	total := 0
	for _, name := range names {
		entries := censusEntries(renderDocs(t, renders[name]...))
		total += len(entries)
		for _, violation := range censusViolations(entries) {
			t.Errorf("render %q: %s", name, violation)
		}
		for _, entry := range entries {
			seen[entry.key] = true
		}
	}
	if total < 20 {
		t.Fatalf("only %d Job and init containers rendered: the census measures nothing", total)
	}
	for key := range connectWaits {
		if !seen[key] {
			t.Errorf("connectWaits row %q: no render runs it; remove the row", key)
		}
	}
}

// The census goes red on a hook Job, an init container and an image it does
// not know.
func TestHookConnectWaitCensusRefusesFixture(t *testing.T) {
	const fixture = `
kind: Job
metadata: {name: fixture-seed, annotations: {helm.sh/hook: pre-upgrade}}
spec:
  template:
    spec:
      containers:
        - name: seed
          image: ghcr.io/full-chaos/dev-health-go-operator@sha256:aaaa
          args: [admin, features, seed, --dry-run]
---
kind: Deployment
metadata: {name: fixture-app}
spec:
  template:
    spec:
      initContainers:
        - {name: wait-db, image: busybox:1, command: [nc, -z, db, "5432"]}
      containers:
        - {name: app, image: ghcr.io/full-chaos/dev-health-go-operator:sha-0123456789ab, args: [admin, features, seed]}
---
kind: Job
metadata: {name: fixture-unknown}
spec:
  template:
    spec:
      containers:
        - {name: run, image: ghcr.io/example/other:1}
`
	var docs []map[string]any
	decoder := yaml.NewDecoder(strings.NewReader(fixture))
	for {
		var doc map[string]any
		if err := decoder.Decode(&doc); err != nil {
			break
		}
		docs = append(docs, doc)
	}
	violations := censusViolations(censusEntries(docs))
	want := []string{
		`Job/fixture-seed containers seed runs "dho admin features seed"`,
		`Deployment/fixture-app initContainers wait-db runs "nc -z db"`,
		`Job/fixture-unknown containers run runs "image ghcr.io/example/other:1"`,
	}
	if len(violations) != len(want) {
		t.Fatalf("%d violations, want %d (a Deployment's main container is not a first-connect pod start):\n%s", len(violations), len(want), strings.Join(violations, "\n"))
	}
	for i, prefix := range want {
		if !strings.HasPrefix(violations[i], prefix) {
			t.Errorf("violation %d = %q, want prefix %q", i, violations[i], prefix)
		}
	}
}

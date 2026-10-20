package bigboy_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

func composeOneFileNoChainScript(g *guard) {
	entries, err := os.ReadDir(g.path("ci/bigboy"))
	if err != nil {
		g.failf("cannot list ci/bigboy: %v", err)
		return
	}
	var files []string
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "compose.bigboy") {
			files = append(files, e.Name())
		}
		if e.Name() == "stack.sh" || e.Name() == "stack.files" {
			g.failf("ci/bigboy/%s is back: the retired chain script", e.Name())
		}
	}
	g.expect(equal(files, []string{"compose.bigboy.yml"}), "ci/bigboy holds compose files %v, want only compose.bigboy.yml", files)
}

func composeNoScriptBuildsItsOwnChain(g *guard) {
	scripts, _ := filepath.Glob(g.path("ci/bigboy/*.sh"))
	sort.Strings(scripts)
	composeVar := regexp.MustCompile(`COMPOSE_FILE|\bCOMPOSE_PROFILES\b`)
	for _, script := range scripts {
		name := filepath.Base(script)
		for n, line := range strings.Split(g.read("ci/bigboy/"+name), "\n") {
			comment := strings.HasPrefix(strings.TrimLeft(line, " \t"), "#")
			code := line
			if comment {
				code, _, _ = strings.Cut(line, "#")
			}
			if composeVar.MatchString(code) && !strings.Contains(code, "grep '^COMPOSE_'") && !strings.Contains(code, "compgen") {
				g.failf("%s:%d sets its own compose chain variable: %s", name, n+1, strings.TrimSpace(line))
			}
			if strings.Contains(code, "docker compose") && !comment && strings.Contains(code, " -f ") && !strings.Contains(code, `compose.bigboy.yml"`) {
				g.failf("%s:%d names a compose file of its own: %s", name, n+1, strings.TrimSpace(line))
			}
		}
	}
}

func composeLiveStackIsTheOldChain(g *guard) {
	got := composeSorted(composeMust(g, composeIn{}).services)
	g.expect(equal(got, composeSortedStrs(composeStackServices)), "the default render runs %v, want the services of the old stack chain %v", got, composeStackServices)
}

func composeProfilesAddOnlyTheirOwn(g *guard) {
	base := composeMust(g, composeIn{})
	for _, c := range []struct {
		profile string
		want    []string
	}{
		{"smoke", []string{"web-smoke"}},
		{"venue", []string{"venue-prove", "venue-tools"}},
		{"venue-hook", []string{"venue-hook"}},
		{"venue-river", []string{"venue-river"}},
	} {
		got := composeAdded(base, composeMust(g, composeIn{profiles: []string{c.profile}}))
		g.expect(equal(got, c.want), "profile %s adds %v, want %v", c.profile, got, c.want)
	}
	every := composeMust(g, composeIn{profiles: composeAllOneOffs})
	for _, retired := range []string{"api", "billing-edge", "metrics-api"} {
		_, on := every.services[retired]
		g.expect(!on, "retired service %s is rendered by a default or one-off profile", retired)
	}
}

var composePinRe = regexp.MustCompile(`^ghcr\.io/full-chaos/[a-z-]+@sha256:[0-9a-f]{64}$`)

func composeDigestsInPinsOnly(g *guard) {
	for _, s := range composeScalars(g.yamlFile(composeFile)) {
		g.expect(!strings.Contains(s, "@sha256:"), "compose.bigboy.yml carries a digest (@sha256:) outside images.env: %.120s", s)
	}
	required := composeSet(composeRequired(g)...)
	pins := composePinsMap(g.read(composePins))
	delete(pins, "DHO_API_CH_USERS_XML")
	delete(pins, "ACR_CONTEXT_FABRIC_PROJECTION_ENABLED")
	var acr, own []string
	for _, name := range composeSorted(pins) {
		if strings.Contains(name, "_ACR_") {
			acr = append(acr, name)
		} else {
			own = append(own, name)
		}
	}
	wantACR := []string{"BIGBOY_ACR_API_IMAGE", "BIGBOY_ACR_FALKORDB_IMAGE", "BIGBOY_ACR_MCP_IMAGE", "BIGBOY_ACR_NGINX_IMAGE", "BIGBOY_ACR_POSTGRES_IMAGE"}
	g.expect(equal(acr, wantACR), "images.env acr pins are %v, want %v", acr, wantACR)
	wantOwn := []string{"BIGBOY_API_TOOLS_IMAGE", "BIGBOY_DHO_IMAGE", "BIGBOY_WEB_IMAGE"}
	g.expect(equal(own, wantOwn), "images.env pins are %v, want %v", own, wantOwn)
	for _, name := range append(acr, own...) {
		g.expect(required[name], "compose.bigboy.yml does not require (${%s:?) the pin %s", name, name)
	}
	for _, name := range own {
		g.expect(composePinRe.MatchString(pins[name]), "pin %s=%s is not a ghcr.io/full-chaos digest reference", name, pins[name])
	}
	r := composeMust(g, composeIn{profiles: composeAllOneOffs})
	for svc, pin := range map[string]string{"query-api": "BIGBOY_DHO_IMAGE", "web": "BIGBOY_WEB_IMAGE", "venue-river": "BIGBOY_DHO_IMAGE", "venue-prove": "BIGBOY_API_TOOLS_IMAGE"} {
		got := str(r.services[svc]["image"])
		g.expect(got == pins[pin], "%s renders image %q, want the %s pin %q", svc, got, pin, pins[pin])
	}
}

func composeOneOffNeedsNoOwnVariables(g *guard) {
	for _, p := range [][]string{nil, {"smoke"}, {"venue"}, {"venue-hook"}, {"venue-river"}, composeAllOneOffs} {
		composeMust(g, composeIn{profiles: p})
	}
	want := []string{"BIGBOY_ACR_API_IMAGE", "BIGBOY_ACR_FALKORDB_IMAGE", "BIGBOY_ACR_MCP_IMAGE", "BIGBOY_ACR_NGINX_IMAGE", "BIGBOY_ACR_POSTGRES_IMAGE",
		"BIGBOY_API_TOOLS_IMAGE", "BIGBOY_DHO_IMAGE", "BIGBOY_WEB_IMAGE", "DHO_API_CH_USERS_XML"}
	got := composeRequired(g)
	g.expect(equal(got, want), "compose.bigboy.yml requires %v, want only the pins and DHO_API_CH_USERS_XML %v", got, want)
}

func composeOneOffScriptsCheckTheirOwnVariables(g *guard) {
	smoke := g.read("ci/bigboy/web-path-smoke.sh")
	g.expect(strings.Contains(smoke, "resolves BLANK") && strings.Contains(smoke, "DHO_SMOKE_ADMIN_EMAIL"), "web-path-smoke.sh lost its BLANK check of DHO_SMOKE_ADMIN_EMAIL")
	prove := g.read("ci/bigboy/bigboy-graphql-prove.sh")
	g.expect(strings.Contains(prove, "JWT_SECRET_KEY has no value in ops/.env") && strings.Contains(prove, "exactly one org"), "bigboy-graphql-prove.sh lost its JWT_SECRET_KEY or exactly-one-org check")
}

func composeEachOldSetRenders(g *guard) {
	stack := composeSet(composeStackServices...)
	for _, c := range []struct {
		name     string
		profiles []string
		extra    []string
	}{
		{"stack", nil, nil},
		{"smoke", []string{"smoke"}, []string{"web-smoke"}},
		{"prove-base", []string{"venue"}, []string{"venue-prove", "venue-tools"}},
		{"prove", []string{"venue"}, []string{"venue-prove", "venue-tools"}},
		{"hook-check", []string{"venue", "venue-hook"}, []string{"venue-prove", "venue-tools", "venue-hook"}},
		{"river-apply", []string{"venue", "venue-river"}, []string{"venue-prove", "venue-tools", "venue-river"}},
	} {
		want := composeSet(c.extra...)
		for s := range stack {
			want[s] = true
		}
		got := composeSorted(composeMust(g, composeIn{profiles: c.profiles}).services)
		g.expect(equal(got, composeSorted(want)), "old set %s renders %v, want %v", c.name, got, composeSorted(want))
	}
}

func composeFreshRootRendersWithTrackedEnvFiles(g *guard) {
	for _, p := range [][]string{nil, composeAllOneOffs} {
		composeMust(g, composeIn{profiles: p, env: map[string]string{}})
	}
	images := strings.Join(composeRender(g, composeIn{env: map[string]string{}}).images, "\n")
	g.expect(strings.Contains(images, "dev-health-go-operator:not-resolved"), "a fresh root renders no worker with the placeholder operator tag")
	g.expect(!strings.Contains(images, "go-operator@sha256"), "a fresh root renders a worker from a go-operator@sha256 digest the tracked copy does not carry")
}

func composeQueryAPIDriftAwk(g *guard) {
	cut := g.read("ci/bigboy/bigboy-cut.sh")
	const open, close = "LIVE_NAMES=$(awk '", `' "$HERE/compose.bigboy.yml"`
	start := strings.Index(cut, open)
	if start < 0 {
		g.failf("bigboy-cut.sh has no LIVE_NAMES awk program")
		return
	}
	start += len(open)
	end := strings.Index(cut[start:], close)
	if end < 0 {
		g.failf("bigboy-cut.sh LIVE_NAMES awk does not read compose.bigboy.yml")
		return
	}
	out, err := exec.Command("awk", cut[start:start+end], g.path(composeFile)).Output()
	if err != nil {
		g.failf("the LIVE_NAMES awk program fails: %v", err)
		return
	}
	names := composeSet(strings.Fields(string(out))...)
	g.expect(names["GO_API_EDGE_JWT_SECRET"], "the LIVE_NAMES awk reads no GO_API_EDGE_JWT_SECRET from query-api's environment (read %d names)", len(names))
	flag := false
	for n := range names {
		flag = flag || strings.HasPrefix(n, "GO_API_") && strings.HasSuffix(n, "_ENABLED")
	}
	g.expect(flag, "the LIVE_NAMES awk reads no GO_API_*_ENABLED name from query-api's environment")
	for _, bad := range []string{"clickhouse", "migrate", "source", "target", "type"} {
		g.expect(!names[bad], "the LIVE_NAMES awk reads %q, which is not an environment name of query-api", bad)
	}
}

func composeHookAndRiverSelectVenueSet(g *guard) {
	for script, own := range map[string]string{"bigboy-hook-check.sh": "venue-hook", "bigboy-river-apply.sh": "venue-river"} {
		want := "--profile venue --profile " + own + " run --rm --no-deps -T " + own
		g.expect(strings.Contains(g.read("ci/bigboy/"+script), want), "%s does not run %q", script, want)
	}
}

var composeOneFileChecks = []check{
	{"one_compose_file_and_no_chain_script", composeOneFileNoChainScript},
	{"no_script_builds_its_own_file_chain", composeNoScriptBuildsItsOwnChain},
	{"the_live_stack_runs_exactly_the_services_of_the_old_chain", composeLiveStackIsTheOldChain},
	{"profiles_add_only_their_own_services", composeProfilesAddOnlyTheirOwn},
	{"the_digests_are_in_the_pins_file_only", composeDigestsInPinsOnly},
	{"a_one_off_needs_none_of_its_own_variables_to_render", composeOneOffNeedsNoOwnVariables},
	{"the_one_off_scripts_check_their_own_variables", composeOneOffScriptsCheckTheirOwnVariables},
	{"each_old_set_renders_the_services_it_rendered", composeEachOldSetRenders},
	{"a_fresh_root_renders_with_only_the_tracked_env_files", composeFreshRootRendersWithTrackedEnvFiles},
	{"the_query_api_drift_check_reads_the_environment_of_the_compose_file", composeQueryAPIDriftAwk},
	{"the_hook_and_river_scripts_select_the_old_venue_set", composeHookAndRiverSelectVenueSet},
}

func TestComposeOneFileReal(t *testing.T) { runReal(t, composeOneFileChecks...) }

var composeFiles = []string{composeFile, composePins}

const (
	composeOpNotResolved = "${BIGBOY_OPERATOR_IMAGE:-ghcr.io/full-chaos/dev-health-go-operator:not-resolved}"
	composeWebImage      = "'${BIGBOY_WEB_IMAGE:?BIGBOY_WEB_IMAGE is the image pin: pass --env-file _records/images.env}'"
)

func TestComposeOneFilePlanted(t *testing.T) {
	runPlanted(t, []planted{
		{name: "second compose file", copy: []string{"ci/bigboy"},
			mutate: func(t *testing.T, r string) { writeFile(t, r, "ci/bigboy/compose.bigboy.extra.yml", "services: {}\n") },
			check:  composeOneFileNoChainScript, want: "compose.bigboy.extra.yml"},
		{name: "stack.sh back", copy: []string{"ci/bigboy"},
			mutate: func(t *testing.T, r string) { writeFile(t, r, "ci/bigboy/stack.sh", "#!/usr/bin/env bash\n") },
			check:  composeOneFileNoChainScript, want: "stack.sh"},
		{name: "script names its own -f", copy: []string{"ci/bigboy"},
			mutate: mut("ci/bigboy/bigboy-hook-check.sh", `HC=("${DC[@]}"`, "docker compose -f compose.extra.yml ps\n"+`HC=("${DC[@]}"`),
			check:  composeNoScriptBuildsItsOwnChain, want: "compose.extra.yml"},
		{name: "script sets COMPOSE_FILE", copy: []string{"ci/bigboy"},
			mutate: mut("ci/bigboy/bigboy-river-apply.sh", `"${DC[@]}" --profile venue`, "export COMPOSE_FILE=a.yml:b.yml\n"+`"${DC[@]}" --profile venue`),
			check:  composeNoScriptBuildsItsOwnChain, want: "COMPOSE_FILE"},
		{name: "web-smoke leaves its profile", copy: composeFiles,
			mutate: mut(composeFile, "    profiles:\n    - smoke\n", ""),
			check:  composeLiveStackIsTheOldChain, want: "web-smoke"},
		{name: "venue-hook moves into venue", copy: composeFiles,
			mutate: mut(composeFile, "    - venue-hook\n", "    - venue\n"),
			check:  composeProfilesAddOnlyTheirOwn, want: "profile venue adds"},
		{name: "retired api back in the default render", copy: composeFiles,
			mutate: mut(composeFile, "    profiles:\n    - retired-python-api\n", ""),
			check:  composeProfilesAddOnlyTheirOwn, want: "retired service api"},
		{name: "digest literal in the compose file", copy: composeFiles,
			mutate: mut(composeFile, composeWebImage, "ghcr.io/full-chaos/dev-health-web@sha256:"+strings.Repeat("5", 64)),
			check:  composeDigestsInPinsOnly, want: "@sha256:"},
		{name: "web pin is a tag", copy: composeFiles,
			mutate: mut(composePins, "BIGBOY_WEB_IMAGE=ghcr.io/full-chaos/dev-health-web@", "BIGBOY_WEB_IMAGE=ghcr.io/full-chaos/dev-health-web:latest@"),
			check:  composeDigestsInPinsOnly, want: "pin BIGBOY_WEB_IMAGE"},
		{name: "a fourth image pin", copy: composeFiles,
			mutate: mut(composePins, "BIGBOY_API_TOOLS_IMAGE=", "BIGBOY_EXTRA_IMAGE=ghcr.io/full-chaos/x@sha256:"+strings.Repeat("6", 64)+"\nBIGBOY_API_TOOLS_IMAGE="),
			check:  composeDigestsInPinsOnly, want: "BIGBOY_EXTRA_IMAGE"},
		{name: "web renders the dho image", copy: composeFiles,
			mutate: mut(composeFile, composeWebImage, "'${BIGBOY_DHO_IMAGE:?pin}'"),
			check:  composeDigestsInPinsOnly, want: "web renders image"},
		{name: "a one-off requires its own variable", copy: composeFiles,
			mutate: mut(composeFile, "    - PROVE_ORG=${PROVE_ORG:-}", "    - PROVE_ORG=${PROVE_ORG:?set PROVE_ORG}"),
			check:  composeOneOffNeedsNoOwnVariables, want: "PROVE_ORG"},
		{name: "smoke drops its BLANK check", copy: []string{"ci/bigboy/web-path-smoke.sh", "ci/bigboy/bigboy-graphql-prove.sh"},
			mutate: mut("ci/bigboy/web-path-smoke.sh", "resolves BLANK", "resolves EMPTY"),
			check:  composeOneOffScriptsCheckTheirOwnVariables, want: "BLANK"},
		{name: "prove drops its one-org check", copy: []string{"ci/bigboy/web-path-smoke.sh", "ci/bigboy/bigboy-graphql-prove.sh"},
			mutate: mut("ci/bigboy/bigboy-graphql-prove.sh", "exactly one org", "one org or more"),
			check:  composeOneOffScriptsCheckTheirOwnVariables, want: "exactly-one-org"},
		{name: "venue-hook profile renamed", copy: composeFiles,
			mutate: mut(composeFile, "    - venue-hook\n", "    - venue-hookx\n"),
			check:  composeEachOldSetRenders, want: "old set hook-check"},
		{name: "operator image required", copy: composeFiles,
			mutate: mut(composeFile, composeOpNotResolved, "${BIGBOY_OPERATOR_IMAGE:?set BIGBOY_OPERATOR_IMAGE}"),
			check:  composeFreshRootRendersWithTrackedEnvFiles, want: "BIGBOY_OPERATOR_IMAGE"},
		{name: "users-file path missing from the env file", copy: composeFiles,
			mutate: mut(composePins, "DHO_API_CH_USERS_XML=", "#DHO_API_CH_USERS_XML="),
			check:  composeFreshRootRendersWithTrackedEnvFiles, want: "DHO_API_CH_USERS_XML"},
		{name: "operator default is a digest", copy: composeFiles,
			mutate: mut(composeFile, composeOpNotResolved, "${BIGBOY_OPERATOR_IMAGE:-ghcr.io/full-chaos/dev-health-go-operator@sha256:"+strings.Repeat("7", 64)+"}"),
			check:  composeFreshRootRendersWithTrackedEnvFiles, want: "go-operator@sha256"},
		{name: "awk reads the mapping form only", copy: []string{composeFile, "ci/bigboy/bigboy-cut.sh"},
			mutate: mut("ci/bigboy/bigboy-cut.sh", ` e&&/^    - [A-Za-z_][A-Za-z0-9_]*=/{n=$2; sub(/=.*/,"",n); print n}`, ""),
			check:  composeQueryAPIDriftAwk, want: "GO_API_EDGE_JWT_SECRET"},
		{name: "awk reads every field of query-api", copy: []string{composeFile, "ci/bigboy/bigboy-cut.sh"},
			mutate: mut("ci/bigboy/bigboy-cut.sh", `{e=($0 ~ /^    environment:/)}`, "{e=1}"),
			check:  composeQueryAPIDriftAwk, want: "not an environment name"},
		{name: "hook check drops the venue profile", copy: []string{"ci/bigboy/bigboy-hook-check.sh", "ci/bigboy/bigboy-river-apply.sh"},
			mutate: mut("ci/bigboy/bigboy-hook-check.sh", "--profile venue --profile venue-hook", "--profile venue-hook"),
			check:  composeHookAndRiverSelectVenueSet, want: "bigboy-hook-check.sh"},
	})
}

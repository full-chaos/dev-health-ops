package bigboy_test

import (
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

const (
	repinProve        = "ci/bigboy/bigboy-graphql-prove.sh"
	repinCompose      = "ci/bigboy/compose.bigboy.yml"
	repinProveSHA     = "0123456789abcdef0123456789abcdef01234567"
	repinProveOrg     = "11111111-1111-4111-8111-111111111111"
	repinProveSecret  = "fixture-not-a-secret"
	repinRoutedGoEdge = " -go-edge -edge-url http://traefik:3000/graphql "
	repinGoEdgeNote   = "through the routed /graphql in Go-edge mode,"
)

var (
	repinProveDigest = "sha256:" + strings.Repeat("ab", 32)
	repinProveIndex  = "sha256:" + strings.Repeat("cd", 32)
	repinProveEnvs   = []string{"PROVE_ORG", "JWT_SECRET_KEY", "GO_API_PROVE_PROOF_BEARER", "DEV_HEALTH_ENV", "GO_API_PROOF_ROUTE_ENABLED"}
)

type repinCall struct {
	argv []string
	env  map[string]string
}

func (c repinCall) has(arg string) bool {
	for _, a := range c.argv {
		if a == arg {
			return true
		}
	}
	return false
}

func (c repinCall) mentions(s string) bool {
	for _, a := range c.argv {
		if strings.Contains(a, s) {
			return true
		}
	}
	return false
}

type repinProveRun struct {
	scriptRun
	calls []repinCall
}

func (p repinProveRun) proves() []repinCall {
	var out []repinCall
	for _, c := range p.calls {
		if c.has("venue-prove") {
			out = append(out, c)
		}
	}
	return out
}

func repinProveDocker(log string) string {
	var env strings.Builder
	for _, v := range repinProveEnvs {
		fmt.Fprintf(&env, "printf '\\t%s=%%s' \"${%s-<unset>}\"; ", v, v)
	}
	return `{ printf 'CALL'; printf '\t%s' "$@"; printf '\n'; printf 'ENV'; ` + env.String() + `printf '\n'; } >> "` + log + `"
case "$1" in
  buildx)
    if [[ "$*" == *--raw* ]]; then
      echo '{"manifests":[{"digest":"` + repinProveDigest + `","platform":{"os":"linux","architecture":"arm64"}}]}'
    else
      echo '{"digest":"` + repinProveIndex + `"}'
    fi ;;
  compose)
    if [[ " $* " == *" exec "* ]]; then cat > /dev/null; echo ` + repinProveOrg + `; else echo 'stub compose run'; fi ;;
esac
`
}

func repinRunProve(g *guard, pin, jwt string, bashArgs ...string) repinProveRun {
	root := repinDir(g, "root")
	repinWrite(g, filepath.Join(root, "_records", "images.env"), "BIGBOY_API_TOOLS_IMAGE="+repinGHCR+"dev-health-go-api-tools@"+pin+"\n", 0o644)
	repinWrite(g, filepath.Join(root, ".remember/lanes/team-lead/reconciler-sweep-override.yml"), "services: {}\n", 0o644)
	repinWrite(g, filepath.Join(root, "ops", ".env"), "JWT_SECRET_KEY="+jwt+"\n", 0o644)
	log := filepath.Join(root, "docker-calls.log")
	repinWrite(g, log, "", 0o644)
	_, path := repinBin(g, map[string]string{"docker": repinProveDocker(log), "gh": "echo '{}'\n"})
	r := repinRun(g, map[string]string{"PATH": path, "BIGBOY_ROOT": root}, bashArgs...)
	out := repinProveRun{scriptRun: r}
	lines := strings.Split(strings.TrimSuffix(repinReadFile(g, log), "\n"), "\n")
	for i := 0; i+1 < len(lines); i += 2 {
		c := repinCall{argv: strings.Split(lines[i], "\t")[1:], env: map[string]string{}}
		for _, kv := range strings.Split(lines[i+1], "\t")[1:] {
			k, v, _ := strings.Cut(kv, "=")
			c.env[k] = v
		}
		out.calls = append(out.calls, c)
	}
	return out
}

func repinProveArgs(g *guard, args ...string) []string {
	return append([]string{g.path(repinProve)}, args...)
}

func repinProveCommand(g *guard, p repinProveRun) string {
	proves := p.proves()
	if len(proves) != 1 {
		g.failf("want exactly one venue-prove run, got %d (rc=%d): %s", len(proves), p.code, p.stderr)
		return ""
	}
	cmd := proves[0].argv[len(proves[0].argv)-1]
	g.expect(strings.Contains(cmd, "dho goapi prove ") && strings.Contains(cmd, "-candidate-build "+repinProveSHA),
		"the venue-prove command is not `dho goapi prove ... -candidate-build %s`: %q", repinProveSHA, cmd)
	return cmd
}

func checkRepinProveParses(g *guard) {
	r := repinRun(g, nil, "-n", g.path(repinProve))
	g.expect(r.code == 0, "bigboy-graphql-prove.sh does not parse: %s", r.stderr)
}

func checkRepinProvePosture(g *guard) {
	for _, args := range [][]string{{repinProveSHA}, {repinProveSHA, "--go-edge"}} {
		p := repinRunProve(g, repinProveDigest, repinProveSecret, repinProveArgs(g, args...)...)
		g.expect(p.code == 0, "harness %v exited %d: %s", args, p.code, p.stderr)
		for _, c := range p.calls {
			for i, a := range c.argv {
				g.expect(!(a == "up" && strings.Contains(strings.Join(c.argv[i:], " "), " -d")), "harness ran `up -d`: docker %q", c.argv)
			}
			for _, bad := range []string{"-proof-url", "-mode shadow", "DEV_HEALTH_ENV", "GO_API_PROOF_ROUTE_ENABLED"} {
				g.expect(!c.mentions(bad), "harness changed query-api posture: docker argv carries %q: %q", bad, c.argv)
			}
			for _, v := range []string{"DEV_HEALTH_ENV", "GO_API_PROOF_ROUTE_ENABLED"} {
				g.expect(c.env[v] == "<unset>", "harness changed query-api posture: docker ran with %s=%q", v, c.env[v])
			}
		}
	}
}

func checkRepinProveRefusesSkewNoRouting(g *guard) {
	p := repinRunProve(g, repinProveDigest, repinProveSecret, repinProveArgs(g, repinProveSHA)...)
	repinProveCommand(g, p)
	asked := false
	for _, c := range p.calls {
		asked = asked || c.has("--raw") && c.mentions("go-api-tools:sha-"+repinProveSHA[:7])
		g.expect(!c.mentions("dho goapi routing"), "harness ran a routing verb: %q", c.argv)
	}
	g.expect(asked, "harness never resolved go-api-tools:sha-%s to compare with the venue-prove image", repinProveSHA[:7])
	skew := repinRunProve(g, repinDigestOf('f'), repinProveSecret, repinProveArgs(g, repinProveSHA)...)
	g.expect(skew.code == 1, "a venue-prove image that is not go-api-tools:sha-<S7> exited %d, want 1", skew.code)
	g.expect(strings.Contains(skew.stderr, "REFUSED: venue-prove image"), "prover build skew was not refused by name: %q", skew.stderr)
	g.expect(len(skew.proves()) == 0, "harness proved with a skewed prover image")
}

func checkRepinProveCredentials(g *guard) {
	p := repinRunProve(g, repinProveDigest, repinProveSecret, repinProveArgs(g, repinProveSHA)...)
	repinProveCommand(g, p)
	g.expect(!strings.Contains(p.repinOut(), repinProveOrg), "the local org id reached the harness stdout or stderr")
	g.expect(!strings.Contains(p.repinOut(), repinProveSecret), "JWT_SECRET_KEY's value reached the harness stdout or stderr")
	for _, c := range p.calls {
		for _, bad := range []string{repinProveOrg, repinProveSecret, "GO_API_PROVE_PROOF_BEARER"} {
			g.expect(!c.mentions(bad), "a credential is on docker argv (%q): %q", bad, c.argv)
		}
		g.expect(c.env["JWT_SECRET_KEY"] == "<unset>", "the harness handed docker JWT_SECRET_KEY in its env (compose substitution must read it from ops/.env)")
		g.expect(c.env["GO_API_PROVE_PROOF_BEARER"] == "<unset>", "the harness handed docker GO_API_PROVE_PROOF_BEARER")
	}
	for _, c := range p.proves() {
		g.expect(c.env["PROVE_ORG"] == repinProveOrg, "venue-prove did not get the org by environment for compose substitution: PROVE_ORG=%q", c.env["PROVE_ORG"])
	}
	// xtrace prints every expanded word: the secret appears only if the script ever expands it into a variable or argv.
	traced := repinRunProve(g, repinProveDigest, repinProveSecret, append([]string{"-x"}, repinProveArgs(g, repinProveSHA)...)...)
	g.expect(!strings.Contains(traced.repinOut(), repinProveSecret), "the harness reads JWT_SECRET_KEY's value into the shell (seen in xtrace)")
	blank := repinRunProve(g, repinProveDigest, "", repinProveArgs(g, repinProveSHA)...)
	g.expect(blank.code == 1, "an empty JWT_SECRET_KEY in ops/.env exited %d, want 1", blank.code)
	g.expect(strings.Contains(blank.stderr, "JWT_SECRET_KEY has no value in ops/.env"), "an empty JWT_SECRET_KEY was not refused by name: %q", blank.stderr)
	g.expect(len(blank.proves()) == 0, "harness proved with an empty JWT_SECRET_KEY")
	env := strs(at(g.yamlFile(repinCompose), "services", "venue-prove", "environment"))
	for _, want := range []string{"JWT_SECRET_KEY=${JWT_SECRET_KEY:-}", "PROVE_ORG=${PROVE_ORG:-}"} {
		found := false
		for _, e := range env {
			found = found || e == want
		}
		g.expect(found, "compose venue-prove environment lacks %q (credentials by compose substitution only): %q", want, env)
	}
}

func repinCheckGoEdge(g *guard, args ...string) {
	p := repinRunProve(g, repinProveDigest, repinProveSecret, repinProveArgs(g, args...)...)
	g.expect(p.code == 0, "harness %v exited %d: %s", args, p.code, p.stderr)
	cmd := repinProveCommand(g, p)
	g.expect(strings.Contains(cmd, repinRoutedGoEdge), "harness %v did not hand the prover %q: %q", args, repinRoutedGoEdge, cmd)
	g.expect(!strings.Contains(cmd, "localhost:8000"), "harness %v points the prover at localhost:8000", args)
	g.expect(strings.Contains(cmd, repinGoEdgeNote), "harness %v review evidence does not say %q", args, repinGoEdgeNote)
}

func checkRepinProveDefaultEdge(g *guard) { repinCheckGoEdge(g, repinProveSHA) }

func checkRepinProveGoEdgeFlag(g *guard) { repinCheckGoEdge(g, repinProveSHA, "--go-edge") }

func checkRepinProveUnknownEdge(second string) func(*guard) {
	return func(g *guard) {
		p := repinRunProve(g, repinProveDigest, repinProveSecret, repinProveArgs(g, repinProveSHA, second)...)
		g.expect(p.code == 2, "edge argument %q exited %d, want 2 (usage)", second, p.code)
		g.expect(len(p.calls) == 0, "edge argument %q ran docker %d time(s) before refusing", second, len(p.calls))
	}
}

func checkRepinProveOrgReadAndProver(g *guard) {
	p := repinRunProve(g, repinProveDigest, repinProveSecret, repinProveArgs(g, repinProveSHA)...)
	g.expect(p.code == 0, "harness exited %d: %s", p.code, p.stderr)
	var execs, proves []repinCall
	for _, c := range p.calls {
		if c.has("exec") && c.has("postgres") {
			execs = append(execs, c)
		}
		if c.has("venue-prove") && c.has("run") {
			proves = append(proves, c)
		}
	}
	if len(execs) != 1 || len(proves) != 1 {
		g.failf("want one postgres exec and one venue-prove run, got %d and %d", len(execs), len(proves))
		return
	}
	for _, c := range []repinCall{execs[0], proves[0]} {
		onFile := false
		for _, a := range c.argv {
			onFile = onFile || strings.HasSuffix(a, "compose.bigboy.yml")
		}
		g.expect(onFile, "docker %q does not run on compose.bigboy.yml", c.argv)
		g.expect(!c.mentions("stack.sh"), "docker %q goes through stack.sh", c.argv)
		g.expect(c.has("venue"), "docker %q does not run under the venue profile", c.argv)
	}
}

func checkRepinProveArm64NotIndex(g *guard) {
	g.expect(repinProveDigest != repinProveIndex, "fixture: the arm64 child and the index digest must differ")
	p := repinRunProve(g, repinProveDigest, repinProveSecret, repinProveArgs(g, repinProveSHA)...)
	g.expect(p.code == 0, "a pin at the arm64 child exited %d, want 0: %s", p.code, p.stderr)
	repinProveCommand(g, p)
}

func checkRepinProveRefusesIndexPin(g *guard) {
	p := repinRunProve(g, repinProveIndex, repinProveSecret, repinProveArgs(g, repinProveSHA)...)
	g.expect(p.code == 1, "a pin at the multi-arch index digest exited %d, want 1", p.code)
	g.expect(len(p.proves()) == 0, "harness proved with a pin at the multi-arch index digest")
}

func TestRepinGraphQLProveHarness(t *testing.T) {
	checks := []check{
		{"script-parses", checkRepinProveParses},
		{"never-changes-query-api-posture", checkRepinProvePosture},
		{"refuses-prover-build-skew-and-runs-no-routing-verb", checkRepinProveRefusesSkewNoRouting},
		{"credentials-never-on-argv-or-stdout", checkRepinProveCredentials},
		{"default-edge-is-routed-graphql-go-edge", checkRepinProveDefaultEdge},
		{"go-edge-flag-is-routed-graphql-go-edge", checkRepinProveGoEdgeFlag},
		{"org-read-and-prover-on-the-one-compose-file", checkRepinProveOrgReadAndProver},
		{"prover-image-check-compares-arm64-child", checkRepinProveArm64NotIndex},
		{"prover-image-check-refuses-index-pin", checkRepinProveRefusesIndexPin},
	}
	for _, second := range []string{"--goedge", "-go-edge", "go-edge", "--go-edge=1"} {
		checks = append(checks, check{"unknown-edge-refused/" + second, checkRepinProveUnknownEdge(second)})
	}
	runReal(t, checks...)
}

func TestRepinGraphQLProveHarnessPlanted(t *testing.T) {
	c := []string{repinProve, repinHelper, repinCompose}
	indexWant := `want=$(docker buildx imagetools inspect "ghcr.io/full-chaos/dev-health-go-api-tools:sha-$S7" --format '{{json .Manifest}}' | jq -r .digest)`
	runPlanted(t, []planted{
		{"syntax-error", c, mut(repinProve, "\nfail=0\n", "\nfail=0; if then\n"), checkRepinProveParses, "does not parse"},
		{"posture-env-in-prove-command", c, mut(repinProve, `"GO_API_ENVELOPE_PRIVATE_KEY=`, `"DEV_HEALTH_ENV=production GO_API_ENVELOPE_PRIVATE_KEY=`),
			checkRepinProvePosture, `docker argv carries "DEV_HEALTH_ENV"`},
		{"posture-up-d", c, mut(repinProve, "st local-org 0; export", `"${DC[@]}" up -d query-api >/dev/null; st local-org 0; export`),
			checkRepinProvePosture, "harness ran `up -d`"},
		{"posture-exported-env", c, mut(repinProve, "st local-org 0; export", "export GO_API_PROOF_ROUTE_ENABLED=1; st local-org 0; export"),
			checkRepinProvePosture, "docker ran with GO_API_PROOF_ROUTE_ENABLED"},
		{"skew-not-refused", c, mut(repinProve, `if [ -n "$want" ] && [ "$want" = "$pinned" ]; then`, "if true; then"),
			checkRepinProveRefusesSkewNoRouting, "a venue-prove image that is not go-api-tools:sha-<S7> exited 0"},
		{"routing-verb", c, mut(repinProve, "dho goapi prove $U", "dho goapi routing enable; dho goapi prove $U"),
			checkRepinProveRefusesSkewNoRouting, "harness ran a routing verb"},
		{"org-echoed", c, mut(repinProve, "st local-org 0; export", `echo "org=$PROVE_ORG"; st local-org 0; export`),
			checkRepinProveCredentials, "the local org id reached the harness stdout"},
		{"org-on-argv", c, mut(repinProve, `-org \"\$PROVE_ORG\"`, `-org $PROVE_ORG`),
			checkRepinProveCredentials, "a credential is on docker argv"},
		{"secret-read-into-shell", c, mut(repinProve, `U="-registry-url`, `JWT=$(grep '^JWT_SECRET_KEY=' "$R/ops/.env"); U="-registry-url`),
			checkRepinProveCredentials, "reads JWT_SECRET_KEY's value into the shell"},
		{"empty-secret-accepted", c, mut(repinProve, `grep -q '^JWT_SECRET_KEY=.'`, `grep -q '^JWT_SECRET_KEY='`),
			checkRepinProveCredentials, "an empty JWT_SECRET_KEY in ops/.env exited 0"},
		{"compose-org-not-substituted", c, mut(repinCompose, "- PROVE_ORG=${PROVE_ORG:-}", "- PROVE_ORG"),
			checkRepinProveCredentials, `compose venue-prove environment lacks "PROVE_ORG=${PROVE_ORG:-}"`},
		{"default-edge-refused", c, mut(repinProve, `""|--go-edge)`, `--go-edge)`), checkRepinProveDefaultEdge, "exited 2"},
		{"go-edge-to-python-edge", c, mut(repinProve, `EDGE_ARGS="-go-edge -edge-url http://traefik:3000/graphql"`, `EDGE_ARGS="-edge-url http://localhost:8000/graphql"`),
			checkRepinProveGoEdgeFlag, "points the prover at localhost:8000"},
		{"misspelt-edge-accepted", c, mut(repinProve, `""|--go-edge)`, `""|--go-edge|--goedge)`), checkRepinProveUnknownEdge("--goedge"),
			`edge argument "--goedge" exited 0, want 2`},
		{"org-read-outside-venue-profile", c, mut(repinProve, "--profile venue exec", "exec"), checkRepinProveOrgReadAndProver,
			"does not run under the venue profile"},
		{"prover-compared-as-index", c, mut(repinProve, `want=$(arm64_child_digest "ghcr.io/full-chaos/dev-health-go-api-tools:sha-$S7")`, indexWant),
			checkRepinProveArm64NotIndex, "a pin at the arm64 child exited 1"},
		{"index-pin-accepted", c, mut(repinProve, `[ "$want" = "$pinned" ]`, `[ -n "$pinned" ]`), checkRepinProveRefusesIndexPin,
			"a pin at the multi-arch index digest exited 0"},
	})
}

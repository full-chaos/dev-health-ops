package chartgates_test

import (
	"fmt"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// Ported from tests/workers/test_helm_pinned_operator_image_helper.py (CHAOS-6958) and
// tests/workers/test_helm_migration_hook_chain.py (CHAOS-4428, CHAOS-6951, CHAOS-9369).

const (
	pinnedOperator = "ghcr.io/full-chaos/dev-health-go-operator@sha256:a6acfd0b8cc78d4d2fd4160c68ab3f8b69eb282b1c2433117af01c954f576534"
	pinnedDHO      = "ghcr.io/full-chaos/dev-health-go-dho:sha-aaaaaaaaaaaa"
	helpersTpl     = "templates/_helpers.tpl"
	riverHooksTpl  = "templates/river-hooks.yaml"
)

func hex64(c string) string { return strings.Repeat(c, 64) }

type hookSite struct {
	name string
	sets []string
	flag string
}

var hookSites = []hookSite{
	{"provision-roles", set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=false", "migrations.hook.routeActivate.enabled=false"), "migrations.hook.provisionRoles.image"},
	{"river-migrate", set("migrations.hook.provisionRoles.enabled=false", "migrations.hook.riverMigrate.enabled=true", "migrations.hook.routeActivate.enabled=false"), "migrations.hook.riverMigrate.image"},
}

func provisionOnly(flagImage string, extra ...string) []string {
	return join(hookSites[0].sets, set("migrations.hook.provisionRoles.image="+flagImage), set(extra...))
}

func pinHookRefusesPythonImageEvenWhenDigestPinned(g *guard) {
	for _, s := range hookSites {
		r := g.tpl("review", join(s.sets, set(s.flag+"=ghcr.io/full-chaos/dev-hops-api@sha256:"+hex64("a")))...)
		g.expect(r.code != 0, "%s: the Python image rendered clean", s.name)
		g.expect(r.code == 0 || strings.Contains(r.stderr, "is not a pinned dho image"), "%s: refusal lacks 'is not a pinned dho image': %s", s.name, firstLines(r.stderr, 2))
		g.expect(r.code == 0 || strings.Contains(r.stderr, "Python api image"), "%s: refusal does not name the Python api image: %s", s.name, firstLines(r.stderr, 2))
	}
}

func pinHookAcceptsPythonImageWhenOnlyTagPinned(g *guard) {
	for _, s := range hookSites {
		r := g.tpl("review", join(s.sets, set(s.flag+"=ghcr.io/full-chaos/dev-hops-api:sha-aaaaaaaaaaaa", "image.tag=sha-aaaaaaaaaaaa"))...)
		g.expect(r.code == 0, "%s: the disclosed tag-form boundary changed: %s", s.name, firstLines(r.stderr, 2))
	}
}

func pinMigrateRefusesRetiredImagesByName(g *guard) {
	for _, c := range []struct{ repo, hex string }{
		{"ghcr.io/full-chaos/dev-health-query-api", "a"}, {"ghcr.io/full-chaos/dev-health-go-worker", "a"},
		{"ghcr.io/full-chaos/dev-health-go-migrate", "b"}, {"ghcr.io/full-chaos/dev-health-go-api-tools", "b"},
		{"ghcr.io/full-chaos/dev-health-go-stream-runner", "b"}, {"ghcr.io/full-chaos/dev-health-go-reconciler", "b"},
		{"ghcr.io/full-chaos/dev-health-go-scheduler", "b"},
	} {
		r := g.tpl("review", set("migrations.hook.image="+c.repo+"@sha256:"+hex64(c.hex))...)
		g.expect(r.code != 0, "%s rendered clean", c.repo)
		g.expect(r.code == 0 || strings.Contains(r.stderr, "has no dho entrypoint"), "%s: the refusal lacks the denylist's own message 'has no dho entrypoint': %s", c.repo, firstLines(r.stderr, 2))
	}
}

func pinMigrateRefusesContractcheckUnderNamedAllowlist(g *guard) {
	r := g.tpl("review", set("migrations.hook.image=ghcr.io/full-chaos/dev-health-go-contractcheck@sha256:"+hex64("c"))...)
	g.expect(r.code != 0, "dev-health-go-contractcheck rendered: the named allowlist carries exactly two names")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "is not a pinned dho image"), "refusal lacks 'is not a pinned dho image': %s", firstLines(r.stderr, 2))
}

func pinAllowlistNamesAnchored(g *guard) {
	r := g.tpl("review", set("migrations.hook.image=ghcr.io/example/dev-health-go-dhotool@sha256:"+hex64("e"))...)
	g.expect(r.code != 0, "a look-alike name (dev-health-go-dhotool) rendered clean")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "is not a pinned dho image"), "refusal lacks 'is not a pinned dho image': %s", firstLines(r.stderr, 2))
}

func pinThirdPartyImageRefused(g *guard) {
	r := g.tpl("review", set("migrations.hook.image=postgres:18-alpine@sha256:"+hex64("9"))...)
	g.expect(r.code != 0, "a third-party image (postgres) rendered clean")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "is not a pinned dho image"), "refusal lacks 'is not a pinned dho image': %s", firstLines(r.stderr, 2))
}

func pinRouteActivateRefusesForeignOperatorName(g *guard) {
	r := g.tpl("review", set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true", "migrations.hook.routeActivate.image=ghcr.io/example/custom-operator@sha256:"+hex64("f"))...)
	g.expect(r.code != 0, "a digest-pinned foreign operator name rendered clean")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "is not a pinned dho image"), "refusal lacks 'is not a pinned dho image': %s", firstLines(r.stderr, 2))
}

func pinHookRefusesMalformedDigest(g *guard) {
	for _, s := range hookSites {
		r := g.tpl("review", join(s.sets, set(s.flag+"=ghcr.io/full-chaos/dev-health-go-dho@sha256:not-a-digest"))...)
		g.expect(r.code != 0, "%s: a malformed digest rendered clean", s.name)
		g.expect(r.code == 0 || strings.Contains(r.stderr, "is not a pinned dho image"), "%s: refusal lacks 'is not a pinned dho image'", s.name)
	}
}

func pinHookAcceptsWellFormedDigest(g *guard) {
	r := g.tpl("review", provisionOnly("ghcr.io/full-chaos/dev-health-go-dho@sha256:"+hex64("a"))...)
	g.expect(r.code == 0, "a real 64-hex digest must render: %s", firstLines(r.stderr, 2))
}

func pinHookRefusesShortHexDigest(g *guard) {
	r := g.tpl("review", provisionOnly("ghcr.io/full-chaos/dev-health-go-dho@sha256:"+strings.Repeat("a", 10))...)
	g.expect(r.code != 0, "a short (10 hex char) digest rendered clean")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "is not a pinned dho image"), "refusal lacks 'is not a pinned dho image'")
}

func pinHookRefusesLocalTagWithAlways(g *guard) {
	r := g.tpl("review", provisionOnly("ghcr.io/full-chaos/dev-health-go-dho:local", "migrations.hook.provisionRoles.pullPolicy=Always")...)
	g.expect(r.code != 0, "repo:local with pullPolicy=Always rendered clean")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "is not a pinned dho image"), "refusal lacks 'is not a pinned dho image'")
}

func pinPgbouncerRefusesMalformedDigest(g *guard) {
	r := g.tpl("review", set("goWorkers.enabled=true", "goWorkers.pgbouncer.enabled=true", "goWorkers.pgbouncer.image=edoburu/pgbouncer@sha256:not-a-digest")...)
	g.expect(r.code != 0, "a malformed pgbouncer digest rendered clean")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "must be pinned by an immutable sha256 digest"), "refusal lacks the digest-pin message: %s", firstLines(r.stderr, 2))
}

func pinPgbouncerAcceptsWellFormedDigest(g *guard) {
	r := g.tpl("review", set("goWorkers.enabled=true", "goWorkers.pgbouncer.enabled=true", "goWorkers.pgbouncer.image=edoburu/pgbouncer@sha256:"+hex64("b"))...)
	g.expect(r.code == 0, "a well-formed pgbouncer digest must render: %s", firstLines(r.stderr, 2))
}

func hookContainerPolicy(docs []doc, container string) (string, bool) {
	for _, d := range ofKind(docs, "Job") {
		for _, c := range containers(d) {
			if str(c["name"]) == container {
				return str(c["imagePullPolicy"]), true
			}
		}
	}
	return "", false
}

func pinHookPullPolicyNeverLeaks(g *guard) {
	for _, s := range hookSites {
		docs := g.docs("review", join(s.sets, set(s.flag+"="+pinnedDHO, "image.pullPolicy=Never"), []string{"--show-only", riverHooksTpl})...)
		got, ok := hookContainerPolicy(docs, s.name)
		g.expect(ok, "%s: the hook container was not rendered", s.name)
		g.expect(got == "IfNotPresent", "%s: the hook container imagePullPolicy is %q, want IfNotPresent, not the application image's Never", s.name, got)
	}
}

func pinHookPullPolicyOverridable(g *guard) {
	docs := g.docs("review", join(hookSites[0].sets, set("migrations.hook.provisionRoles.image="+pinnedDHO, "migrations.hook.provisionRoles.pullPolicy=Always", "image.pullPolicy=Never"), []string{"--show-only", riverHooksTpl})...)
	got, _ := hookContainerPolicy(docs, "provision-roles")
	g.expect(got == "Always", "the per-hook pullPolicy override did not take effect: imagePullPolicy is %q", got)
}

// --- migration chain ---

const (
	hookChain       = "hook-chain"
	chainMigrate    = hookChain + "-dev-health-migrate"
	chainProvision  = hookChain + "-dev-health-provision-roles"
	chainRiver      = hookChain + "-dev-health-river-migrate"
	chainProvConn   = chainProvision + "-conn"
	chainMigSecrets = hookChain + "-dev-health-migrate-secrets"
	chainRivSecrets = hookChain + "-dev-health-river-migrate-secrets"
	migrationDSN    = "postgresql://migrator:pw@postgres:5432/devhealth"
)

var bothOn = set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true", "migrations.hook.routeActivate.image="+pinnedOperator)

func jobsOf(docs []doc) map[string]doc {
	out := map[string]doc{}
	for _, d := range ofKind(docs, "Job") {
		out[name(d)] = d
	}
	return out
}

func secretsOf(docs []doc) map[string]doc {
	out := map[string]doc{}
	for _, d := range ofKind(docs, "Secret") {
		out[name(d)] = d
	}
	return out
}

func chainJobs(g *guard, sets ...string) map[string]doc { return jobsOf(g.docs(hookChain, sets...)) }

func ann(d doc, k string) string { return str(at(d, "metadata", "annotations", k)) }

func firstContainer(d doc) doc {
	if cs := containers(d); len(cs) > 0 {
		return cs[0]
	}
	return doc{}
}

func chainValues(riverMigrate bool) string {
	return fmt.Sprintf(`
migrations:
  hook:
    provisionRoles: {enabled: true}
    riverMigrate: {enabled: %t}
    routeActivate:
      image: "%s"
    secretData:
      MIGRATION_DATABASE_URI: "%s"
      POSTGRES_URI: ""
      CLICKHOUSE_URI: ""
`, riverMigrate, pinnedOperator, migrationDSN)
}

func chainDocsFromValues(g *guard, values string) []doc {
	return g.docs(hookChain, "-f", g.valuesFile(values))
}

func chainHooksOffByDefault(g *guard) {
	jobs := chainJobs(g)
	_, p := jobs[chainProvision]
	_, r := jobs[chainRiver]
	g.expect(!p && !r, "an existing deployment must not acquire two new Jobs by upgrading (provision=%v river=%v)", p, r)
	_, m := jobs[chainMigrate]
	g.expect(m, "the pre-existing migrate hook must still render")
}

func weight(d doc) int {
	var n int
	fmt.Sscanf(ann(d, "helm.sh/hook-weight"), "%d", &n)
	return n
}

func chainRendersInWeightOrder(g *guard) {
	jobs := chainJobs(g, bothOn...)
	for _, n := range []string{chainMigrate, chainProvision, chainRiver} {
		g.expect(jobs[n] != nil, "%s did not render", n)
	}
	if jobs[chainMigrate] == nil || jobs[chainProvision] == nil || jobs[chainRiver] == nil {
		return
	}
	g.expect(weight(jobs[chainMigrate]) < weight(jobs[chainProvision]) && weight(jobs[chainProvision]) < weight(jobs[chainRiver]),
		"River's preflight needs the logins provisioning creates, and provisioning needs the tables Alembic creates: weights %d, %d, %d", weight(jobs[chainMigrate]), weight(jobs[chainProvision]), weight(jobs[chainRiver]))
}

func chainMigrateWeightUnchanged(g *guard) {
	jobs := chainJobs(g, bothOn...)
	g.expect(jobs[chainMigrate] != nil && ann(jobs[chainMigrate], "helm.sh/hook-weight") == "0", "the migrate Job keeps weight 0; the new hooks bracket it (got %q)", ann(jobs[chainMigrate], "helm.sh/hook-weight"))
}

func chainNewHooksRunOnInstallAndUpgrade(g *guard) {
	jobs := chainJobs(g, bothOn...)
	for _, n := range []string{chainProvision, chainRiver} {
		ev := strings.Split(ann(jobs[n], "helm.sh/hook"), ",")
		g.expect(sortedHas(ev, "pre-install"), "%s must run on a fresh install", n)
		g.expect(sortedHas(ev, "pre-upgrade"), "%s must also run on upgrade: riverMigrate re-applies the full grant posture on every run", n)
	}
}

func chainFailedPodsRetained(g *guard) {
	jobs := chainJobs(g, bothOn...)
	for _, n := range []string{chainProvision, chainRiver} {
		pol := ann(jobs[n], "helm.sh/hook-delete-policy")
		g.expect(strings.Contains(pol, "before-hook-creation"), "%s delete policy %q lacks before-hook-creation", n, pol)
		g.expect(!strings.Contains(pol, "hook-succeeded"), "%s would delete its own evidence on the run that needs it least", n)
	}
}

func chainProvisioningRunsDhoMigrateRoles(g *guard) {
	jobs := chainJobs(g, bothOn...)
	job := jobs[chainProvision]
	if job == nil {
		g.failf("the provision-roles Job did not render")
		return
	}
	cs := containers(job)
	g.expect(len(cs) == 1, "want one container, got %d", len(cs))
	if len(cs) == 1 {
		g.expect(str(cs[0]["image"]) == pinnedOperator, "image %q, want the pinned operator image", str(cs[0]["image"]))
		_, cmd := cs[0]["command"]
		g.expect(!cmd, "the hook sets a command: the image is distroless, args only")
		g.expect(equal(cs[0]["args"], any(anyStrs("migrate", "roles"))), "args %v, want [migrate roles]", cs[0]["args"])
	}
	g.expect(len(initContainers(job)) == 0, "the hook has initContainers")
	rendered := serialized(job)
	for _, retired := range []string{"psql", "provision_river_roles", "PGPASSWORD", "APP_DATABASE", "dev-hops-api", "/bin/sh"} {
		g.expect(!strings.Contains(rendered, retired), "the hook still carries %q", retired)
	}
}

func chainProvisioningImagePreference(g *guard) {
	own := "ghcr.io/full-chaos/dev-health-go-dho@sha256:" + hex64("1")
	river := "ghcr.io/full-chaos/dev-health-go-dho@sha256:" + hex64("2")
	jobs := chainJobs(g, join(bothOn, set("migrations.hook.riverMigrate.image="+river))...)
	g.expect(str(firstContainer(jobs[chainProvision])["image"]) == river, "provision-roles must default to the River image")
	jobs = chainJobs(g, join(bothOn, set("migrations.hook.riverMigrate.image="+river, "migrations.hook.provisionRoles.image="+own))...)
	g.expect(str(firstContainer(jobs[chainProvision])["image"]) == own, "provision-roles' own image must win")
	g.expect(str(firstContainer(jobs[chainRiver])["image"]) == river, "the River hook keeps its own image")
}

func chainProvisioningRefusesUnpinnedImage(g *guard) {
	for _, image := range []string{"", "ghcr.io/full-chaos/dev-health-go-operator:latest", "ghcr.io/full-chaos/dev-hops-api:0.1.0"} {
		r := g.tpl(hookChain, set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=false", "migrations.hook.routeActivate.enabled=false", "migrations.hook.provisionRoles.image="+image)...)
		g.expect(r.code != 0, "an unpinned provision-roles hook image %q rendered", image)
		g.expect(r.code == 0 || (strings.Contains(r.stderr, "the provision-roles hook image") && strings.Contains(r.stderr, "not a pinned dho image")), "refusal of %q lacks the hook label and 'not a pinned dho image': %s", image, firstLines(r.stderr, 2))
	}
}

func chainProvisioningRefusesLockstepMismatch(g *guard) {
	r := g.tpl(hookChain, set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=false", "migrations.hook.routeActivate.enabled=false",
		"migrations.hook.provisionRoles.image=ghcr.io/full-chaos/dev-health-go-dho:sha-aaaaaaaaaaaa", "queryApi.enabled=true", "queryApi.image.tag=sha-bbbbbbbbbbbb")...)
	g.expect(r.code != 0, "a provision-roles image pinned to another commit rendered")
	for _, want := range []string{"pinned to different commits", "queryApi.image", "the provision-roles hook image"} {
		g.expect(r.code == 0 || strings.Contains(r.stderr, want), "refusal lacks %q: %s", want, firstLines(r.stderr, 2))
	}
}

func chainProvisioningAcceptsLockstepAndLocal(g *guard) {
	jobs := chainJobs(g, set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.routeActivate.enabled=false",
		"migrations.hook.provisionRoles.image=ghcr.io/full-chaos/dev-health-go-dho:sha-cccccccccccc", "queryApi.enabled=true", "queryApi.image.tag=sha-cccccccccccc")...)
	g.expect(strings.HasSuffix(str(firstContainer(jobs[chainProvision])["image"]), ":sha-cccccccccccc"), "a lockstep match must render")
	jobs = chainJobs(g, set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.routeActivate.enabled=false",
		"migrations.hook.provisionRoles.image=dho:local", "migrations.hook.provisionRoles.pullPolicy=Never")...)
	c := firstContainer(jobs[chainProvision])
	g.expect(str(c["image"]) == "dho:local" && str(c["imagePullPolicy"]) == "Never", "a sideloaded dho:local image with pullPolicy Never must render: %v / %v", c["image"], c["imagePullPolicy"])
}

func chainRiverDefaultsToPinnedOperator(g *guard) {
	jobs := chainJobs(g, bothOn...)
	g.expect(str(firstContainer(jobs[chainRiver])["image"]) == pinnedOperator, "the River hook image is %q, want the pinned operator image", str(firstContainer(jobs[chainRiver])["image"]))
}

func chainRiverRefusesUnpinnedImage(g *guard) {
	for _, image := range []string{"", "ghcr.io/full-chaos/dev-health-go-operator:latest", "ghcr.io/full-chaos/dev-health-go-dho:0.1.0"} {
		r := g.tpl(hookChain, set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true", "migrations.hook.routeActivate.enabled=false", "migrations.hook.riverMigrate.image="+image)...)
		g.expect(r.code != 0, "an unpinned River hook image %q rendered", image)
		g.expect(r.code == 0 || strings.Contains(r.stderr, "is not a pinned dho image"), "refusal of %q lacks 'is not a pinned dho image'", image)
	}
}

func chainRiverTakesItsOwnPinnedImage(g *guard) {
	own := "ghcr.io/full-chaos/dev-health-go-dho@sha256:" + hex64("b")
	jobs := chainJobs(g, join(bothOn, set("migrations.hook.riverMigrate.image="+own))...)
	g.expect(str(firstContainer(jobs[chainRiver])["image"]) == own, "riverMigrate.image must win over the operator image")
}

func chainRiverRefusesLockstepMismatch(g *guard) {
	r := g.tpl(hookChain, set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true", "migrations.hook.routeActivate.enabled=false",
		"migrations.hook.riverMigrate.image=ghcr.io/full-chaos/dev-health-go-dho:sha-aaaaaaaaaaaa", "queryApi.enabled=true", "queryApi.image.tag=sha-bbbbbbbbbbbb")...)
	g.expect(r.code != 0, "a River hook image pinned to a different commit than image.tag rendered")
	for _, want := range []string{"pinned to different commits", "sha-aaaaaaaaaaaa", "sha-bbbbbbbbbbbb"} {
		g.expect(r.code == 0 || strings.Contains(r.stderr, want), "refusal lacks %q", want)
	}
}

func chainRiverAcceptsLockstepMatch(g *guard) {
	jobs := chainJobs(g, set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true", "migrations.hook.routeActivate.enabled=false",
		"migrations.hook.riverMigrate.image=ghcr.io/full-chaos/dev-health-go-dho:sha-cccccccccccc", "queryApi.enabled=true", "queryApi.image.tag=sha-cccccccccccc")...)
	g.expect(str(firstContainer(jobs[chainRiver])["image"]) == "ghcr.io/full-chaos/dev-health-go-dho:sha-cccccccccccc", "the same commit on both sides must render")
}

func chainRiverDigestPinIsNeverCompared(g *guard) {
	own := "ghcr.io/full-chaos/dev-health-go-dho@sha256:" + hex64("d")
	jobs := chainJobs(g, set("migrations.hook.provisionRoles.enabled=true", "migrations.hook.riverMigrate.enabled=true", "migrations.hook.routeActivate.enabled=false",
		"migrations.hook.riverMigrate.image="+own, "image.repository=ghcr.io/full-chaos/dev-hops-api", "image.tag=sha-eeeeeeeeeeee", "queryApi.enabled=true", "queryApi.image.tag=sha-eeeeeeeeeeee")...)
	g.expect(str(firstContainer(jobs[chainRiver])["image"]) == own, "a digest pin encodes no commit and is never compared with the api image (a digest pin was compared and refused)")
}

func chainRiverAppliesWithNoShell(g *guard) {
	c := firstContainer(chainJobs(g, bothOn...)[chainRiver])
	_, cmd := c["command"]
	g.expect(!cmd, "the River hook sets a command: the dho image is distroless")
	g.expect(equal(c["args"], any(anyStrs("migrate", "river", "--apply-and-check"))), "args %v, want [migrate river --apply-and-check]", c["args"])
}

func chainRolePasswordsNeverInManifest(g *guard) {
	jobs := chainJobs(g, join(bothOn, set("goWorkers.pgbouncer.secret.data.RIVER_DOMAIN_DATABASE_PASSWORD=s3cret-domain"))...)
	g.expect(!strings.Contains(serialized(jobs[chainProvision]), "s3cret-domain"), "the provisioning Job embeds a role password in its own manifest")
	env := envMap(firstContainer(jobs[chainProvision]))
	g.expect(at(env["RIVER_DOMAIN_DATABASE_PASSWORD"], "valueFrom", "secretKeyRef") != nil, "RIVER_DOMAIN_DATABASE_PASSWORD must come from a secretKeyRef")
}

func chainPasswordSecretAvailableToHook(g *guard) {
	docs := chainDocsFromValues(g, chainValues(true))
	jobs, secrets := jobsOf(docs), secretsOf(docs)
	env := envMap(firstContainer(jobs[chainProvision]))
	ref := str(at(env["RIVER_DOMAIN_DATABASE_PASSWORD"], "valueFrom", "secretKeyRef", "name"))
	sec := secrets[ref]
	g.expect(sec != nil, "the provisioning Job references Secret %q, which the chart does not render at all", ref)
	if sec == nil {
		return
	}
	for _, k := range []string{"RIVER_DOMAIN_DATABASE_PASSWORD", "RIVER_QUEUE_DATABASE_PASSWORD", "RIVER_COORDINATOR_DATABASE_PASSWORD"} {
		_, ok := mp(sec["stringData"])[k]
		g.expect(ok, "%s omits %s", ref, k)
	}
	g.expect(sortedHas(strings.Split(ann(sec, "helm.sh/hook"), ","), "pre-install"), "%s is a regular resource, which Helm creates only AFTER the pre-install hooks: the weight-5 Job would start against a Secret that does not exist yet", ref)
	g.expect(weight(sec) < 5, "%s must be created before the Job that mounts it: weight %d", ref, weight(sec))
}

func chainAcceptsPreCreatedExternalSecret(g *guard) {
	docs := chainDocsFromValues(g, chainValues(true)+"\ngoWorkers:\n  pgbouncer:\n    secret:\n      create: false\n      externalSecretName: river-role-credentials\n")
	env := envMap(firstContainer(jobsOf(docs)[chainProvision]))
	for _, k := range []string{"RIVER_DOMAIN_DATABASE_PASSWORD", "RIVER_QUEUE_DATABASE_PASSWORD", "RIVER_COORDINATOR_DATABASE_PASSWORD"} {
		g.expect(str(at(env[k], "valueFrom", "secretKeyRef", "name")) == "river-role-credentials", "%s does not read the operator's own Secret", k)
	}
}

func chainWithoutAnyPasswordSecretFails(g *guard) {
	r := g.tpl(hookChain, "-f", g.valuesFile(chainValues(true)+"\ngoWorkers:\n  enabled: false\n  pgbouncer:\n    secret:\n      create: false\n      externalSecretName: \"\"\n"))
	g.expect(r.code != 0, "with neither Secret the render must refuse to guess")
	g.expect(r.code == 0 || strings.Contains(r.stderr, "RIVER_"), "the error must name what the Secret has to carry: %s", firstLines(r.stderr, 2))
}

func envFromSecretNames(d doc) []string {
	var out []string
	for _, i := range list(firstContainer(d)["envFrom"]) {
		if n := str(at(i, "secretRef", "name")); n != "" {
			out = append(out, n)
		}
	}
	return out
}

func chainProvisioningReadsItsOwnHookSecret(g *guard) {
	docs := chainDocsFromValues(g, chainValues(true))
	jobs, secrets := jobsOf(docs), secretsOf(docs)
	g.expect(equal(envFromSecretNames(jobs[chainProvision]), []string{chainProvConn}), "provision-roles reads secrets %v, want only its own %s", envFromSecretNames(jobs[chainProvision]), chainProvConn)
	g.expect(secrets[chainProvConn] != nil && equal(mp(secrets[chainProvConn]["stringData"]), doc{"MIGRATION_DATABASE_URI": migrationDSN}), "the hook Secret must carry only MIGRATION_DATABASE_URI")
	if s := secrets[chainProvConn]; s != nil {
		g.expect(sortedHas(strings.Split(ann(s, "helm.sh/hook"), ","), "pre-install") && weight(s) < 5, "the DSN Secret must be a pre-install hook resource created before the weight-5 Job")
	}
}

func chainDSNSecretHasNoPasswordKey(g *guard) {
	s := secretsOf(chainDocsFromValues(g, chainValues(true)))[chainProvConn]
	if s == nil {
		g.failf("no %s Secret", chainProvConn)
		return
	}
	for k := range mp(s["stringData"]) {
		g.expect(!strings.Contains(k, "PASSWORD"), "the DSN Secret carries a password key %s", k)
	}
}

func chainProvisioningKeepsDSNAliases(g *guard) {
	for _, alias := range []string{"POSTGRES_URI", "DATABASE_URI"} {
		values := fmt.Sprintf(`
migrations:
  hook:
    provisionRoles: {enabled: true}
    routeActivate:
      image: "%s"
    secretData:
      MIGRATION_DATABASE_URI: ""
      POSTGRES_URI: ""
      DATABASE_URI: ""
      CLICKHOUSE_URI: ""
      %s: "postgresql+asyncpg://postgres:pw@db:5432/devhealth"
`, pinnedOperator, alias)
		s := secretsOf(chainDocsFromValues(g, values))[chainProvConn]
		g.expect(s != nil && str(at(s, "stringData", "MIGRATION_DATABASE_URI")) == "postgresql+asyncpg://postgres:pw@db:5432/devhealth", "an alias-only install (%s) must still hand the verb a DSN", alias)
	}
}

func chainProvisioningReadsOperatorSecretDirectly(g *guard) {
	values := fmt.Sprintf(`
secrets:
  create: false
  externalSecretName: operator-app-secret
migrations:
  hook:
    externalSecretName: operator-migration-secret
    provisionRoles: {enabled: true}
    routeActivate:
      image: "%s"
`, pinnedOperator)
	docs := chainDocsFromValues(g, values)
	g.expect(equal(envFromSecretNames(jobsOf(docs)[chainProvision]), []string{"operator-migration-secret"}), "the Job must read the operator's Secret, got %v", envFromSecretNames(jobsOf(docs)[chainProvision]))
	g.expect(secretsOf(docs)[chainProvConn] == nil, "no DSN Secret may be rendered when the operator owns it")
}

func chainWithoutDSNRendersEmptySecret(g *guard) {
	values := fmt.Sprintf(`
migrations:
  hook:
    provisionRoles: {enabled: true}
    routeActivate:
      image: "%s"
    secretData:
      MIGRATION_DATABASE_URI: ""
      POSTGRES_URI: ""
      DATABASE_URI: ""
      CLICKHOUSE_URI: ""
`, pinnedOperator)
	s := secretsOf(chainDocsFromValues(g, values))[chainProvConn]
	g.expect(s != nil, "the hook Secret must still render")
	if s != nil {
		_, has := s["stringData"]
		g.expect(!has, "no DSN must never become a local-socket attempt: the Secret must carry no connection key")
	}
}

var kedaEnv = []string{"RIVER_KEDA_READONLY_DATABASE_ROLE", "RIVER_DATABASE_SCHEMA", "RIVER_KEDA_READONLY_PASSWORD"}

func chainProvisioningEnvIsExactlyTheVerbs(g *guard) {
	env := envMap(firstContainer(jobsOf(chainDocsFromValues(g, chainValues(true)))[chainProvision]))
	var names, kedaPresent []string
	for n := range env {
		if sortedHas(kedaEnv, n) {
			kedaPresent = append(kedaPresent, n)
		} else {
			names = append(names, n)
		}
	}
	g.expect(sameSet(names, []string{"RIVER_DOMAIN_DATABASE_ROLE", "RIVER_QUEUE_DATABASE_ROLE", "RIVER_COORDINATOR_DATABASE_ROLE", "RIVER_DOMAIN_DATABASE_PASSWORD", "RIVER_QUEUE_DATABASE_PASSWORD", "RIVER_COORDINATOR_DATABASE_PASSWORD"}), "the provisioning env is %v", names)
	g.expect(len(kedaPresent) == 0 || len(kedaPresent) == len(kedaEnv), "the KEDA trio must come together: %v", kedaPresent)
	for n, e := range env {
		if strings.HasSuffix(n, "_PASSWORD") {
			_, v := e["value"]
			g.expect(at(e, "valueFrom", "secretKeyRef") != nil && !v, "%s must come from a secretKeyRef, never a value", n)
		}
	}
}

var monitorEnv = []string{"OTEL_PG_MONITOR_DATABASE_ROLE", "OTEL_PG_MONITOR_DATABASE_PASSWORD"}

func monitorValues(monitor map[string]any) string {
	var base map[string]any
	_ = yaml.Unmarshal([]byte(chainValues(true)), &base)
	mp(at(base, "migrations", "hook", "provisionRoles"))["monitorRole"] = monitor
	b, _ := yaml.Marshal(base)
	return string(b)
}

func provisionEnv(g *guard, values string) (map[string]doc, []doc) {
	docs := chainDocsFromValues(g, values)
	return envMap(firstContainer(jobsOf(docs)[chainProvision])), docs
}

func chainMonitorLoginOffByDefault(g *guard) {
	env, _ := provisionEnv(g, chainValues(true))
	for _, k := range monitorEnv {
		g.expect(!has(env, k), "the monitor login env %s is present by default", k)
	}
}

func chainMonitorEnvFromSeparateSecret(g *guard) {
	env, docs := provisionEnv(g, monitorValues(map[string]any{"name": "otel_pg_monitor"}))
	g.expect(str(env["OTEL_PG_MONITOR_DATABASE_ROLE"]["value"]) == "otel_pg_monitor", "monitor role env is %q", str(env["OTEL_PG_MONITOR_DATABASE_ROLE"]["value"]))
	ref := mp(at(env["OTEL_PG_MONITOR_DATABASE_PASSWORD"], "valueFrom", "secretKeyRef"))
	g.expect(equal(ref, doc{"name": "otel-collector-postgres", "key": "POSTGRES_MONITOR_PASSWORD"}), "monitor password ref is %v", ref)
	_, v := env["OTEL_PG_MONITOR_DATABASE_PASSWORD"]["value"]
	g.expect(!v, "the monitor password must never be a value")
	secrets := secretsOf(docs)
	g.expect(secrets["otel-collector-postgres"] == nil, "the chart must never create the otel-collector-postgres Secret")
	for n, s := range secrets {
		_, leak := mp(s["stringData"])["POSTGRES_MONITOR_PASSWORD"]
		g.expect(!leak, "Secret %s carries POSTGRES_MONITOR_PASSWORD", n)
	}
}

func chainMonitorSecretNameAndKeyAreValues(g *guard) {
	env, _ := provisionEnv(g, monitorValues(map[string]any{"name": "m", "secretName": "other-secret", "secretKey": "PW"}))
	g.expect(equal(mp(at(env["OTEL_PG_MONITOR_DATABASE_PASSWORD"], "valueFrom", "secretKeyRef")), doc{"name": "other-secret", "key": "PW"}), "the monitor secretName/secretKey values were not honoured")
}

func chainMonitorWithoutItsSecretFails(g *guard) {
	for _, missing := range []string{"secretName", "secretKey"} {
		m := map[string]any{"name": "otel_pg_monitor", "secretName": "s", "secretKey": "k"}
		m[missing] = ""
		r := g.tpl(hookChain, "-f", g.valuesFile(monitorValues(m)))
		g.expect(r.code != 0, "a monitor login with an empty %s rendered", missing)
		g.expect(r.code == 0 || strings.Contains(r.stderr, "monitorRole."+missing), "the refusal does not name monitorRole.%s", missing)
	}
}

func chainMonitorWithoutNameStaysOff(g *guard) {
	env, _ := provisionEnv(g, monitorValues(map[string]any{"name": "", "secretName": ""}))
	for _, k := range monitorEnv {
		g.expect(!has(env, k), "%s present although the monitor login has no name", k)
	}
}

func chainWeight0SecretOmitsElevatedDSN(g *guard) {
	data := mp(secretsOf(chainDocsFromValues(g, chainValues(true)))[chainMigSecrets]["stringData"])
	_, has := data["MIGRATION_DATABASE_URI"]
	g.expect(!has, "the weight-0 Job envFroms this Secret: the key reaching it makes `migrate postgres` run the River step before the roles exist")
	g.expect(str(data["POSTGRES_URI"]) == migrationDSN, "Alembic still needs the DSN under POSTGRES_URI, got %q", str(data["POSTGRES_URI"]))
}

func chainWeight10SecretCarriesElevatedDSN(g *guard) {
	docs := chainDocsFromValues(g, chainValues(true))
	secrets := secretsOf(docs)
	g.expect(secrets[chainRivSecrets] != nil, "no dedicated Secret for the River hook")
	g.expect(str(at(secrets[chainRivSecrets], "stringData", "MIGRATION_DATABASE_URI")) == migrationDSN, "the River Secret lacks the elevated DSN")
	g.expect(sortedHas(envFromSecretNames(jobsOf(docs)[chainRiver]), chainRivSecrets), "the River Job does not read its own Secret")
}

func chainRiverSecretCreatedBeforeJob(g *guard) {
	s := secretsOf(chainDocsFromValues(g, chainValues(true)))[chainRivSecrets]
	g.expect(s != nil && sortedHas(strings.Split(ann(s, "helm.sh/hook"), ","), "pre-install") && weight(s) < 10, "the River Secret must be a pre-install hook resource with weight below 10")
}

func chainWeight0SecretUnchangedWhenRiverHookOff(g *guard) {
	docs := chainDocsFromValues(g, chainValues(false))
	secrets := secretsOf(docs)
	g.expect(str(at(secrets[chainMigSecrets], "stringData", "MIGRATION_DATABASE_URI")) == migrationDSN, "with the River hook off the weight-0 Secret must keep MIGRATION_DATABASE_URI")
	g.expect(secrets[chainRivSecrets] == nil, "no River Secret may render with the hook off")
}

func chainCutoverAuthorisationAbsentByDefault(g *guard) {
	_, has := mp(secretsOf(chainDocsFromValues(g, chainValues(true)))[chainMigSecrets]["stringData"])["DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER"]
	g.expect(!has, "the cutover authorisation must be an operator passthrough that defaults to absent")
}

func chainWeight10JobAppliesRiverWithElevatedDSN(g *guard) {
	docs := chainDocsFromValues(g, chainValues(true))
	jobs, secrets := jobsOf(docs), secretsOf(docs)
	srcs := envFromSecretNames(jobs[chainRiver])
	if len(srcs) != 1 {
		g.failf("the River Job must read exactly one Secret, got %v", srcs)
		return
	}
	g.expect(str(at(secrets[srcs[0]], "stringData", "MIGRATION_DATABASE_URI")) == migrationDSN, "the weight-10 Job does not see the elevated DSN")
	g.expect(equal(firstContainer(jobs[chainRiver])["args"], any(anyStrs("migrate", "river", "--apply-and-check"))), "the River Job args are %v", firstContainer(jobs[chainRiver])["args"])
}

func TestOperatorImageGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"pin-hook-refuses-python-image-digest-pinned", pinHookRefusesPythonImageEvenWhenDigestPinned},
		check{"pin-hook-accepts-python-image-tag-pinned", pinHookAcceptsPythonImageWhenOnlyTagPinned},
		check{"pin-migrate-refuses-retired-images-by-name", pinMigrateRefusesRetiredImagesByName},
		check{"pin-migrate-refuses-contractcheck", pinMigrateRefusesContractcheckUnderNamedAllowlist},
		check{"pin-allowlist-names-anchored", pinAllowlistNamesAnchored},
		check{"pin-third-party-image-refused", pinThirdPartyImageRefused},
		check{"pin-route-activate-refuses-foreign-name", pinRouteActivateRefusesForeignOperatorName},
		check{"pin-hook-refuses-malformed-digest", pinHookRefusesMalformedDigest},
		check{"pin-hook-accepts-well-formed-digest", pinHookAcceptsWellFormedDigest},
		check{"pin-hook-refuses-short-hex-digest", pinHookRefusesShortHexDigest},
		check{"pin-hook-refuses-local-tag-with-always", pinHookRefusesLocalTagWithAlways},
		check{"pin-pgbouncer-refuses-malformed-digest", pinPgbouncerRefusesMalformedDigest},
		check{"pin-pgbouncer-accepts-well-formed-digest", pinPgbouncerAcceptsWellFormedDigest},
		check{"pin-hook-pull-policy-never-leaks", pinHookPullPolicyNeverLeaks},
		check{"pin-hook-pull-policy-overridable", pinHookPullPolicyOverridable},
		check{"chain-hooks-off-by-default", chainHooksOffByDefault},
		check{"chain-renders-in-weight-order", chainRendersInWeightOrder},
		check{"chain-migrate-weight-unchanged", chainMigrateWeightUnchanged},
		check{"chain-new-hooks-run-on-install-and-upgrade", chainNewHooksRunOnInstallAndUpgrade},
		check{"chain-failed-pods-retained", chainFailedPodsRetained},
		check{"chain-provisioning-runs-dho-migrate-roles", chainProvisioningRunsDhoMigrateRoles},
		check{"chain-provisioning-image-preference", chainProvisioningImagePreference},
		check{"chain-provisioning-refuses-unpinned-image", chainProvisioningRefusesUnpinnedImage},
		check{"chain-provisioning-refuses-lockstep-mismatch", chainProvisioningRefusesLockstepMismatch},
		check{"chain-provisioning-accepts-lockstep-and-local", chainProvisioningAcceptsLockstepAndLocal},
		check{"chain-river-defaults-to-pinned-operator", chainRiverDefaultsToPinnedOperator},
		check{"chain-river-refuses-unpinned-image", chainRiverRefusesUnpinnedImage},
		check{"chain-river-takes-its-own-pinned-image", chainRiverTakesItsOwnPinnedImage},
		check{"chain-river-refuses-lockstep-mismatch", chainRiverRefusesLockstepMismatch},
		check{"chain-river-accepts-lockstep-match", chainRiverAcceptsLockstepMatch},
		check{"chain-river-digest-pin-never-compared", chainRiverDigestPinIsNeverCompared},
		check{"chain-river-applies-with-no-shell", chainRiverAppliesWithNoShell},
		check{"chain-role-passwords-never-in-manifest", chainRolePasswordsNeverInManifest},
		check{"chain-password-secret-available-to-hook", chainPasswordSecretAvailableToHook},
		check{"chain-accepts-pre-created-external-secret", chainAcceptsPreCreatedExternalSecret},
		check{"chain-without-any-password-secret-fails", chainWithoutAnyPasswordSecretFails},
		check{"chain-provisioning-reads-its-own-hook-secret", chainProvisioningReadsItsOwnHookSecret},
		check{"chain-dsn-secret-has-no-password-key", chainDSNSecretHasNoPasswordKey},
		check{"chain-provisioning-keeps-dsn-aliases", chainProvisioningKeepsDSNAliases},
		check{"chain-provisioning-reads-operator-secret-directly", chainProvisioningReadsOperatorSecretDirectly},
		check{"chain-without-dsn-renders-empty-secret", chainWithoutDSNRendersEmptySecret},
		check{"chain-provisioning-env-is-exactly-the-verbs", chainProvisioningEnvIsExactlyTheVerbs},
		check{"chain-monitor-login-off-by-default", chainMonitorLoginOffByDefault},
		check{"chain-monitor-env-from-separate-secret", chainMonitorEnvFromSeparateSecret},
		check{"chain-monitor-secret-name-and-key-are-values", chainMonitorSecretNameAndKeyAreValues},
		check{"chain-monitor-without-its-secret-fails", chainMonitorWithoutItsSecretFails},
		check{"chain-monitor-without-name-stays-off", chainMonitorWithoutNameStaysOff},
		check{"chain-weight0-secret-omits-elevated-dsn", chainWeight0SecretOmitsElevatedDSN},
		check{"chain-weight10-secret-carries-elevated-dsn", chainWeight10SecretCarriesElevatedDSN},
		check{"chain-river-secret-created-before-job", chainRiverSecretCreatedBeforeJob},
		check{"chain-weight0-secret-unchanged-when-river-hook-off", chainWeight0SecretUnchangedWhenRiverHookOff},
		check{"chain-cutover-authorisation-absent-by-default", chainCutoverAuthorisationAbsentByDefault},
		check{"chain-weight10-job-applies-river-with-elevated-dsn", chainWeight10JobAppliesRiverWithElevatedDSN},
	)
}

func TestOperatorImageGuardsPlantedDefects(t *testing.T) {
	const hooks = chartRel + "/" + riverHooksTpl
	const helpers = chartRel + "/" + helpersTpl
	const mjob = chartRel + "/templates/migrate-job.yaml"
	const hookBlock = "helm.sh/hook: {{ join \",\" $hookEvents }}\n    helm.sh/hook-weight: \"5\"\n    helm.sh/hook-delete-policy: before-hook-creation"
	runPlanted(t, []planted{
		{"python-image-name-dropped-from-the-denylist", chartOnly,
			mut(helpers, "    \"dev-hops-api\" \"the Python api image\"\n", ""),
			pinHookRefusesPythonImageEvenWhenDigestPinned, "Python api image"},
		{"tag-pinned-python-image-now-refused", chartOnly,
			mut(helpers, "{{- if $digestPinned -}}\n{{- range $name, $label", "{{- if true -}}\n{{- range $name, $label"),
			pinHookAcceptsPythonImageWhenOnlyTagPinned, "disclosed tag-form boundary"},
		{"retired-image-dropped-from-the-denylist", chartOnly,
			mut(helpers, "    \"dev-health-query-api\" \"the retired dev-health-query-api image\"\n", ""),
			pinMigrateRefusesRetiredImagesByName, "has no dho entrypoint"},
		{"published-image-dropped-from-the-denylist", chartOnly,
			mut(helpers, "    \"dev-health-go-migrate\" \"the dev-health-go-migrate image (entrypoint dev-health-worker-migrate, not dho)\"\n", ""),
			pinMigrateRefusesRetiredImagesByName, "has no dho entrypoint"},
		{"contractcheck-joins-the-allowlist", chartOnly,
			mut(helpers, `(regexMatch "(^|/)dev-health-go-dho([:@]|$)" $image)`, `(regexMatch "(^|/)dev-health-go-(dho|contractcheck)([:@]|$)" $image)`),
			pinMigrateRefusesContractcheckUnderNamedAllowlist, "rendered"},
		{"allowlist-regex-unanchored", chartOnly,
			mut(helpers, `(regexMatch "(^|/)dev-health-go-dho([:@]|$)" $image)`, `(regexMatch "dev-health-go-dho" $image)`),
			pinAllowlistNamesAnchored, "rendered clean"},
		{"name-allowlist-removed", chartOnly,
			mut(helpers, `{{- if not (or (regexMatch "(^|/)dev-health-go-operator([:@]|$)" $image) (regexMatch "(^|/)dev-health-go-dho([:@]|$)" $image)) -}}`, `{{- if false -}}`),
			pinThirdPartyImageRefused, "rendered clean"},
		{"foreign-operator-name-accepted", chartOnly,
			mut(helpers, `{{- if not (or (regexMatch "(^|/)dev-health-go-operator([:@]|$)" $image) (regexMatch "(^|/)dev-health-go-dho([:@]|$)" $image)) -}}`, `{{- if false -}}`),
			pinRouteActivateRefusesForeignOperatorName, "rendered clean"},
		{"digest-shape-accepts-any-text", chartOnly,
			mut(helpers, `{{- $digestPinned := regexMatch "@sha256:[0-9a-f]{64}$" $image -}}`, `{{- $digestPinned := regexMatch "@sha256:" $image -}}`),
			pinHookRefusesMalformedDigest, "rendered clean"},
		{"digest-length-not-checked", chartOnly,
			mut(helpers, `{{- $digestPinned := regexMatch "@sha256:[0-9a-f]{64}$" $image -}}`, `{{- $digestPinned := regexMatch "@sha256:[0-9a-f]+$" $image -}}`),
			pinHookRefusesShortHexDigest, "rendered clean"},
		{"well-formed-digest-refused", chartOnly,
			mut(helpers, `{{- $digestPinned := regexMatch "@sha256:[0-9a-f]{64}$" $image -}}`, `{{- $digestPinned := regexMatch "@sha256:[0-9a-f]{65}$" $image -}}`),
			pinHookAcceptsWellFormedDigest, "must render"},
		{"local-tag-accepted-with-always", chartOnly,
			mut(helpers, `(or (eq .pullPolicy "Never") (eq .pullPolicy "IfNotPresent"))`, `true`),
			pinHookRefusesLocalTagWithAlways, "rendered clean"},
		{"pgbouncer-digest-shape-loosened", chartOnly,
			mut(chartFile("templates/go-pgbouncer.yaml"), `{{- if not (regexMatch "@sha256:[0-9a-f]{64}$" .Values.goWorkers.pgbouncer.image) }}`, `{{- if not (regexMatch "@sha256:" .Values.goWorkers.pgbouncer.image) }}`),
			pinPgbouncerRefusesMalformedDigest, "rendered clean"},
		{"pgbouncer-well-formed-digest-refused", chartOnly,
			mut(chartFile("templates/go-pgbouncer.yaml"), `{{- if not (regexMatch "@sha256:[0-9a-f]{64}$" .Values.goWorkers.pgbouncer.image) }}`, `{{- if not (regexMatch "@sha256:[0-9a-f]{65}$" .Values.goWorkers.pgbouncer.image) }}`),
			pinPgbouncerAcceptsWellFormedDigest, "must render"},
		{"hook-pull-policy-follows-the-application-image", chartOnly,
			mut(hooks, `{{- $rolesPullPolicy := .Values.migrations.hook.provisionRoles.pullPolicy | default .Values.migrations.hook.riverMigrate.pullPolicy | default .Values.migrations.hook.routeActivate.pullPolicy | default "IfNotPresent" }}`, `{{- $rolesPullPolicy := .Values.migrations.hook.provisionRoles.pullPolicy | default .Values.image.pullPolicy }}`),
			pinHookPullPolicyNeverLeaks, "IfNotPresent"},
		{"river-pull-policy-follows-the-application-image", chartOnly,
			mut(hooks, `{{- $riverPullPolicy := .Values.migrations.hook.riverMigrate.pullPolicy | default .Values.migrations.hook.routeActivate.pullPolicy | default "IfNotPresent" }}`, `{{- $riverPullPolicy := .Values.migrations.hook.riverMigrate.pullPolicy | default .Values.image.pullPolicy }}`),
			pinHookPullPolicyNeverLeaks, "IfNotPresent"},
		{"pull-policy-line-hidden-behind-a-yaml-comment", chartOnly,
			mut(hooks, "          imagePullPolicy: {{ $rolesPullPolicy }}", "          imagePullPolicy: Never\n          # imagePullPolicy: IfNotPresent"),
			pinHookPullPolicyNeverLeaks, "imagePullPolicy is"},
		{"hook-pull-policy-override-ignored", chartOnly,
			mut(hooks, `{{- $rolesPullPolicy := .Values.migrations.hook.provisionRoles.pullPolicy | default .Values.migrations.hook.riverMigrate.pullPolicy | default .Values.migrations.hook.routeActivate.pullPolicy | default "IfNotPresent" }}`, `{{- $rolesPullPolicy := "IfNotPresent" }}`),
			pinHookPullPolicyOverridable, "override did not take effect"},
		{"provisioning-weight-passes-the-river-weight", chartOnly,
			mut(hooks, hookBlock, strings.Replace(hookBlock, `"5"`, `"15"`, 1)),
			chainRendersInWeightOrder, "weights"},
		{"migrate-job-weight-moves", chartOnly,
			mut(mjob, `helm.sh/hook-weight: "0"`, `helm.sh/hook-weight: "3"`),
			chainMigrateWeightUnchanged, "weight 0"},
		{"provisioning-hook-skips-upgrades", chartOnly,
			mut(hooks, hookBlock, strings.Replace(hookBlock, `{{ join "," $hookEvents }}`, `pre-install`, 1)),
			chainNewHooksRunOnInstallAndUpgrade, "must also run on upgrade"},
		{"provisioning-pod-deleted-on-success", chartOnly,
			mut(hooks, hookBlock, hookBlock+",hook-succeeded"),
			chainFailedPodsRetained, "evidence"},
		{"provisioning-args-change", chartOnly,
			mut(hooks, `args: [migrate, roles]`, `args: [migrate, roles, --dry-run]`),
			chainProvisioningRunsDhoMigrateRoles, "args"},
		{"provisioning-lockstep-check-removed", chartOnly,
			mut(helpers, `{{- if and $queryApiTag (ne $queryApiTag $hookImageTag) -}}`, `{{- if false -}}`),
			chainProvisioningRefusesLockstepMismatch, "pinned to another commit"},
		{"river-lockstep-check-removed", chartOnly,
			mut(helpers, `{{- if and $queryApiTag (ne $queryApiTag $hookImageTag) -}}`, `{{- if false -}}`),
			chainRiverRefusesLockstepMismatch, "different commit"},
		{"unpinned-image-accepted", chartOnly,
			mut(helpers, `{{- if not (or $digestPinned $immutableTag $sideloadedLocal) -}}`, `{{- if false -}}`),
			chainRiverRefusesUnpinnedImage, "rendered"},
		{"provisioning-ignores-the-river-image", chartOnly,
			mut(hooks, "{{- $rolesImage := .Values.migrations.hook.provisionRoles.image | default .Values.migrations.hook.riverMigrate.image | default .Values.migrations.hook.routeActivate.image }}", "{{- $rolesImage := .Values.migrations.hook.provisionRoles.image | default .Values.migrations.hook.routeActivate.image }}"),
			chainProvisioningImagePreference, "default to the River image"},
		{"river-args-lose-apply-and-check", chartOnly,
			mut(hooks, `args: [migrate, river, --apply-and-check]`, `args: [migrate, river]`),
			chainRiverAppliesWithNoShell, "args"},
		{"provisioning-password-inlined-as-a-value", chartOnly,
			mut(hooks, "            - name: RIVER_DOMAIN_DATABASE_PASSWORD\n              valueFrom:\n                secretKeyRef:\n                  name: {{ $provisionSecretName }}\n                  key: RIVER_DOMAIN_DATABASE_PASSWORD", "            - name: RIVER_DOMAIN_DATABASE_PASSWORD\n              value: {{ $poolerSecret.data.RIVER_DOMAIN_DATABASE_PASSWORD | quote }}"),
			chainRolePasswordsNeverInManifest, "embeds a role password"},
		{"provisioning-dsn-secret-created-after-the-job", chartOnly,
			mut(hooks, "    helm.sh/hook-weight: \"-5\"\n    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded\ntype: Opaque\n{{- if $rolesDSN }}", "    helm.sh/hook-weight: \"9\"\n    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded\ntype: Opaque\n{{- if $rolesDSN }}"),
			chainProvisioningReadsItsOwnHookSecret, "pre-install hook resource"},
		{"river-secret-created-after-the-job", chartOnly,
			mut(hooks, "    helm.sh/hook-weight: \"-5\"\n    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded\ntype: Opaque\nstringData:\n  MIGRATION_DATABASE_URI: {{ $riverDSN | quote }}", "    helm.sh/hook-weight: \"20\"\n    helm.sh/hook-delete-policy: before-hook-creation,hook-succeeded\ntype: Opaque\nstringData:\n  MIGRATION_DATABASE_URI: {{ $riverDSN | quote }}"),
			chainRiverSecretCreatedBeforeJob, "weight below 10"},
		{"provisioning-reads-the-wrong-hook-secret", chartOnly,
			mut(hooks, "                name: {{ include \"dev-health.fullname\" . }}-provision-roles-conn\n              {{- else }}", "                name: {{ include \"dev-health.fullname\" . }}-river-migrate-secrets\n              {{- else }}"),
			chainProvisioningReadsItsOwnHookSecret, "reads secrets"},
		{"provisioning-env-gains-a-variable", chartOnly,
			mut(hooks, "            - name: RIVER_DOMAIN_DATABASE_PASSWORD\n              valueFrom:", "            - name: EXTRA_VAR\n              value: x\n            - name: RIVER_DOMAIN_DATABASE_PASSWORD\n              valueFrom:"),
			chainProvisioningEnvIsExactlyTheVerbs, "the provisioning env is"},
		{"monitor-secret-name-check-removed", chartOnly,
			mut(hooks, `{{- if not $monitorSecretName }}`, `{{- if false }}`),
			chainMonitorWithoutItsSecretFails, "secretName"},
		{"monitor-secret-key-check-removed", chartOnly,
			mut(hooks, `{{- if not $monitorSecretKey }}`, `{{- if false }}`),
			chainMonitorWithoutItsSecretFails, "secretKey"},
		{"monitor-login-always-on", chartOnly,
			mut(hooks, "{{- if $monitorName }}\n            - name: OTEL_PG_MONITOR_DATABASE_ROLE", "{{- if true }}\n            - name: OTEL_PG_MONITOR_DATABASE_ROLE"),
			chainMonitorLoginOffByDefault, "present by default"},
		{"weight-0-secret-keeps-the-elevated-dsn", chartOnly,
			mut(helpers, `{{- if and $value (not (and $riverHookOwnsRiver (eq $key "MIGRATION_DATABASE_URI"))) }}`, `{{- if $value }}`),
			chainWeight0SecretOmitsElevatedDSN, "weight-0 Job"},
		{"weight-0-secret-loses-the-alembic-alias", chartOnly,
			mut(helpers, `{{- if and $riverHookOwnsRiver $hasDedicatedMigrationURI (not $hasHookPostgresURI) (not $hasHookDatabaseURI) }}`, `{{- if false }}`),
			chainWeight0SecretOmitsElevatedDSN, "Alembic still needs"},
		{"split-leaks-into-the-hook-off-case", chartOnly,
			mut(helpers, `{{- $riverHookOwnsRiver := .Values.migrations.hook.riverMigrate.enabled }}`, `{{- $riverHookOwnsRiver := true }}`),
			chainWeight0SecretUnchangedWhenRiverHookOff, "with the River hook off"},
		{"river-job-reads-the-shared-secret", chartOnly,
			mut(hooks, "                name: {{ include \"dev-health.fullname\" . }}-river-migrate-secrets\n              {{- else }}", "                name: {{ include \"dev-health.secretName\" . }}\n              {{- else }}"),
			chainWeight10SecretCarriesElevatedDSN, "does not read its own Secret"},
		{"cutover-variable-added-by-default", chartOnly,
			mut(helpers, `{{- if not (index .Values.migrations.hook.secretData "CLICKHOUSE_URI") }}`, "DEV_HEALTH_ALLOW_CELERY_RIVER_CUTOVER: \"1\"\n{{- if not (index .Values.migrations.hook.secretData \"CLICKHOUSE_URI\") }}"),
			chainCutoverAuthorisationAbsentByDefault, "defaults to absent"},
		{"river-dsn-ignores-the-hook-aliases", chartOnly,
			mut(helpers, `{{- $dsn := or (index $hook "MIGRATION_DATABASE_URI") (index $hook "POSTGRES_URI") (index $hook "DATABASE_URI") (index .Values.secrets.data "POSTGRES_URI") (index .Values.secrets.data "DATABASE_URI") }}`, `{{- $dsn := or (index $hook "MIGRATION_DATABASE_URI") (index $hook "POSTGRES_URI") }}`),
			chainProvisioningKeepsDSNAliases, "alias-only install"},
	})
}

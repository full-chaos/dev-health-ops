package chartgates_test

import (
	"encoding/json"
	"regexp"
	"slices"
	"sort"
	"strings"
	"testing"

	"github.com/full-chaos/dev-health-ops/internal/platform/config"
	"github.com/full-chaos/dev-health-ops/internal/platform/dsnclass"
)

// The worker groups take the non-DSN keys they read from the shared Secret one secretKeyRef at a time
// (goWorkers.secretKeys); the Secret is no envFrom source of a worker pod, so no database DSN it holds
// reaches one. Every key the deploy loader puts in that Secret is classified exactly once below.

// The set of keys the deploy loader writes is not listed here: the loader script lives in the deploy repo, and a copy
// would only prove the copy. The deploy repo's own test compares that script with this chart's allow-list. This test
// checks what the ops repo can see: the chart, the config registry and the Go sources.

// notReadByWorkers are the shared-Secret keys no worker group reads, each with the reason (a decision, not an inventory).
var notReadByWorkers = map[string]string{
	"ADMIN_API_KEY":                    "read by no Go process of the worker groups",
	"ADMIN_USERS":                      "read by no Go process of the worker groups",
	"AUTH_AUTO_CREATE_ORG_ON_REGISTER": "registration setting of the api",
	"CLICKHOUSE_NATIVE_URI":            "reaches the workers as CLICKHOUSE_URI through its own secretKeyRef",
	"CORS_ALLOWED_ORIGINS":             "http edge setting of the api and query-api",
	"DOMAIN":                           "no Go reader of this name",
	"GITHUB_APP_CALLBACK_URL":          "GitHub App install route of the api",
	"GITHUB_APP_CLIENT_ID":             "GitHub App install route of the api",
	"GITHUB_APP_CLIENT_SECRET":         "GitHub App install route of the api",
	"GITHUB_APP_PRIVATE_KEY":           "GitHub App install route of the api",
	"GITHUB_APP_SLUG":                  "GitHub App install route of the api",
	"GITHUB_WEBHOOK_SECRET":            "webhook intake of the api",
	"GOOGLE_CLIENT_ID":                 "no Go reader of this name",
	"GOOGLE_CLIENT_SECRET":             "no Go reader of this name",
	"JWT_SECRET_KEY":                   "the HS256 access-token key has its own Secret and no worker group carries it (CHAOS-10446)",
	"LICENSE_PUBLIC_KEY":               "licence verification of the api",
	"SENTRY_DSN":                       "no Go reader of this name",
	"SENTRY_ENVIRONMENT":               "no Go reader of this name",
	"SENTRY_TRACES_RATE":               "no Go reader of this name",
	"SOCIAL_GITHUB_CLIENT_ID":          "social sign-in of the auth service",
	"SOCIAL_GITHUB_CLIENT_SECRET":      "social sign-in of the auth service",
	"SOCIAL_GOOGLE_CLIENT_ID":          "social sign-in of the auth service",
	"SOCIAL_GOOGLE_CLIENT_SECRET":      "social sign-in of the auth service",
	"TRUSTED_PROXIES":                  "client address handling of the api",
}

var workerServices = []string{"dev-health-worker", "dev-health-reconciler", "dev-health-stream-runner", "dev-health-scheduler"}

func chartWorkerSecretKeys(g *guard) []string {
	return strs(valuesGoWorkers(g)["secretKeys"])
}

func sharedSecretRefNames(c doc, secret string) []string {
	var out []string
	for _, e := range list(c["env"]) {
		if str(at(e, "valueFrom", "secretKeyRef", "name")) == secret {
			out = append(out, str(at(e, "name")))
		}
	}
	sort.Strings(out)
	return out
}

func workerGroupContainers(docs []doc) map[string]doc {
	out := map[string]doc{}
	for _, d := range ofKind(docs, "Deployment") {
		group := label(d, "dev-health.io/worker-group")
		if group == "" {
			continue
		}
		for _, c := range containers(d) {
			out[group] = c
		}
	}
	return out
}

func workerGroupsMountNoWholeSharedSecret(g *guard) {
	docs := g.docs("worker-secret-keys", poolGoValues...)
	groups := workerGroupContainers(docs)
	g.expect(len(groups) == len(processes(g)), "%d worker groups rendered, the manifest lists %d", len(groups), len(processes(g)))
	for group, c := range groups {
		for _, ref := range envFromRefs(c) {
			g.expect(ref[0] != "secretRef", "worker group %s takes the Secret %s through envFrom", group, ref[1])
		}
	}
	extra := workerGroupContainers(g.docs("worker-secret-keys-extra", join(poolGoValues, set("goWorkers.extraEnvFrom[0].secretRef.name=operator-env"))...))
	for group, c := range extra {
		g.expect(slices.Contains(envFromRefs(c), [2]string{"secretRef", "operator-env"}), "worker group %s lost the operator envFrom source", group)
	}
}

func workerGroupsRenderSecretKeysAsOptionalRefs(g *guard) {
	keys := chartWorkerSecretKeys(g)
	g.expect(len(keys) > 0, "goWorkers.secretKeys is empty, so the check measured nothing")
	secret := ""
	for _, d := range ofKind(g.docs("worker-secret-keys-name", poolGoValues...), "Secret") {
		if name(d) == "worker-secret-keys-name-dev-health-secrets" {
			secret = name(d)
		}
	}
	g.expect(secret != "", "the shared Secret was not rendered, so the check measured nothing")
	groups := workerGroupContainers(g.docs("worker-secret-keys-name", poolGoValues...))
	g.expect(len(groups) == len(processes(g)), "%d worker groups rendered, the manifest lists %d", len(groups), len(processes(g)))
	for group, c := range groups {
		env := envMap(c)
		want := slices.Clone(keys)
		if entry := env["CLICKHOUSE_URI"]; entry != nil && str(entry["value"]) != "" {
			want = slices.DeleteFunc(want, func(k string) bool { return k == "CLICKHOUSE_URI" })
		}
		sort.Strings(want)
		got := sharedSecretRefNames(c, secret)
		g.expect(slices.Equal(got, want), "worker group %s takes %v from the shared Secret, goWorkers.secretKeys lists %v", group, got, want)
		for _, k := range got {
			if k == "CLICKHOUSE_URI" {
				continue
			}
			g.expect(at(env[k], "valueFrom", "secretKeyRef", "key") == k, "worker group %s: %s is not a secretKeyRef on the key of the same name", group, k)
			g.expect(at(env[k], "valueFrom", "secretKeyRef", "optional") == true, "worker group %s: %s is not optional", group, k)
		}
	}
}

func sharedSecretKeysAreClassified(g *guard, readers map[string][]string) {
	keys := chartWorkerSecretKeys(g)
	allowed := chartAllowedKeys(g)
	registry := config.EnvNames()
	for _, k := range keys {
		g.expect(!isDSNSecret(k), "goWorkers.secretKeys lists %s, a database DSN secret", k)
		g.expect(slices.Contains(registry, k) || len(readers[k]) > 0, "goWorkers.secretKeys lists %s, which neither the config registry nor an environment lookup in the Go sources reads", k)
		_, notRead := notReadByWorkers[k]
		g.expect(!notRead, "goWorkers.secretKeys lists %s, which the not-read table says no worker group reads", k)
	}
	for k, reason := range notReadByWorkers {
		g.expect(reason != "" && !isDSNSecret(k) && !slices.Contains(allowed, k), "%s is in the not-read table without a reason, as a database DSN, or in the chart allow-list", k)
	}
	for _, svc := range workerServices {
		for _, queues := range []bool{true, false} {
			for _, o := range config.OptionsFor(svc, queues) {
				if o.Env == "" || isDSNSecret(o.Env) || !slices.Contains(allowed, o.Env) {
					continue
				}
				g.expect(slices.Contains(keys, o.Env), "%s applies to %s and is in the chart allow-list but not in goWorkers.secretKeys", o.Env, svc)
			}
		}
	}
}

// chartDSNNames reads the one DSN name list of the chart helper that both the secretKeys refusal and the blanks use.
func chartDSNNames(g *guard) []string {
	m := regexp.MustCompile(`(?s)define "dev-health\.goWorkerDSNNames".*?toJson \(list (.*?)\)`).FindStringSubmatch(g.read(chartFile("templates/_helpers.tpl")))
	if m == nil {
		g.failf("the chart has no dev-health.goWorkerDSNNames helper")
		return nil
	}
	return regexp.MustCompile(`"([A-Z0-9_]+)"`).FindAllString(m[1], -1)
}

func chartAllowedKeys(g *guard) []string {
	m := regexp.MustCompile(`(?s)define "dev-health\.goWorkerSecretKeyNames".*?toJson \(list (.*?)\)`).FindStringSubmatch(g.read(chartFile("templates/_helpers.tpl")))
	if m == nil {
		g.failf("the chart has no dev-health.goWorkerSecretKeyNames helper")
		return nil
	}
	return unquoted(regexp.MustCompile(`"([A-Z0-9_]+)"`).FindAllString(m[1], -1))
}

func unquoted(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strings.Trim(s, `"`)
	}
	return out
}

// Every database DSN name known to the code is in the chart list, apart from the names the chart itself renders
// into a worker group from its own pooler Secret.
var envNameShape = regexp.MustCompile(`^[A-Z][A-Z0-9_]*$`)

// testOnlyDSNNames are DSN-shaped names the Go sources look up that no deployed worker process reads: test harness inputs.
var testOnlyDSNNames = map[string]string{
	"DEVHEALTH_AUTH_ORACLE_POSTGRES_URI": "auth oracle test harness input",
	"DEV_HEALTH_TEST_POSTGRES_DSN":       "test database input of the Go tests",
}

func chartDSNListMatchesDSNClass(g *guard, readers map[string][]string) {
	listed := unquoted(chartDSNNames(g))
	g.expect(len(listed) > 0, "the chart DSN list is empty, so the check measured nothing")
	ownRendered := []string{"POSTGRES_URI"}
	census := slices.Concat(config.EnvNames(), dsnclass.Allowed(dsnclass.Control), dsnclass.Allowed(dsnclass.Migration))
	for name := range readers {
		if _, test := testOnlyDSNNames[name]; !test && envNameShape.MatchString(name) {
			census = append(census, name)
		}
	}
	for _, k := range census {
		if isDSNSecret(k) && !slices.Contains(ownRendered, k) {
			g.expect(slices.Contains(listed, k), "%s is a database DSN secret by dsnclass and is not in the chart DSN list", k)
		}
	}
	for _, k := range listed {
		g.expect(isDSNSecret(k), "the chart DSN list names %s, which dsnclass does not classify as a database DSN secret", k)
	}
}

// The allow-list of worker secret keys holds only keys some code reads and no worker group is recorded as skipping: none a
// database DSN, none in the not-read table, each known to the config registry or read by an environment lookup in the Go
// sources, and it holds the chart defaults. That every key the deploy loader supplies is classified is checked in the deploy repo.
func chartAllowListIsTheCensus(g *guard, readers map[string][]string) {
	allowed := chartAllowedKeys(g)
	g.expect(len(allowed) > 0, "the chart allow-list is empty, so the check measured nothing")
	registry := config.EnvNames()
	for _, k := range allowed {
		g.expect(!isDSNSecret(k), "the chart allow-list names %s, a database DSN secret", k)
		_, skipped := notReadByWorkers[k]
		g.expect(!skipped, "the chart allow-list names %s, which the not-read table says no worker group reads", k)
		g.expect(slices.Contains(registry, k) || len(readers[k]) > 0, "the chart allow-list names %s, which neither the config registry nor an environment lookup in the Go sources reads", k)
	}
	for _, k := range chartWorkerSecretKeys(g) {
		g.expect(slices.Contains(allowed, k), "the default goWorkers.secretKeys lists %s, outside the chart allow-list", k)
	}
}

// A name outside the allow-list, DSN-shaped or not, refuses the render by the template and by the schema.
func workerSecretKeysRefuseUnlistedNames(g *guard) {
	for _, name := range []string{"ANALYTICS_DATABASE_URI", "SOME_OTHER_KEY"} {
		override := set("goWorkers.secretKeys={APP_BASE_URL," + name + "}")
		out := g.refusal("template refusal of "+name, "worker-secret-keys-unlisted-tpl", join(poolGoValues, override, []string{"--skip-schema-validation"})...)
		g.expect(strings.Contains(out, "goWorkers.secretKeys lists "+name+", which is not a worker secret key"), "the template refusal of %s says %q", name, firstLines(out, 2))
		sch := g.refusal("schema refusal of "+name, "worker-secret-keys-unlisted-schema", join(poolGoValues, override)...)
		g.expect(strings.Contains(sch, "values don't meet the specifications of the schema"), "the refusal of %s is not the schema error: %q", name, firstLines(sch, 2))
	}
}

// goWorkers.extraEnvFrom may not name the shared Secret; an unrelated operator Secret still renders.
func workerExtraEnvFromRefusesTheSharedSecret(g *guard) {
	out := g.refusal("extraEnvFrom names the shared Secret", "worker-extra-shared", join(poolGoValues, set("goWorkers.extraEnvFrom[0].secretRef.name=worker-extra-shared-dev-health-secrets"))...)
	g.expect(strings.Contains(out, "goWorkers.extraEnvFrom names the shared Secret worker-extra-shared-dev-health-secrets"), "the extraEnvFrom refusal says %q", firstLines(out, 2))
	g.mustRender("an unrelated operator Secret in extraEnvFrom", "worker-extra-other", join(poolGoValues, set("goWorkers.extraEnvFrom[0].secretRef.name=operator-env"))...)
}

// groupWithExtraEnv is the chart's worker groups with one extraEnv entry on the first group.
func groupWithExtraEnv(g *guard, entry map[string]any) []string {
	groups := list(valuesGoWorkers(g)["groups"])
	if len(groups) == 0 {
		g.failf("the chart has no worker groups, so the check measured nothing")
		return nil
	}
	first := mp(groups[0])
	first["extraEnv"] = []any{map[string]any{"name": "TRIAL_SWITCH", "value": "1"}, entry}
	groups[0] = map[string]any(first)
	b, err := json.Marshal(groups)
	if err != nil {
		g.failf("cannot encode the groups: %v", err)
		return nil
	}
	return setJSON("goWorkers.groups=" + string(b))
}

// A group's extraEnv cannot bring a database DSN back: not by name, and not as a key of the shared Secret the allow-list excludes.
func workerGroupExtraEnvRefusesDSNs(g *guard) {
	shared := "worker-extra-env-dev-health-secrets"
	byName := groupWithExtraEnv(g, map[string]any{"name": "DATABASE_URI", "value": "postgresql://x@h/db"})
	out := g.refusal("extraEnv names a DSN", "worker-extra-env", join(poolGoValues, byName)...)
	g.expect(strings.Contains(out, "extraEnv names DATABASE_URI, a database DSN name"), "the extraEnv DSN-name refusal says %q", firstLines(out, 2))
	ref := groupWithExtraEnv(g, map[string]any{"name": "ALIAS", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": shared, "key": "DATABASE_URI"}}})
	out = g.refusal("extraEnv reads a DSN key of the shared Secret", "worker-extra-env", join(poolGoValues, ref)...)
	g.expect(strings.Contains(out, "extraEnv reads key DATABASE_URI of the shared Secret"), "the extraEnv shared-Secret refusal says %q", firstLines(out, 2))
	okRef := groupWithExtraEnv(g, map[string]any{"name": "ALIAS_BASE_URL", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": shared, "key": "APP_BASE_URL"}}})
	g.mustRender("extraEnv reads an allowed key of the shared Secret", "worker-extra-env", join(poolGoValues, okRef)...)
	other := groupWithExtraEnv(g, map[string]any{"name": "ALIAS", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "operator-env", "key": "DATABASE_URI"}}})
	g.mustRender("extraEnv reads an operator Secret", "worker-extra-env", join(poolGoValues, other)...)
}

// A group's extraEnv may not reuse any env name the chart already renders for the worker containers; it would win as the last entry.
func workerGroupExtraEnvRefusesChartOwnedNames(g *guard) {
	owned := map[string]bool{}
	for _, c := range workerGroupContainers(g.docs("worker-extra-env-owned", poolGoValues...)) {
		for _, e := range list(c["env"]) {
			owned[str(at(e, "name"))] = true
		}
	}
	for _, n := range []string{"POSTGRES_URI", "CLICKHOUSE_URI", "APP_BASE_URL", "OTEL_SERVICE_NAME", "OPERATIONAL_ORDERING_CONTRACT"} {
		g.expect(owned[n], "the default render sets no %s, so the check measured nothing", n)
	}
	names := make([]string, 0, len(owned))
	for n := range owned {
		names = append(names, n)
	}
	sort.Strings(names)
	dsnNames := unquoted(chartDSNNames(g))
	for _, n := range names {
		out := g.refusal("extraEnv reuses the chart-owned "+n, "worker-extra-env-owned", join(poolGoValues, groupWithExtraEnv(g, map[string]any{"name": n, "value": "x"}))...)
		dsnMessage, ownedMessage := "extraEnv names "+n+", a database DSN name", "extraEnv names "+n+", which the chart already sets"
		if slices.Contains(dsnNames, n) {
			g.expect(strings.Contains(out, dsnMessage), "the refusal of the DSN name %s says %q, not the DSN message", n, firstLines(out, 2))
		} else {
			g.expect(strings.Contains(out, ownedMessage), "the refusal of the chart-owned name %s says %q, not the chart-owned message", n, firstLines(out, 2))
		}
	}
	out := g.refusal("extraEnv replaces POSTGRES_URI", "worker-extra-env-owned", join(poolGoValues, groupWithExtraEnv(g, map[string]any{"name": "POSTGRES_URI", "valueFrom": map[string]any{"secretKeyRef": map[string]any{"name": "other-secret", "key": "POSTGRES_URI"}}}))...)
	g.expect(strings.Contains(out, "extraEnv names POSTGRES_URI, which the chart already sets"), "the POSTGRES_URI refusal says %q", firstLines(out, 2))
	for _, n := range []string{"TRIAL_SWITCH", "OTEL_SAMPLE_RATE", "TYPESAFE_API_KEY"} {
		g.mustRender("extraEnv sets "+n, "worker-extra-env-free", join(poolGoValues, groupWithExtraEnv(g, map[string]any{"name": n, "value": "x"}))...)
	}
}

// goWorkers.secretKeys naming a database DSN refuses the render: by the template (schema validation skipped) and by the schema.
func workerSecretKeysRefuseDSNNames(g *guard) {
	for _, name := range unquoted(chartDSNNames(g)) {
		override := set("goWorkers.secretKeys={APP_BASE_URL," + name + "}")
		out := g.refusal("template refusal of "+name, "worker-secret-keys-dsn-tpl", join(poolGoValues, override, []string{"--skip-schema-validation"})...)
		g.expect(strings.Contains(out, "goWorkers.secretKeys lists "+name+", a database DSN name"), "the template refusal of %s says %q", name, firstLines(out, 2))
		sch := g.refusal("schema refusal of "+name, "worker-secret-keys-dsn-schema", join(poolGoValues, override)...)
		g.expect(strings.Contains(sch, "values don't meet the specifications of the schema"), "the refusal of %s is not the schema error: %q", name, firstLines(sch, 2))
	}
}

// The schema refuses the same names, so a values check that runs the schema fails early; the template stays the authority.
func workerSecretKeysSchemaListsChartDSNNames(g *guard) {
	items := at(g.yamlFile(chartFile("values.schema.json")), "properties", "goWorkers", "properties", "secretKeys", "items", "enum")
	got := strs(items)
	want := chartAllowedKeys(g)
	sort.Strings(got)
	sort.Strings(want)
	g.expect(len(want) > 0 && slices.Equal(got, want), "values.schema.json allows %v in goWorkers.secretKeys, the chart allow-list is %v", got, want)
}

// The blanks render after the secretKeyRef entries, so a DSN name that reached the list still ends blank.
func workerDSNBlanksRenderLast(g *guard) {
	dsn := unquoted(chartDSNNames(g))
	groups := workerGroupContainers(g.docs("worker-dsn-blank-order", poolGoValues...))
	g.expect(len(groups) == len(processes(g)), "%d worker groups rendered, the manifest lists %d", len(groups), len(processes(g)))
	for group, c := range groups {
		lastRef, firstBlank, blanks := -1, -1, 0
		for i, e := range list(c["env"]) {
			n := str(at(e, "name"))
			if at(e, "valueFrom", "secretKeyRef", "optional") == true {
				lastRef = i
			}
			if slices.Contains(dsn, n) && str(at(e, "value")) == "" && at(e, "valueFrom") == nil {
				blanks++
				if firstBlank < 0 {
					firstBlank = i
				}
			}
		}
		g.expect(lastRef >= 0 && blanks > 0, "worker group %s renders no optional secret reference or no blank DSN, so the check measured nothing", group)
		g.expect(firstBlank > lastRef, "worker group %s renders a blank DSN entry (%d) before its last secretKeyRef entry (%d)", group, firstBlank, lastRef)
	}
}

func isDSNSecret(k string) bool { _, ok := dsnclass.Secret(k); return ok }

func TestWorkerSecretKeysGuardsRealTree(t *testing.T) {
	runReal(t,
		check{"worker-groups-mount-no-whole-shared-secret", workerGroupsMountNoWholeSharedSecret},
		check{"worker-groups-render-secret-keys-as-optional-refs", workerGroupsRenderSecretKeysAsOptionalRefs},
		check{"shared-secret-keys-are-classified", func(g *guard) { sharedSecretKeysAreClassified(g, realEnvReaders(g)) }},
		check{"chart-dsn-list-matches-dsnclass", func(g *guard) { chartDSNListMatchesDSNClass(g, realEnvReaders(g)) }},
		check{"worker-secret-keys-refuse-dsn-names", workerSecretKeysRefuseDSNNames},
		check{"chart-allow-list-is-the-census", func(g *guard) { chartAllowListIsTheCensus(g, realEnvReaders(g)) }},
		check{"dsn-alias-limits-are-pinned", func(g *guard) { dsnAliasLimits(g, dsnclass.CheckEnviron) }},
		check{"worker-secret-keys-refuse-unlisted-names", workerSecretKeysRefuseUnlistedNames},
		check{"worker-group-extra-env-refuses-dsns", workerGroupExtraEnvRefusesDSNs},
		check{"worker-group-extra-env-refuses-chart-owned-names", workerGroupExtraEnvRefusesChartOwnedNames},
		check{"worker-extra-envfrom-refuses-the-shared-secret", workerExtraEnvFromRefusesTheSharedSecret},
		check{"worker-secret-keys-schema-lists-chart-dsn-names", workerSecretKeysSchemaListsChartDSNNames},
		check{"worker-dsn-blanks-render-last", workerDSNBlanksRenderLast},
	)
}

func TestWorkerSecretKeysGuardsPlantedDefects(t *testing.T) {
	const workers = "templates/go-workers.yaml"
	const values = "values.yaml"
	reader := []string{chartRel, deploymentJSON}
	runPlanted(t, []planted{
		{"the-shared-secret-is-an-envfrom-source-again", reader,
			mut(chartFile(workers), "          {{- $groupEnvFrom = append $groupEnvFrom (dict \"configMapRef\" (dict \"name\" (include \"dev-health.configMapName\" $))) }}\n",
				"          {{- $groupEnvFrom = append $groupEnvFrom (dict \"configMapRef\" (dict \"name\" (include \"dev-health.configMapName\" $))) }}\n          {{- $groupEnvFrom = append $groupEnvFrom (dict \"secretRef\" (dict \"name\" (include \"dev-health.secretName\" $))) }}\n"),
			workerGroupsMountNoWholeSharedSecret, "through envFrom"},
		{"a-listed-key-is-not-rendered", reader,
			mut(chartFile(workers), "            {{- range $key := $.Values.goWorkers.secretKeys }}", "            {{- range $key := list }}"),
			workerGroupsRenderSecretKeysAsOptionalRefs, "takes"},
		{"a-listed-key-is-required", reader,
			mut(chartFile(workers), "key: {{ $key }}, optional: true}", "key: {{ $key }}}"),
			workerGroupsRenderSecretKeysAsOptionalRefs, "is not optional"},
		{"a-database-dsn-is-listed", reader,
			mut(chartFile(values), "    - APP_BASE_URL\n", "    - APP_BASE_URL\n    - DATABASE_URI\n"),
			func(g *guard) { sharedSecretKeysAreClassified(g, nil) }, "a database DSN secret"},
		{"the-template-stops-refusing-dsn-names", reader,
			all(mut(chartFile(workers), "{{- fail (printf \"goWorkers.secretKeys lists %s, a database", "{{- $_ := (printf \"goWorkers.secretKeys lists %s, a database"),
				mut(chartFile(workers), "{{- fail (printf \"goWorkers.secretKeys lists %s, which is not", "{{- $_ := (printf \"goWorkers.secretKeys lists %s, which is not"),
				mut(chartFile("values.schema.json"), `"enum": ["APP_BASE_URL"`, `"x-enum": ["APP_BASE_URL"`)),
			workerSecretKeysRefuseDSNNames, "must be refused"},
		{"the-schema-stops-refusing-dsn-names", reader,
			mut(chartFile("values.schema.json"), `"enum": ["APP_BASE_URL"`, `"x-enum": ["APP_BASE_URL"`),
			workerSecretKeysSchemaListsChartDSNNames, "values.schema.json allows"},
		{"the-template-stops-refusing-unlisted-names", reader,
			all(mut(chartFile(workers), "{{- fail (printf \"goWorkers.secretKeys lists %s, which is not", "{{- $_ := (printf \"goWorkers.secretKeys lists %s, which is not"),
				mut(chartFile("values.schema.json"), `"enum": ["APP_BASE_URL"`, `"x-enum": ["APP_BASE_URL"`)),
			workerSecretKeysRefuseUnlistedNames, "must be refused"},
		{"extraenv-may-replace-a-chart-owned-name", reader,
			mut(chartFile(workers), "{{- fail (printf \"goWorkers.groups[%s].extraEnv names %s, which the chart", "{{- $_ := (printf \"goWorkers.groups[%s].extraEnv names %s, which the chart"),
			workerGroupExtraEnvRefusesChartOwnedNames, "must be refused"},
		{"extraenv-may-name-a-dsn", reader,
			all(mut(chartFile(workers), "{{- fail (printf \"goWorkers.groups[%s].extraEnv names %s, a database", "{{- $_ := (printf \"goWorkers.groups[%s].extraEnv names %s, a database"),
				mut(chartFile(workers), "{{- fail (printf \"goWorkers.groups[%s].extraEnv names %s, which the chart", "{{- $_ := (printf \"goWorkers.groups[%s].extraEnv names %s, which the chart")),
			workerGroupExtraEnvRefusesDSNs, "must be refused"},
		{"extraenv-may-read-a-dsn-key-of-the-shared-secret", reader,
			mut(chartFile(workers), "{{- fail (printf \"goWorkers.groups[%s].extraEnv reads", "{{- $_ := (printf \"goWorkers.groups[%s].extraEnv reads"),
			workerGroupExtraEnvRefusesDSNs, "must be refused"},
		{"extraenvfrom-may-name-the-shared-secret", reader,
			mut(chartFile(workers), "{{- fail (printf \"goWorkers.extraEnvFrom names", "{{- $_ := (printf \"goWorkers.extraEnvFrom names"),
			workerExtraEnvFromRefusesTheSharedSecret, "must be refused"},
		{"the-allow-list-loses-a-worker-key", reader,
			mut(chartFile("templates/_helpers.tpl"), `"EMAIL_API_KEY" `, ""),
			func(g *guard) { chartAllowListIsTheCensus(g, nil) }, "outside the chart allow-list"},
		{"the-allow-list-gains-a-dsn-name", reader,
			mut(chartFile("templates/_helpers.tpl"), `(list "APP_BASE_URL"`, `(list "API_DATABASE_URI" "APP_BASE_URL"`),
			func(g *guard) { chartAllowListIsTheCensus(g, nil) }, "a database DSN secret"},
		{"a-listed-key-has-no-reader-in-the-sources", reader,
			mut(chartFile(values), "    - APP_BASE_URL\n", "    - APP_BASE_URL\n    - A_KEY_NO_ONE_READS\n"),
			func(g *guard) { sharedSecretKeysAreClassified(g, map[string][]string{}) }, "neither the config registry nor an environment lookup"},
		{"a-source-reads-a-dsn-name-the-chart-list-lacks", reader, nil,
			func(g *guard) {
				chartDSNListMatchesDSNClass(g, map[string][]string{"ANALYTICS_DATABASE_URI": {"internal/analytics/store.go"}})
			}, "ANALYTICS_DATABASE_URI is a database DSN secret"},
		{"the-blanks-are-gone", reader,
			mut(chartFile(workers), "            {{- range $blank := include \"dev-health.goWorkerDSNNames\" $ | fromJsonArray }}", "            {{- range $blank := list }}"),
			workerDSNBlanksRenderLast, "no optional secret reference or no blank"},
		{"the-chart-list-loses-a-dsn-name", reader,
			mut(chartFile("templates/_helpers.tpl"), `"API_DATABASE_URI" `, ""),
			func(g *guard) {
				chartDSNListMatchesDSNClass(g, map[string][]string{"API_DATABASE_URI": {"internal/apiservice/service.go"}})
			}, "API_DATABASE_URI is a database DSN secret"},
		{"the-chart-owned-refusal-loses-its-message", reader,
			mut(chartFile(workers), "which the chart already sets for this container", "which is set"),
			workerGroupExtraEnvRefusesChartOwnedNames, "not the chart-owned message"},
		{"a-dsn-name-gets-the-chart-owned-message", reader,
			mut(chartFile(workers), "{{- if has ($entry.name | default \"\") $dsnNames }}", "{{- if false }}"),
			workerGroupExtraEnvRefusesChartOwnedNames, "not the DSN message"},
		{"the-start-guard-stops-refusing-a-dsn-alias", reader, nil,
			func(g *guard) { dsnAliasLimits(g, func(string, []string) error { return nil }) },
			"a DSN URL under an unknown name"},
		{"the-start-guard-starts-refusing-a-libpq-string", reader, nil,
			func(g *guard) {
				dsnAliasLimits(g, func(class string, environ []string) error {
					if strings.Contains(environ[0], "host=") {
						return &dsnclass.Refusal{Class: class}
					}
					return dsnclass.CheckEnviron(class, environ)
				})
			},
			"libpq key=value"},
	})
}

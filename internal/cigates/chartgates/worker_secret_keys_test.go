package chartgates_test

import (
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

// loaderSecretKeys are the key names the deploy loader (deploy repo scripts/load-secrets.sh ALLOW_OPS, its
// DATABASE_URI / GO_API_REGISTRY_POSTGRES_URI / CLICKHOUSE_URI lines and add-ch-native.sh) puts in the shared
// Secret, plus API_DATABASE_URI, which go-api reads from it by secretKeyRef. A key added to the loader needs a row here.
var loaderSecretKeys = []string{
	"ADMIN_API_KEY", "ADMIN_USERS", "API_DATABASE_URI", "APP_BASE_URL", "AUTH_AUTO_CREATE_ORG_ON_REGISTER",
	"CLICKHOUSE_NATIVE_URI", "CLICKHOUSE_URI", "CORS_ALLOWED_ORIGINS", "DATABASE_URI", "DOMAIN", "EMAIL_API_KEY",
	"EMAIL_FROM_ADDRESS", "EMAIL_PROVIDER", "EXPECTED_WORKER_GROUPS", "GITHUB_APP_CALLBACK_URL", "GITHUB_APP_CLIENT_ID",
	"GITHUB_APP_CLIENT_SECRET", "GITHUB_APP_PRIVATE_KEY", "GITHUB_APP_SLUG", "GITHUB_WEBHOOK_SECRET",
	"GO_API_REGISTRY_POSTGRES_URI", "GOOGLE_CLIENT_ID", "GOOGLE_CLIENT_SECRET", "INVESTMENT_LLM_BATCH_MODE",
	"INVESTMENT_LLM_BATCH_TIMEOUT_SECONDS", "JWT_SECRET_KEY", "LICENSE_PRIVATE_KEY", "LICENSE_PUBLIC_KEY",
	"LINEAR_API_KEY", "LLM_MODEL", "LLM_PROVIDER", "OPENAI_API_KEY", "PAGER_DUTY_CLIENT_ID", "PAGER_DUTY_REDIRECT_URI",
	"PAGER_DUTY_SECRET", "SENTRY_DSN", "SENTRY_ENVIRONMENT", "SENTRY_TRACES_RATE", "SETTINGS_ENCRYPTION_KEY",
	"SOCIAL_GITHUB_CLIENT_ID", "SOCIAL_GITHUB_CLIENT_SECRET", "SOCIAL_GOOGLE_CLIENT_ID", "SOCIAL_GOOGLE_CLIENT_SECRET",
	"STRIPE_SECRET_KEY", "STRIPE_WEBHOOK_SECRET", "TRUSTED_PROXIES", "WORKER_GITHUB_WORK_ITEMS_STATUS_MAPPING_PATH",
}

// notReadByWorkers are the loader keys no worker group reads, each with the reason.
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

// directReaders are the worker keys outside the config registry, with a file that reads the name.
var directReaders = map[string]string{
	"EMAIL_API_KEY":                        "internal/mail/sender.go",
	"EMAIL_FROM_ADDRESS":                   "internal/mail/sender.go",
	"EMAIL_PROVIDER":                       "internal/mail/sender.go",
	"INVESTMENT_LLM_BATCH_MODE":            "internal/jobs/investment/batchsettings.go",
	"INVESTMENT_LLM_BATCH_TIMEOUT_SECONDS": "internal/jobs/investment/batchsettings.go",
	"LINEAR_API_KEY":                       "internal/syncbudget/loader.go",
	"LLM_MODEL":                            "internal/llmbudget/llmbudget.go",
	"LLM_PROVIDER":                         "internal/llmbudget/llmbudget.go",
	"OPENAI_API_KEY":                       "internal/llmbudget/llmbudget.go",
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

func sharedSecretKeysAreClassified(g *guard, loader []string) {
	keys := chartWorkerSecretKeys(g)
	registry := config.EnvNames()
	for _, k := range keys {
		g.expect(!isDSNSecret(k), "goWorkers.secretKeys lists %s, a database DSN secret", k)
		_, direct := directReaders[k]
		g.expect(slices.Contains(registry, k) || direct, "goWorkers.secretKeys lists %s, which neither the config registry nor the direct-reader table knows", k)
		if file, ok := directReaders[k]; ok {
			g.expect(strings.Contains(g.read(file), `"`+k+`"`), "%s does not read %s", file, k)
		}
		g.expect(slices.Contains(loader, k), "goWorkers.secretKeys lists %s, which the deploy loader table does not carry", k)
	}
	for _, k := range loader {
		listed := slices.Contains(keys, k)
		_, notRead := notReadByWorkers[k]
		dsn := isDSNSecret(k)
		n := 0
		for _, b := range []bool{listed, notRead, dsn} {
			if b {
				n++
			}
		}
		g.expect(n == 1, "loader key %s is classified %d times (secretKeys %v, not read by workers %v, database DSN %v); it needs exactly one", k, n, listed, notRead, dsn)
	}
	for k, reason := range notReadByWorkers {
		g.expect(reason != "" && slices.Contains(loader, k), "%s is in the not-read table without a reason or outside the loader table", k)
	}
	for _, svc := range workerServices {
		for _, queues := range []bool{true, false} {
			for _, o := range config.OptionsFor(svc, queues) {
				if o.Env == "" || !slices.Contains(loader, o.Env) || isDSNSecret(o.Env) {
					continue
				}
				_, notRead := notReadByWorkers[o.Env]
				g.expect(slices.Contains(keys, o.Env) || notRead, "%s applies to %s and is in neither goWorkers.secretKeys nor the not-read table", o.Env, svc)
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

func unquoted(in []string) []string {
	out := make([]string, len(in))
	for i, s := range in {
		out[i] = strings.Trim(s, `"`)
	}
	return out
}

// Every database DSN name known to the code is in the chart list, apart from the names the chart itself renders
// into a worker group from its own pooler Secret.
func chartDSNListMatchesDSNClass(g *guard) {
	listed := unquoted(chartDSNNames(g))
	g.expect(len(listed) > 0, "the chart DSN list is empty, so the check measured nothing")
	ownRendered := []string{"POSTGRES_URI"}
	census := slices.Concat(loaderSecretKeys, config.EnvNames(), []string{"COORDINATOR_DATABASE_URI", "WORKER_DATABASE_URI"})
	for _, k := range census {
		if isDSNSecret(k) && !slices.Contains(ownRendered, k) {
			g.expect(slices.Contains(listed, k), "%s is a database DSN secret by dsnclass and is not in the chart DSN list", k)
		}
	}
	for _, k := range listed {
		g.expect(isDSNSecret(k), "the chart DSN list names %s, which dsnclass does not classify as a database DSN secret", k)
	}
}

// goWorkers.secretKeys naming a database DSN refuses the render: by the template (schema validation skipped) and by the schema.
func workerSecretKeysRefuseDSNNames(g *guard) {
	for _, name := range unquoted(chartDSNNames(g)) {
		override := set("goWorkers.secretKeys={APP_BASE_URL," + name + "}")
		out := g.refusal("template refusal of "+name, "worker-secret-keys-dsn-tpl", join(poolGoValues, override, []string{"--skip-schema-validation"})...)
		g.expect(strings.Contains(out, "goWorkers.secretKeys lists "+name+", a database DSN name"), "the template refusal of %s says %q", name, firstLines(out, 2))
		g.refusal("schema refusal of "+name, "worker-secret-keys-dsn-schema", join(poolGoValues, override)...)
	}
}

// The schema refuses the same names, so a values check that runs the schema fails early; the template stays the authority.
func workerSecretKeysSchemaListsChartDSNNames(g *guard) {
	items := at(g.yamlFile(chartFile("values.schema.json")), "properties", "goWorkers", "properties", "secretKeys", "items", "not", "enum")
	got := strs(items)
	want := unquoted(chartDSNNames(g))
	sort.Strings(got)
	sort.Strings(want)
	g.expect(len(want) > 0 && slices.Equal(got, want), "values.schema.json refuses %v in goWorkers.secretKeys, the chart DSN list is %v", got, want)
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
		check{"shared-secret-keys-are-classified", func(g *guard) { sharedSecretKeysAreClassified(g, loaderSecretKeys) }},
		check{"chart-dsn-list-matches-dsnclass", chartDSNListMatchesDSNClass},
		check{"worker-secret-keys-refuse-dsn-names", workerSecretKeysRefuseDSNNames},
		check{"worker-secret-keys-schema-lists-chart-dsn-names", workerSecretKeysSchemaListsChartDSNNames},
		check{"worker-dsn-blanks-render-last", workerDSNBlanksRenderLast},
	)
}

func TestWorkerSecretKeysGuardsPlantedDefects(t *testing.T) {
	const workers = "templates/go-workers.yaml"
	const values = "values.yaml"
	reader := []string{chartRel, deploymentJSON, "internal/mail/sender.go", "internal/jobs/investment/batchsettings.go", "internal/syncbudget/loader.go", "internal/llmbudget/llmbudget.go"}
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
			func(g *guard) { sharedSecretKeysAreClassified(g, loaderSecretKeys) }, "a database DSN secret"},
		{"a-worker-key-is-dropped-from-the-list", reader,
			mut(chartFile(values), "    - EMAIL_API_KEY\n", ""),
			func(g *guard) { sharedSecretKeysAreClassified(g, loaderSecretKeys) }, "loader key EMAIL_API_KEY is classified 0 times"},
		{"the-loader-gains-an-unclassified-key", reader, nil,
			func(g *guard) {
				sharedSecretKeysAreClassified(g, append(slices.Clone(loaderSecretKeys), "A_NEW_SECRET_KEY"))
			},
			"loader key A_NEW_SECRET_KEY is classified 0 times"},
		{"the-template-stops-refusing-dsn-names", reader,
			all(mut(chartFile(workers), "{{- fail (printf \"goWorkers.secretKeys lists", "{{- $_ := (printf \"goWorkers.secretKeys lists"),
				mut(chartFile("values.schema.json"), `"not": { "enum": [`, `"x-not": { "enum": [`)),
			workerSecretKeysRefuseDSNNames, "must be refused"},
		{"the-schema-stops-refusing-dsn-names", reader,
			mut(chartFile("values.schema.json"), `"not": { "enum": [`, `"x-not": { "enum": [`),
			workerSecretKeysSchemaListsChartDSNNames, "values.schema.json refuses"},
		{"the-blanks-are-gone", reader,
			mut(chartFile(workers), "            {{- range $blank := include \"dev-health.goWorkerDSNNames\" $ | fromJsonArray }}", "            {{- range $blank := list }}"),
			workerDSNBlanksRenderLast, "no optional secret reference or no blank"},
		{"the-chart-list-loses-a-dsn-name", reader,
			mut(chartFile("templates/_helpers.tpl"), `"API_DATABASE_URI" `, ""),
			chartDSNListMatchesDSNClass, "API_DATABASE_URI is a database DSN secret"},
		{"a-listed-key-has-no-reader", reader,
			mut("internal/syncbudget/loader.go", `"LINEAR_API_KEY"`, `"LINEAR_API_KEY_RENAMED"`),
			func(g *guard) { sharedSecretKeysAreClassified(g, loaderSecretKeys) }, "does not read LINEAR_API_KEY"},
	})
}

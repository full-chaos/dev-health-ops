package bigboy_test

import (
	"regexp"
	"strings"
	"testing"
)

var (
	composeACRAll   = []string{"acr-api", "acr-db-acl", "acr-db-init", "acr-falkordb", "acr-mcp", "acr-migrate", "acr-postgres", "acr-projector"}
	composeACRVenue = []string{"acr-db-acl", "acr-db-init", "acr-migrate", "acr-postgres"}
	composeACRUp    = []string{"acr-postgres", "acr-db-init", "acr-migrate", "acr-db-acl", "acr-falkordb", "acr-api", "acr-projector", "acr-mcp"}
	composeACRPins  = []string{"BIGBOY_ACR_API_IMAGE", "BIGBOY_ACR_MCP_IMAGE", "BIGBOY_ACR_POSTGRES_IMAGE", "BIGBOY_ACR_NGINX_IMAGE", "BIGBOY_ACR_FALKORDB_IMAGE"}
)

const composeSecretsDir = "/var/lib/oci-cache/mcp-venue/secrets/"

func composeACR(g *guard) composeOut { return composeMust(g, composeIn{profiles: []string{"acr"}}) }

func composeNetworks(s doc) []string { return composeSorted(mp(s["networks"])) }

func composeACRDefaultStack(g *guard) {
	got := composeSorted(composeMust(g, composeIn{}).services)
	g.expect(equal(got, composeSortedStrs(composeStackServices)), "the default stack renders %v, want %v (no acr service)", got, composeStackServices)
}

func composeACRProfilesAddOnlyACR(g *guard) {
	base := composeMust(g, composeIn{})
	for _, c := range []struct {
		profiles []string
		want     []string
	}{
		{[]string{"acr"}, composeACRAll},
		{[]string{"acr-venue"}, composeACRVenue},
		{[]string{"acr", "acr-venue"}, composeACRAll},
	} {
		got := composeAdded(base, composeMust(g, composeIn{profiles: c.profiles}))
		g.expect(equal(got, c.want), "profiles %v add %v, want %v", c.profiles, got, c.want)
	}
}

func composeACRDataHostPaths(g *guard) {
	r := composeACR(g)
	binds := map[string]string{}
	for name, s := range r.services {
		if !strings.HasPrefix(name, "acr-") {
			continue
		}
		for _, v := range list(s["volumes"]) {
			if str(at(v, "type")) == "bind" {
				binds[name+" "+str(at(v, "target"))] = str(at(v, "source"))
			}
		}
	}
	for key, want := range map[string]string{
		"acr-postgres /var/lib/postgresql":    "/var/lib/oci-cache/mcp-venue/postgres-data",
		"acr-falkordb /var/lib/falkordb/data": "/var/lib/oci-cache/mcp-venue/falkordb-data",
	} {
		g.expect(binds[key] == want, "bind %s has source %q, want the old project's host path %q", key, binds[key], want)
	}
}

func composeACRContainerNames(g *guard) {
	r := composeACR(g)
	want := map[string]string{
		"acr-api": "dev-health-acr-mcp-acr-api-1", "acr-db-acl": "dev-health-acr-mcp-acr-db-acl-1",
		"acr-db-init":  "dev-health-acr-mcp-acr-db-init-1",
		"acr-falkordb": "dev-health-acr-mcp-falkordb-1", "acr-mcp": "dev-health-acr-mcp-acr-mcp-1",
		"acr-migrate": "dev-health-acr-mcp-acr-migrate-1", "acr-postgres": "dev-health-acr-mcp-postgres-1",
		"acr-projector": "dev-health-acr-mcp-acr-projector-1",
	}
	for _, n := range composeACRAll {
		got := str(r.services[n]["container_name"])
		g.expect(got == want[n], "%s container_name is %q, want %q", n, got, want[n])
	}
	for svc, alias := range map[string]string{"acr-postgres": "postgres", "acr-falkordb": "falkordb"} {
		got := strs(at(r.services[svc], "networks", "acr", "aliases"))
		g.expect(equal(got, []string{alias}), "%s answers on the acr network as %v, want [%s]", svc, got, alias)
	}
	_, onACR := mp(r.services["postgres"]["networks"])["acr"]
	g.expect(!onACR, "the dev-health postgres is attached to the acr network")
}

var composeQueryURLRe = regexp.MustCompile(`^http://query-api:\d+$`)

func composeACRNetworks(g *guard) {
	r := composeACR(g)
	for _, n := range composeACRAll {
		want := []string{"acr"}
		switch n {
		case "acr-mcp":
			want = []string{"acr", "acr-edge"}
		case "acr-api":
			want = []string{"acr", "acr-edge", "acr_auth_workload", "dho-clickhouse-readers", "dho-query-internal"}
		case "acr-projector":
			want = []string{"acr", "dho-clickhouse-readers"}
		}
		got := composeNetworks(r.services[n])
		g.expect(equal(got, want), "%s is on networks %v, want %v", n, got, want)
		for _, net := range got {
			g.expect(!strings.Contains(net, "dev-health"), "%s is attached to the ops network %s", n, net)
		}
	}
	env := mp(r.services["acr-api"]["environment"])
	for _, k := range []string{"ACR_DATA_QUERY_URL", "ACR_DATA_GRAPHQL_URL"} {
		g.expect(composeQueryURLRe.MatchString(str(env[k])), "acr-api %s=%q is not http://query-api:<port>", k, str(env[k]))
	}
}

func composeOnNetwork(s doc, net string) bool {
	if l, ok := s["networks"].([]any); ok {
		for _, n := range l {
			if str(n) == net {
				return true
			}
		}
		return false
	}
	_, on := mp(s["networks"])[net]
	return on
}

// The edge traefik is not in this file: it joins acr-edge from the host compose file of traefik (README "Staging acr host").
// In this file acr-api and acr-mcp are the only members, so the edge reaches them and nothing else of acr; acr-api publishes no port.
func composeACREdgeNetwork(g *guard) {
	r := composeACR(g)
	net := mp(at(r.top, "networks", "acr-edge"))
	g.expect(str(net["name"]) == "dev-health-acr-edge" && str(net["driver"]) == "bridge", "the network acr-edge is %v, want a bridge named dev-health-acr-edge", net)
	var on []string
	for name, s := range mp(r.top["services"]) {
		if composeOnNetwork(mp(s), "acr-edge") {
			on = append(on, name)
		}
	}
	on = composeSortedStrs(on)
	g.expect(equal(on, []string{"acr-api", "acr-mcp"}), "the network acr-edge holds %v, want only acr-api and acr-mcp", on)
}

func composeLabels(pairs ...[2]string) doc {
	out := doc{}
	for _, kv := range pairs {
		out[kv[0]] = kv[1]
	}
	return out
}

// The mcp router admits exactly the five rules of the k3s ingress dev-health-acr-mcp it replaces.
var composeACREdgeLabels = map[string]doc{
	"acr-api": composeLabels(
		[2]string{"traefik.docker.network", "dev-health-acr-edge"},
		[2]string{"traefik.enable", "true"},
		[2]string{"traefik.http.routers.acr-staging.entrypoints", "web"},
		[2]string{"traefik.http.routers.acr-staging.rule", "Host(`acr.commanderkeen.dev`)"},
		[2]string{"traefik.http.services.acr-staging.loadbalancer.server.port", "8080"},
		[2]string{"traefik.scope", "dev-health"},
	),
	"acr-mcp": composeLabels(
		[2]string{"traefik.docker.network", "dev-health-acr-edge"},
		[2]string{"traefik.enable", "true"},
		[2]string{"traefik.http.routers.mcp-staging.entrypoints", "web"},
		[2]string{"traefik.http.routers.mcp-staging.rule", "Host(`mcp.commanderkeen.dev`) && (PathPrefix(`/mcp`) || Path(`/healthz`) || Path(`/readyz`) || Path(`/`) || PathPrefix(`/.well-known/oauth-protected-resource`))"},
		[2]string{"traefik.http.services.mcp-staging.loadbalancer.server.port", "8081"},
		[2]string{"traefik.scope", "dev-health"},
	),
}

func composeACREdgeRoute(g *guard) {
	r := composeACR(g)
	for _, n := range composeACRAll {
		got := mp(r.services[n]["labels"])
		if want, routed := composeACREdgeLabels[n]; routed {
			g.expect(equal(got, want), "%s traefik labels are %v, want exactly %v", n, got, want)
			continue
		}
		for k := range got {
			g.expect(!strings.HasPrefix(k, "traefik."), "%s carries the traefik label %s: only acr-api and acr-mcp are routed by the edge traefik", n, k)
		}
	}
}

func composeACRPorts(g *guard) {
	r := composeACR(g)
	var published []string
	for _, n := range composeACRAll {
		if len(list(r.services[n]["ports"])) > 0 {
			published = append(published, n)
		}
	}
	g.expect(equal(published, []string{"acr-falkordb", "acr-mcp"}), "acr services publishing a port: %v, want only acr-falkordb and acr-mcp", published)
	for _, c := range []struct {
		svc       string
		published string
		target    int
	}{{"acr-mcp", "8081", 8081}, {"acr-falkordb", "16379", 6379}} {
		ports := list(r.services[c.svc]["ports"])
		if len(ports) != 1 {
			g.failf("%s publishes %d ports, want 1", c.svc, len(ports))
			continue
		}
		got := []any{at(ports[0], "host_ip"), at(ports[0], "published"), at(ports[0], "target")}
		g.expect(equal(got, []any{"127.0.0.1", c.published, c.target}), "%s publishes %v, want [127.0.0.1 %s %d] (loopback only)", c.svc, got, c.published, c.target)
	}
}

func composeSecretNames(s doc) map[string]bool {
	out := map[string]bool{}
	for _, v := range list(s["secrets"]) {
		out[str(at(v, "source"))] = true
	}
	return out
}

func composeACRFalkorAuth(g *guard) {
	r := composeACR(g)
	fk := r.services["acr-falkordb"]
	args := str(mp(fk["environment"])["REDIS_ARGS"])
	g.expect(args == "--aclfile /run/secrets/acr_falkordb_acl", "acr-falkordb REDIS_ARGS=%q, want --aclfile /run/secrets/acr_falkordb_acl", args)
	for _, sec := range []string{"acr_falkordb_acl", "acr_falkordb_password"} {
		g.expect(composeSecretNames(fk)[sec], "acr-falkordb does not mount the secret %s", sec)
	}
	test := strings.Join(strs(at(fk, "healthcheck", "test")), " ")
	g.expect(strings.Contains(test, "REDISCLI_AUTH=") && strings.Contains(test, "/run/secrets/acr_falkordb_password"), "the acr-falkordb healthcheck does not read the password from the secret file: %q", test)
	for _, n := range []string{"acr-api", "acr-projector"} {
		got := str(mp(r.services[n]["environment"])["ACR_CONTEXT_FABRIC_FALKOR_PASSWORD_FILE"])
		g.expect(got == "/run/secrets/acr_falkordb_password", "%s ACR_CONTEXT_FABRIC_FALKOR_PASSWORD_FILE=%q, want /run/secrets/acr_falkordb_password", n, got)
		g.expect(composeSecretNames(r.services[n])["acr_falkordb_password"], "%s does not mount the secret acr_falkordb_password", n)
	}
	for _, n := range composeACRAll {
		if mp(r.services[n]["environment"])["ACR_CONTEXT_FABRIC_FALKOR_ADDR"] != nil && n != "acr-api" && n != "acr-projector" {
			g.failf("%s reaches falkordb but has no password wiring in this guard", n)
		}
	}
	for _, sec := range []string{"acr_falkordb_acl", "acr_falkordb_password"} {
		file := str(at(r.top, "secrets", sec, "file"))
		g.expect(strings.HasPrefix(file, composeSecretsDir), "secret %s reads %q, outside %s", sec, file, composeSecretsDir)
	}
}

var (
	composeURLCredRe   = regexp.MustCompile(`://[^/\s]*:[^/\s@]*@`)
	composeSecretKeyRe = regexp.MustCompile(`(PASSWORD|KEY|TOKEN|DSN|KEYS)$`)
)

func composeACRSecretsAreFiles(g *guard) {
	r := composeACR(g)
	for name, sec := range mp(r.top["secrets"]) {
		if strings.HasPrefix(name, "acr_") {
			file := str(at(sec, "file"))
			g.expect(strings.HasPrefix(file, composeSecretsDir), "secret %s reads %q, outside %s", name, file, composeSecretsDir)
		}
	}
	for _, n := range composeACRAll {
		for k, v := range mp(r.services[n]["environment"]) {
			g.expect(!composeURLCredRe.MatchString(str(v)), "%s %s carries a credential in a URL", n, k)
			if composeSecretKeyRe.MatchString(k) {
				g.expect(strings.HasPrefix(str(v), "/run/secrets/"), "%s %s is a secret value, not a /run/secrets/ file path", n, k)
			}
		}
	}
}

var (
	composeACROwnPinRe = regexp.MustCompile(`^ghcr\.io/full-chaos/dev-health-acr/acr-(api|mcp)@sha256:[0-9a-f]{64}$`)
	composeDigestEndRe = regexp.MustCompile(`@sha256:[0-9a-f]{64}$`)
)

func composeACRImagesPinned(g *guard) {
	r := composeACR(g)
	pins := composePinsMap(g.read(composePins))
	for _, c := range []struct {
		pin   string
		own   bool
		names []string
	}{
		{"BIGBOY_ACR_API_IMAGE", true, []string{"acr-api", "acr-migrate", "acr-projector"}},
		{"BIGBOY_ACR_MCP_IMAGE", true, []string{"acr-mcp"}},
		{"BIGBOY_ACR_POSTGRES_IMAGE", false, []string{"acr-postgres", "acr-db-init", "acr-db-acl"}},
		{"BIGBOY_ACR_NGINX_IMAGE", false, nil}, // auth-issuer (profile auth): compose_auth_venue_test.go
		{"BIGBOY_ACR_FALKORDB_IMAGE", false, []string{"acr-falkordb"}},
	} {
		re := composeDigestEndRe
		if c.own {
			re = composeACROwnPinRe
		}
		g.expect(re.MatchString(pins[c.pin]), "pin %s=%q is not pinned by registry digest", c.pin, pins[c.pin])
		for _, n := range c.names {
			got := str(r.services[n]["image"])
			g.expect(got == pins[c.pin], "%s renders image %q, want the %s pin", n, got, c.pin)
		}
	}
	for _, pin := range composeACRPins {
		for _, p := range [][]string{nil, {"acr"}} {
			out := composeRender(g, composeIn{profiles: p, pins: composeDropLine(pin + "=")})
			g.expect(out.err != "" && strings.Contains(out.err, pin) && strings.Contains(out.err, "is required"),
				"without the %s pin the render (profiles %v) does not fail naming it as required: %q", pin, p, out.err)
		}
	}
}

func composeACRStartOrder(g *guard) {
	r := composeACR(g)
	cond := func(n string) map[string]string {
		out := map[string]string{}
		for d, v := range mp(r.services[n]["depends_on"]) {
			out[d] = str(at(v, "condition"))
		}
		return out
	}
	healthy, done := "service_healthy", "service_completed_successfully"
	for n, want := range map[string]map[string]string{
		"acr-db-init":   {"acr-postgres": healthy},
		"acr-migrate":   {"acr-postgres": healthy, "acr-db-init": done},
		"acr-db-acl":    {"acr-postgres": healthy, "acr-migrate": done},
		"acr-api":       {"acr-postgres": healthy, "acr-db-acl": done},
		"acr-projector": {"acr-postgres": healthy, "acr-db-acl": done},
		"acr-mcp":       {"acr-api": "service_started"},
	} {
		got := cond(n)
		g.expect(equal(got, want), "%s depends_on %v, want %v", n, got, want)
	}
}

func composeACRTalksToRealOps(g *guard) {
	r := composeACR(g)
	api := r.services["acr-api"]
	env := mp(api["environment"])
	g.expect(str(env["ACR_DEV_HEALTH_ENTITLEMENT_URL"]) == "http://go-api:8091", "acr-api ACR_DEV_HEALTH_ENTITLEMENT_URL=%q, want http://go-api:8091", str(env["ACR_DEV_HEALTH_ENTITLEMENT_URL"]))
	g.expect(str(env["ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL"]) == "workload", "acr-api ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL=%q, want workload", str(env["ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL"]))
	_, tokenFile := env["ACR_DEV_HEALTH_ENTITLEMENT_TOKEN_FILE"]
	g.expect(!tokenFile, "acr-api still names a service token file (ACR_DEV_HEALTH_ENTITLEMENT_TOKEN_FILE)")
	for k, want := range map[string]string{
		"ACR_AUTH_WORKLOAD_EXCHANGE_URL":     "http://auth-service:8095",
		"ACR_AUTH_WORKLOAD_ALLOW_PLAIN_HTTP": "true",
		"ACR_AUTH_WORKLOAD_ISSUER":           composeAuthIssuer,
		"ACR_AUTH_WORKLOAD_BOOTSTRAP_FILE":   "/run/workload/acr-api.bootstrap",
	} {
		g.expect(str(env[k]) == want, "acr-api %s=%q, want %q", k, str(env[k]), want)
	}
	_, subjectFile := env["ACR_AUTH_WORKLOAD_SUBJECT_FILE"]
	g.expect(!subjectFile, "acr-api names ACR_AUTH_WORKLOAD_SUBJECT_FILE too: the bootstrap file is the one subject")
	var secrets []string
	for _, s := range list(api["secrets"]) {
		secrets = append(secrets, str(at(s, "source")))
	}
	g.expect(!composeSet(secrets...)["acr_ops_svc_token"], "acr-api still mounts the secret acr_ops_svc_token (mounts %v)", secrets)
	var bootstrap bool
	for _, v := range list(api["volumes"]) {
		if str(at(v, "source")) == "acr_workload_bootstrap" {
			bootstrap = str(at(v, "type")) == "volume" && str(at(v, "target")) == "/run/workload" && at(v, "read_only") == true
		}
	}
	g.expect(bootstrap, "acr-api does not mount the volume acr_workload_bootstrap read-only at /run/workload")
	_, standin := composeMust(g, composeIn{profiles: []string{"acr", "acr-venue", "auth"}}).services["acr-entitlement-standin"]
	g.expect(!standin, "the entitlement stand-in is still in the file: prod has none, and acr-api sends a workload token")
	_, standinSecret := mp(r.top["secrets"])["acr_entitlement_token"]
	g.expect(!standinSecret, "the file still declares the stand-in's secret acr_entitlement_token")
	_, retries := env["ACR_CONTEXT_FABRIC_MODEL_MAX_TRANSPORT_RETRIES"]
	g.expect(!retries, "acr-api sets the removed ACR_CONTEXT_FABRIC_MODEL_MAX_TRANSPORT_RETRIES")
	_, svcSecret := mp(r.top["secrets"])["acr_ops_svc_token"]
	g.expect(!svcSecret, "the file still declares the secret acr_ops_svc_token")
	text := g.read(composeFile)
	for _, gone := range []string{"svc_acr_", "acr_ops_svc_token", "ops_svc_acr_token", "ACR_OPS_SVC_TOKEN_FILE"} {
		g.expect(!strings.Contains(text, gone), "compose.bigboy.yml still names %s: the venue keeps no svc_acr_ service token", gone)
	}
	_, apiNet := mp(api["networks"])["dho-query-internal"]
	_, goNet := mp(composeMust(g, composeIn{}).services["go-api"]["networks"])["dho-query-internal"]
	g.expect(apiNet && goNet, "acr-api and go-api do not share the network dho-query-internal (acr-api %v, go-api %v)", apiNet, goNet)
}

const composeLLMSource = "ACR_CONTEXT_FABRIC_ORG_LLM_CONFIG_SOURCE"

func composeACROrgLLMSourceIsGone(g *guard) {
	for _, line := range strings.Split(g.read(composePins), "\n") {
		g.expect(!strings.HasPrefix(line, composeLLMSource), "images.env sets %s, which acr no longer reads (CHAOS-12997)", composeLLMSource)
	}
	for _, name := range []string{"acr-api", "acr-projector", "acr-mcp"} {
		_, passed := mp(composeACR(g).services[name]["environment"])[composeLLMSource]
		g.expect(!passed, "%s passes %s, which acr no longer reads (CHAOS-12997)", name, composeLLMSource)
	}
}

var (
	composeReadmeCmdRe = regexp.MustCompile("`([^`]*docker [^`]+)`")
	composeDownRe      = regexp.MustCompile(`\bdown\b`)
	composeStopRe      = regexp.MustCompile(`\bstop\b|\bkill\b`)
	composeDownBadRe   = regexp.MustCompile(`\s-v\b|--volumes|--rmi`)
	composeRepoCountRe = regexp.MustCompile(`GRAPH\.QUERY <graph> "MATCH \(r:Repository\) RETURN count\(r\)"`)
)

func composeACRSwitchProcedure(g *guard) {
	readme := g.read("ci/bigboy/README.md")
	start := strings.Index(readme, "The one-time move from the old project")
	if start < 0 {
		g.failf("README.md has no section on the one-time move from the old project")
		return
	}
	section := readme[start:]
	if end := strings.Index(section, "\n## "); end >= 0 {
		section = section[:end]
	}
	idx := func(s string) int { return strings.Index(section, s) }
	var commands, downs, ups []string
	for _, m := range composeReadmeCmdRe.FindAllStringSubmatch(section, -1) {
		commands = append(commands, m[1])
		if composeDownRe.MatchString(m[1]) {
			downs = append(downs, m[1])
		}
		if strings.Contains(m[1], "--profile acr up") {
			ups = append(ups, m[1])
		}
		g.expect(!composeStopRe.MatchString(m[1]), "the switch procedure stops or kills instead of removing: %s", m[1])
	}
	if len(downs) != 1 || len(ups) != 1 {
		g.failf("the switch procedure has %d down and %d acr up commands, want one each: %v", len(downs), len(ups), commands)
		return
	}
	down, up := downs[0], ups[0]
	g.expect(strings.Contains(down, "-p dev-health-acr-mcp ") && strings.Contains(down, "-f compose.yml"), "the down is not of the old project dev-health-acr-mcp compose.yml: %s", down)
	g.expect(!composeDownBadRe.MatchString(down), "the down removes data or images (-v, --volumes or --rmi): %s", down)
	g.expect(idx(down) < idx("--profile acr up -d"), "the down of the old project does not come before the new up")
	g.expect(strings.Contains(section, "REMOVED, not only stopped"), "the switch procedure lost 'REMOVED, not only stopped'")
	_, names, _ := strings.Cut(up, " up -d --no-build ")
	g.expect(equal(strings.Fields(names), composeACRUp), "the up starts %v, want exactly the nine acr services in start order %v", strings.Fields(names), composeACRUp)
	g.expect(idx("ACR_CONTEXT_FABRIC_PROJECTION_ENABLED=true") >= 0 && idx("ACR_CONTEXT_FABRIC_PROJECTION_ENABLED=true") < idx(up), "the projection switch is not named before the up")
	g.expect(idx("BGSAVE") >= 0 && idx("BGSAVE") < idx(down), "the graph is not saved (BGSAVE) before the down")
	g.expect(strings.Contains(section, "falkordb-data.backup-"), "the switch procedure lost the falkordb-data.backup- copy")
	g.expect(idx("--profile acr up -d") < idx("docker inspect --format"), "the images are not checked (docker inspect) after the up")
	counts := composeRepoCountRe.FindAllStringIndex(section, -1)
	g.expect(len(counts) == 2 && counts[0][0] < idx(down) && idx(down) < idx("--profile acr up -d") && idx("--profile acr up -d") < counts[1][0],
		"the repository count is not read before the down and again after the up: %v", counts)
	g.expect(strings.Contains(section, "docker volume rm dev-health-acr-mcp_acr_entitlement_tmp"), "the switch procedure lost the removal of the old entitlement volume")
}

func composeACROldDefaults(g *guard) {
	ops := "POSTGRES_USER=devhealth\n"
	r := composeMust(g, composeIn{profiles: []string{"acr"}, opsEnv: ops})
	for _, n := range []string{"acr-postgres", "acr-db-init"} {
		got := mp(r.services[n]["environment"])["POSTGRES_USER"]
		g.expect(got == "acr_admin", "%s POSTGRES_USER=%q with ops/.env setting the ops user, want acr_admin", n, got)
	}
	const key = "ACR_CONTEXT_FABRIC_PROJECTION_ENABLED"
	got := mp(r.services["acr-projector"]["environment"])[key]
	g.expect(got == "true", "acr-projector %s=%q from the tracked env file, want true", key, got)
	off := composeMust(g, composeIn{profiles: []string{"acr"}, opsEnv: ops, pins: composeDropLine(key)})
	got = mp(off.services["acr-projector"]["environment"])[key]
	g.expect(got == "false", "acr-projector %s=%q without the env-file line, want the old default false", key, got)
}

func composeACRDBInitScriptIsPinned(g *guard) {
	r := composeACR(g)
	pins := composeEnvFile(g.read(composePins))
	want := pins["ACR_DB_INIT_SCRIPT"]
	g.expect(strings.HasPrefix(want, "/var/lib/oci-cache/mcp-venue/") && strings.HasSuffix(want, ".sh"), "images.env ACR_DB_INIT_SCRIPT=%q is not a script path under /var/lib/oci-cache/mcp-venue/", want)
	for _, n := range []string{"acr-db-init", "acr-db-acl"} {
		var src []string
		for _, v := range list(r.services[n]["volumes"]) {
			if str(at(v, "target")) == "/usr/local/bin/acr-db-init" {
				src = append(src, str(at(v, "source")))
			}
		}
		g.expect(len(src) == 1 && src[0] == want, "%s mounts the script from %v, want only the ACR_DB_INIT_SCRIPT path %q (no hand-kept checkout)", n, src, want)
	}
	if strings.Contains(g.read(composeFile), "ACR_VENUE_DIR") {
		g.failf("%s still names ACR_VENUE_DIR", composeFile)
	}
	pin := composeEnvFile(g.read("ci/bigboy/acr-db-init.pin"))
	g.expect(regexp.MustCompile(`^[0-9a-f]{40}$`).MatchString(pins["BIGBOY_ACR_COMMIT"]), "images.env BIGBOY_ACR_COMMIT=%q is not a 40-hex acr commit", pins["BIGBOY_ACR_COMMIT"])
	g.expect(pin["ACR_DB_INIT_COMMIT"] == pins["BIGBOY_ACR_COMMIT"], "acr-db-init.pin commit %q != images.env BIGBOY_ACR_COMMIT %q", pin["ACR_DB_INIT_COMMIT"], pins["BIGBOY_ACR_COMMIT"])
	g.expect(regexp.MustCompile(`^[0-9a-f]{64}$`).MatchString(pin["ACR_DB_INIT_SHA256"]), "acr-db-init.pin sha256 %q is not 64 hex", pin["ACR_DB_INIT_SHA256"])
}

var composeACRChecks = []check{
	{"the_default_stack_does_not_start_an_acr_service", composeACRDefaultStack},
	{"each_acr_profile_renders_alone_and_adds_only_acr_services", composeACRProfilesAddOnlyACR},
	{"the_data_stays_on_the_host_paths_of_the_old_project", composeACRDataHostPaths},
	{"container_names_and_aliases_keep_every_acr_hostname", composeACRContainerNames},
	{"the_acr_services_reach_the_networks_they_always_did", composeACRNetworks},
	{"only_the_mcp_server_and_the_graph_publish_a_port_and_only_on_loopback", composeACRPorts},
	{"the_edge_network_acr_edge_holds_only_acr_api_and_acr_mcp", composeACREdgeNetwork},
	{"only_acr_api_and_acr_mcp_are_routed_by_the_edge_traefik_on_the_staging_hosts", composeACREdgeRoute},
	{"falkordb_requires_a_password_and_every_client_reads_it_from_a_secret_file", composeACRFalkorAuth},
	{"no_secret_value_is_in_the_file_only_paths_of_secret_files", composeACRSecretsAreFiles},
	{"the_acr_images_are_pinned_by_registry_digest_and_a_missing_pin_fails_the_render", composeACRImagesPinned},
	{"the_start_order_of_the_acr_stack_is_kept", composeACRStartOrder},
	{"acr_api_talks_to_the_real_ops_and_the_stand_in_is_off_its_path", composeACRTalksToRealOps},
	{"the_removed_org_llm_config_source_is_not_passed_and_no_file_sets_it", composeACROrgLLMSourceIsGone},
	{"the_switch_procedure_removes_the_old_containers_and_keeps_the_data", composeACRSwitchProcedure},
	{"the_acr_settings_keep_the_old_defaults_where_the_cutover_changes_the_env", composeACROldDefaults},
	{"the_acr_db_init_script_is_the_one_of_the_pinned_acr_commit", composeACRDBInitScriptIsPinned},
}

func TestComposeACRVenueReal(t *testing.T) { runReal(t, composeACRChecks...) }

func TestComposeACRVenuePlanted(t *testing.T) {
	readme := []string{"ci/bigboy/README.md"}
	cases := []planted{
		{name: "acr-mcp in the default stack", mutate: mut(composeFile, "      published: '8081'\n      protocol: tcp\n    profiles: [acr]\n", "      published: '8081'\n      protocol: tcp\n"),
			check: composeACRDefaultStack, want: "acr-mcp"},
		{name: "falkordb joins acr-venue", mutate: mut(composeFile, "      protocol: tcp\n    profiles: [acr]\n    restart: unless-stopped\n    secrets: [acr_falkordb_acl", "      protocol: tcp\n    profiles: [acr, acr-venue]\n    restart: unless-stopped\n    secrets: [acr_falkordb_acl"),
			check: composeACRProfilesAddOnlyACR, want: "profiles [acr-venue] add"},
		{name: "falkordb data moves", mutate: mut(composeFile, "source: /var/lib/oci-cache/mcp-venue/falkordb-data", "source: /var/lib/oci-cache/falkordb-new"),
			check: composeACRDataHostPaths, want: "acr-falkordb /var/lib/falkordb/data"},
		{name: "postgres container renamed", mutate: mut(composeFile, "container_name: dev-health-acr-mcp-postgres-1", "container_name: dev-health-acr-postgres-1"),
			check: composeACRContainerNames, want: "acr-postgres container_name"},
		{name: "falkordb alias lost", mutate: mut(composeFile, "aliases: [falkordb]", "aliases: [graph]"),
			check: composeACRContainerNames, want: "acr-falkordb answers"},
		{name: "ops postgres on the acr network", mutate: mut(composeFile, "    image: postgres:18-alpine\n    networks:\n      auth_plane: null\n      dev-health: null\n    ports:\n    - ${POSTGRES_PORT", "    image: postgres:18-alpine\n    networks:\n      auth_plane: null\n      dev-health: null\n      acr: null\n    ports:\n    - ${POSTGRES_PORT"),
			check: composeACRContainerNames, want: "dev-health postgres"},
		{name: "acr-api on the ops network", mutate: mut(composeFile, "      acr_auth_workload: null\n      dho-clickhouse-readers: null\n      dho-query-internal: null\n    profiles: [acr]", "      acr_auth_workload: null\n      dho-clickhouse-readers: null\n      dho-query-internal: null\n      dev-health: null\n    profiles: [acr]"),
			check: composeACRNetworks, want: "acr-api is on networks"},
		{name: "acr-projector joins acr-edge", mutate: mut(composeFile, "      acr: null\n      dho-clickhouse-readers: null\n    profiles: [acr]", "      acr: null\n      acr-edge: null\n      dho-clickhouse-readers: null\n    profiles: [acr]"),
			check: composeACREdgeNetwork, want: "the network acr-edge holds"},
		{name: "acr-postgres joins acr-edge", mutate: mut(composeFile, "    networks:\n      acr:\n        aliases: [postgres]\n", "    networks:\n      acr:\n        aliases: [postgres]\n      acr-edge: null\n"),
			check: composeACREdgeNetwork, want: "the network acr-edge holds"},
		{name: "the ops traefik joins acr-edge", mutate: mut(composeFile, "    networks:\n      dev-health: null\n    ports:\n    - mode: ingress\n      protocol: tcp\n      published: '3000'", "    networks:\n      dev-health: null\n      acr-edge: null\n    ports:\n    - mode: ingress\n      protocol: tcp\n      published: '3000'"),
			check: composeACREdgeNetwork, want: "the network acr-edge holds"},
		{name: "acr-edge renamed", mutate: mut(composeFile, "  acr-edge:\n    driver: bridge\n    name: dev-health-acr-edge\n", "  acr-edge:\n    driver: bridge\n    name: dev-health-acr-edge-2\n"),
			check: composeACREdgeNetwork, want: "the network acr-edge is"},
		{name: "acr-projector routed by the edge", mutate: mut(composeFile, "    networks:\n      acr: null\n      dho-clickhouse-readers: null\n    profiles: [acr]", "    labels:\n      traefik.enable: 'true'\n    networks:\n      acr: null\n      dho-clickhouse-readers: null\n    profiles: [acr]"),
			check: composeACREdgeRoute, want: "acr-projector carries the traefik label traefik.enable"},
		{name: "acr-mcp leaves acr-edge", mutate: mut(composeFile, "      acr: null\n      acr-edge: null\n    ports:", "      acr: null\n    ports:"),
			check: composeACREdgeNetwork, want: "the network acr-edge holds"},
		{name: "mcp route admits every path", mutate: mut(composeFile, "(PathPrefix(`/mcp`) || ", "(PathPrefix(`/mcp`) || PathPrefix(`/`) || "),
			check: composeACREdgeRoute, want: "acr-mcp traefik labels"},
		{name: "mcp route loses readyz", mutate: mut(composeFile, " || Path(`/readyz`)", ""),
			check: composeACREdgeRoute, want: "acr-mcp traefik labels"},
		{name: "mcp route healthz by prefix", mutate: mut(composeFile, "Path(`/healthz`)", "PathPrefix(`/healthz`)"),
			check: composeACREdgeRoute, want: "acr-mcp traefik labels"},
		{name: "mcp route without the host", mutate: mut(composeFile, "rule: Host(`mcp.commanderkeen.dev`) && (", "rule: ("),
			check: composeACREdgeRoute, want: "acr-mcp traefik labels"},
		{name: "acr-api route on another host", mutate: mut(composeFile, "traefik.http.routers.acr-staging.rule: Host(`acr.commanderkeen.dev`)", "traefik.http.routers.acr-staging.rule: Host(`acr.commanderkeen.dev`) || Host(`mcp.commanderkeen.dev`)"),
			check: composeACREdgeRoute, want: "acr-api traefik labels"},
		{name: "acr-api route over the ops network", mutate: mut(composeFile, "traefik.docker.network: dev-health-acr-edge", "traefik.docker.network: dev-health_dev-health"),
			check: composeACREdgeRoute, want: "acr-api traefik labels"},
		{name: "acr-api route without the scope", mutate: mut(composeFile, "      traefik.http.services.acr-staging.loadbalancer.server.port: '8080'\n      traefik.scope: dev-health\n", "      traefik.http.services.acr-staging.loadbalancer.server.port: '8080'\n"),
			check: composeACREdgeRoute, want: "acr-api traefik labels"},
		{name: "acr-api gets a second router", mutate: mut(composeFile, "      traefik.http.routers.acr-staging.entrypoints: web\n", "      traefik.http.routers.acr-staging.entrypoints: web\n      traefik.http.routers.acr-any.rule: PathPrefix(`/`)\n"),
			check: composeACREdgeRoute, want: "acr-api traefik labels"},
		{name: "acr-api query url by address", mutate: mut(composeFile, "ACR_DATA_QUERY_URL: http://query-api:8095", "ACR_DATA_QUERY_URL: http://10.199.79.2:8095"),
			check: composeACRNetworks, want: "ACR_DATA_QUERY_URL"},
		{name: "mcp port on every address", mutate: mut(composeFile, "      host_ip: 127.0.0.1\n      target: 8081", "      host_ip: 0.0.0.0\n      target: 8081"),
			check: composeACRPorts, want: "loopback only"},
		{name: "falkordb port on every address", mutate: mut(composeFile, "      host_ip: 127.0.0.1\n      target: 6379", "      host_ip: 0.0.0.0\n      target: 6379"),
			check: composeACRPorts, want: "acr-falkordb publishes"},
		{name: "falkordb port moves", mutate: mut(composeFile, "published: '16379'", "published: '16380'"),
			check: composeACRPorts, want: "acr-falkordb publishes"},
		{name: "falkordb publishes a second port", mutate: mut(composeFile, "      published: '16379'\n      protocol: tcp\n", "      published: '16379'\n      protocol: tcp\n    - mode: ingress\n      host_ip: 127.0.0.1\n      target: 3000\n      published: '13000'\n      protocol: tcp\n"),
			check: composeACRPorts, want: "acr-falkordb publishes 2 ports"},
		{name: "falkordb loses the aclfile", mutate: mut(composeFile, "      REDIS_ARGS: --aclfile /run/secrets/acr_falkordb_acl\n", "      REDIS_ARGS: --save 60\n"),
			check: composeACRFalkorAuth, want: "REDIS_ARGS"},
		{name: "projector loses the falkordb password", mutate: mut(composeFile, "      ACR_CONTEXT_FABRIC_FALKOR_PASSWORD_FILE: /run/secrets/acr_falkordb_password\n      ACR_CONTEXT_FABRIC_FALKOR_TLS: 'false'\n      ACR_CONTEXT_FABRIC_PROJECT_TEAMS", "      ACR_CONTEXT_FABRIC_FALKOR_TLS: 'false'\n      ACR_CONTEXT_FABRIC_PROJECT_TEAMS"),
			check: composeACRFalkorAuth, want: "acr-projector ACR_CONTEXT_FABRIC_FALKOR_PASSWORD_FILE"},
		{name: "api does not mount the falkordb password", mutate: mut(composeFile, "    - acr_openai_api_key\n    - acr_falkordb_password\n", "    - acr_openai_api_key\n"),
			check: composeACRFalkorAuth, want: "acr-api does not mount the secret acr_falkordb_password"},
		{name: "falkordb healthcheck unauthenticated", mutate: mut(composeFile, "[CMD-SHELL, 'REDISCLI_AUTH=\"$$(cat /run/secrets/acr_falkordb_password)\" redis-cli GRAPH.QUERY _acr_health \"RETURN 1\" | grep -q \"^1$$\"']", "[CMD, redis-cli, GRAPH.QUERY, _acr_health, RETURN 1]"),
			check: composeACRFalkorAuth, want: "healthcheck"},
		{name: "falkordb password file outside the secrets dir", mutate: mut(composeFile, "${ACR_FALKORDB_PASSWORD_FILE:-/var/lib/oci-cache/mcp-venue/secrets/falkordb_password}", "${ACR_FALKORDB_PASSWORD_FILE:-/home/ubuntu/falkordb_password}"),
			check: composeACRFalkorAuth, want: "secret acr_falkordb_password reads"},
		{name: "secret file outside the secrets dir", mutate: mut(composeFile, "${ACR_RUNTIME_DSN_FILE:-/var/lib/oci-cache/mcp-venue/secrets/runtime_dsn}", "${ACR_RUNTIME_DSN_FILE:-/home/ubuntu/runtime_dsn}"),
			check: composeACRSecretsAreFiles, want: "secret acr_runtime_dsn"},
		{name: "dsn value inline", mutate: mut(composeFile, "ACR_POSTGRES_MIGRATION_DSN_FILE: /run/secrets/acr_migration_dsn", "ACR_POSTGRES_MIGRATION_DSN: postgres://acr:pw@postgres/acr"),
			check: composeACRSecretsAreFiles, want: "credential in a URL"},
		{name: "acr-db-acl mounts a hand-kept checkout", mutate: mut(composeFile, "${ACR_DB_INIT_SCRIPT:?ACR_DB_INIT_SCRIPT is required (a line of the pins file ci/bigboy/images.env, kept in _records/images.env; acr-db-init-fetch.sh writes the script there at the pinned acr commit)}", "/home/ubuntu/devhealth/dev-health-acr-mcp/deploy/compose/acr-db-init.sh"),
			check: composeACRDBInitScriptIsPinned, want: "mounts the script from"},
		{name: "pin file drifts from images.env", copy: append([]string{"ci/bigboy/acr-db-init.pin"}, composeFiles...), mutate: mut("ci/bigboy/acr-db-init.pin", "ACR_DB_INIT_COMMIT=d168fa95", "ACR_DB_INIT_COMMIT=0000000"),
			check: composeACRDBInitScriptIsPinned, want: "!= images.env BIGBOY_ACR_COMMIT"},
		{name: "migrate runs the mcp image", mutate: mut(composeFile, "    image: ${BIGBOY_ACR_API_IMAGE:?", "    image: ${BIGBOY_ACR_MCP_IMAGE:?"),
			check: composeACRImagesPinned, want: "acr-migrate renders image"},
		{name: "falkordb pin has a default", mutate: mut(composeFile, "${BIGBOY_ACR_FALKORDB_IMAGE:?BIGBOY_ACR_FALKORDB_IMAGE is required", "${BIGBOY_ACR_FALKORDB_IMAGE:-falkordb/falkordb:latest"),
			check: composeACRImagesPinned, want: "without the BIGBOY_ACR_FALKORDB_IMAGE pin"},
		{name: "nginx pin is a tag", mutate: mut(composePins, "BIGBOY_ACR_NGINX_IMAGE=nginx:1.27-alpine@sha256:", "BIGBOY_ACR_NGINX_IMAGE=nginx:1.27-alpine#"),
			check: composeACRImagesPinned, want: "pin BIGBOY_ACR_NGINX_IMAGE"},
		{name: "mcp starts before api is up", mutate: mut(composeFile, "      acr-api:\n        condition: service_started", "      acr-api:\n        condition: service_healthy"),
			check: composeACRStartOrder, want: "acr-mcp depends_on"},
		{name: "the stand-in comes back", mutate: mut(composeFile, "  acr-api:\n    container_name: dev-health-acr-mcp-acr-api-1\n", "  acr-entitlement-standin:\n    image: ${BIGBOY_ACR_NGINX_IMAGE:?BIGBOY_ACR_NGINX_IMAGE is required}\n    profiles: [acr-venue]\n    restart: unless-stopped\n  acr-api:\n    container_name: dev-health-acr-mcp-acr-api-1\n"),
			check: composeACRTalksToRealOps, want: "stand-in is still in the file"},
		{name: "acr-api back on the stand-in", mutate: mut(composeFile, "${ACR_DEV_HEALTH_ENTITLEMENT_URL:-http://go-api:8091}", "${ACR_DEV_HEALTH_ENTITLEMENT_URL:-http://acr-entitlement-standin:8088}"),
			check: composeACRTalksToRealOps, want: "ACR_DEV_HEALTH_ENTITLEMENT_URL"},
		{name: "acr-api back on the service token", mutate: mut(composeFile, "      ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL: workload\n", "      ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL: service_token\n      ACR_DEV_HEALTH_ENTITLEMENT_TOKEN_FILE: /run/secrets/acr_ops_svc_token\n"),
			check: composeACRTalksToRealOps, want: "ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL"},
		{name: "acr-api keeps the token file beside the workload", mutate: mut(composeFile, "      ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL: workload\n", "      ACR_DEV_HEALTH_ENTITLEMENT_CREDENTIAL: workload\n      ACR_DEV_HEALTH_ENTITLEMENT_TOKEN_FILE: /run/secrets/acr_entitlement_token\n"),
			check: composeACRTalksToRealOps, want: "ACR_DEV_HEALTH_ENTITLEMENT_TOKEN_FILE"},
		{name: "acr-api issuer differs", mutate: mut(composeFile, "      ACR_AUTH_WORKLOAD_ISSUER: https://auth.bigboy.internal:8443\n", "      ACR_AUTH_WORKLOAD_ISSUER: https://auth.bigboy.internal\n"),
			check: composeACRTalksToRealOps, want: "ACR_AUTH_WORKLOAD_ISSUER"},
		{name: "acr-api bootstrap mount writable", mutate: mut(composeFile, "      source: acr_workload_bootstrap\n      target: /run/workload\n      read_only: true\n    working_dir: /tmp", "      source: acr_workload_bootstrap\n      target: /run/workload\n    working_dir: /tmp"),
			check: composeACRTalksToRealOps, want: "acr_workload_bootstrap read-only"},
		{name: "the svc token secret comes back", mutate: mut(composeFile, "  acr_falkordb_acl:\n    file:", "  acr_ops_svc_token:\n    file: ${ACR_OPS_SVC_TOKEN_FILE:-/var/lib/oci-cache/mcp-venue/secrets/ops_svc_acr_token}\n  acr_falkordb_acl:\n    file:"),
			check: composeACRTalksToRealOps, want: "acr_ops_svc_token"},
		{name: "retries setting back", mutate: mut(composeFile, "      ACR_CONTEXT_FABRIC_MODEL_MAX_ATTEMPTS: '1'\n", "      ACR_CONTEXT_FABRIC_MODEL_MAX_ATTEMPTS: '1'\n      ACR_CONTEXT_FABRIC_MODEL_MAX_TRANSPORT_RETRIES: '2'\n"),
			check: composeACRTalksToRealOps, want: "MAX_TRANSPORT_RETRIES"},
		{name: "compose passes the removed llm source", mutate: mut(composeFile, "      ACR_CONTEXT_FABRIC_MODEL_PROVIDER: openai\n", "      ACR_CONTEXT_FABRIC_MODEL_PROVIDER: openai\n      ACR_CONTEXT_FABRIC_ORG_LLM_CONFIG_SOURCE: ${ACR_CONTEXT_FABRIC_ORG_LLM_CONFIG_SOURCE:-}\n"),
			check: composeACROrgLLMSourceIsGone, want: "passes ACR_CONTEXT_FABRIC_ORG_LLM_CONFIG_SOURCE"},
		{name: "env file sets the llm source", mutate: mut(composePins, "ACR_CONTEXT_FABRIC_PROJECTION_ENABLED=true", "ACR_CONTEXT_FABRIC_PROJECTION_ENABLED=true\nACR_CONTEXT_FABRIC_ORG_LLM_CONFIG_SOURCE=ops"),
			check: composeACROrgLLMSourceIsGone, want: "images.env sets"},
		{name: "down removes the volumes", copy: readme, mutate: mut("ci/bigboy/README.md", "-f compose.yml down`", "-f compose.yml down -v`"),
			check: composeACRSwitchProcedure, want: "removes data"},
		{name: "up starts the whole profile", copy: readme, mutate: mut("ci/bigboy/README.md", " acr-projector acr-mcp`", " acr-projector`"),
			check: composeACRSwitchProcedure, want: "nine acr services"},
		{name: "no graph save", copy: readme, mutate: mut("ci/bigboy/README.md", "redis-cli BGSAVE", "redis-cli SAVE"),
			check: composeACRSwitchProcedure, want: "BGSAVE"},
		{name: "acr admin is the ops user", mutate: mut(composeFile, "      POSTGRES_DB: ${ACR_DB_NAME:-acr}\n      POSTGRES_PASSWORD_FILE: /run/secrets/acr_postgres_password\n      POSTGRES_USER: ${ACR_POSTGRES_ADMIN_USER:-acr_admin}",
			"      POSTGRES_DB: ${ACR_DB_NAME:-acr}\n      POSTGRES_PASSWORD_FILE: /run/secrets/acr_postgres_password\n      POSTGRES_USER: ${POSTGRES_USER:-acr_admin}"),
			check: composeACROldDefaults, want: "acr-postgres POSTGRES_USER"},
		{name: "projector defaults on", mutate: mut(composeFile, "${ACR_CONTEXT_FABRIC_PROJECTION_ENABLED:-false}", "${ACR_CONTEXT_FABRIC_PROJECTION_ENABLED:-true}"),
			check: composeACROldDefaults, want: "old default false"},
	}
	for i := range cases {
		if cases[i].copy == nil {
			cases[i].copy = composeFiles
		}
	}
	runPlanted(t, cases)
}

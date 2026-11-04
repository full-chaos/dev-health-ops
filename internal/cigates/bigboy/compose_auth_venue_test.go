package bigboy_test

import (
	"regexp"
	"strings"
	"testing"
)

// The auth venue of the bigboy file (CHAOS-12976): auth-service, its one-shots, the acr-api bootstrap pair and the
// issuer origin, in profile auth, and the workload token of acr-api to go-api. The reference shapes are ops compose.yml
// (profile auth), acr deploy/compose/acr-auth-workload.compose.yml and ci/auth_rotation_rehearsal.override.yml (issuer TLS).

const (
	composeAuthIssuer   = "https://auth.bigboy.internal:8443"
	composeAuthIssuerCA = "/run/auth-issuer-ca"
)

var (
	composeAuthAll      = []string{"acr-workload-bootstrap", "acr-workload-volume", "auth-fence-init", "auth-grants", "auth-issuer", "auth-keys-init", "auth-migrate", "auth-owner-grants", "auth-provision", "auth-service", "auth-workload-seed"}
	composeAuthOneShots = []string{"acr-workload-bootstrap", "acr-workload-volume", "auth-fence-init", "auth-grants", "auth-keys-init", "auth-migrate", "auth-owner-grants", "auth-provision", "auth-workload-seed"}
	composeAuthPins     = []string{"BIGBOY_AUTH_MIGRATE_IMAGE", "BIGBOY_AUTH_SERVICE_IMAGE"}
)

func composeAuth(g *guard) composeOut {
	return composeMust(g, composeIn{profiles: []string{"auth", "acr", "acr-venue", "smoke", "venue", "venue-hook", "venue-river"}})
}

func composeAuthProfileAddsOnlyAuth(g *guard) {
	base := composeMust(g, composeIn{})
	got := composeAdded(base, composeMust(g, composeIn{profiles: []string{"auth"}}))
	g.expect(equal(got, composeAuthAll), "profile auth adds %v, want %v", got, composeAuthAll)
	for _, n := range composeAuthAll {
		_, inACR := composeMust(g, composeIn{profiles: []string{"acr"}}).services[n]
		g.expect(!inACR, "%s renders under profile acr: the auth venue is profile auth only", n)
	}
}

func composeAuthIssuerIsOne(g *guard) {
	r := composeAuth(g)
	for _, c := range []struct{ svc, key string }{
		{"auth-service", "AUTH_TOKEN_ISSUER_URL"},
		{"go-api", "AUTH_ACCESS_TOKEN_ISSUER"},
		{"acr-api", "ACR_AUTH_WORKLOAD_ISSUER"},
	} {
		got := str(mp(r.services[c.svc]["environment"])[c.key])
		g.expect(got == composeAuthIssuer, "%s %s=%q, want the one issuer %q", c.svc, c.key, got, composeAuthIssuer)
	}
	front := r.services["auth-issuer"]
	var aliases []string
	for _, a := range list(at(front, "networks", "dev-health", "aliases")) {
		aliases = append(aliases, str(a))
	}
	g.expect(equal(aliases, []string{"auth.bigboy.internal"}), "auth-issuer answers %v on dev-health, want [auth.bigboy.internal] (the issuer host go-api dials)", aliases)
	conf := str(at(r.top, "configs", "auth_issuer_nginx", "content"))
	g.expect(strings.Contains(conf, "listen 8443 ssl;"), "auth-issuer does not listen on 8443 with TLS (the issuer port)")
	locations := regexp.MustCompile(`location ([^{]+)\{`).FindAllStringSubmatch(conf, -1)
	var got []string
	for _, l := range locations {
		got = append(got, strings.TrimSpace(l[1]))
	}
	want := []string{"= /.well-known/openid-configuration", "= /.well-known/jwks.json", "/"}
	g.expect(equal(got, want), "auth-issuer locations %v, want %v (the two public documents only)", got, want)
	g.expect(strings.Contains(conf, "location / { return 404; }"), "auth-issuer serves more than the two documents")
	g.expect(strings.Count(conf, "proxy_pass $auth_upstream;") == 2 && strings.Contains(conf, "set $auth_upstream http://auth-service:8095;"),
		"auth-issuer does not forward exactly the two documents to auth-service:8095")
}

func composeAuthNetworks(g *guard) {
	r := composeAuth(g)
	for n, want := range map[string][]string{
		"auth-service":           {"acr_auth_workload", "auth_plane"},
		"auth-issuer":            {"auth_plane", "dev-health"},
		"auth-provision":         {"auth_plane"},
		"auth-owner-grants":      {"auth_plane"},
		"auth-migrate":           {"auth_plane"},
		"auth-grants":            {"auth_plane"},
		"auth-fence-init":        {"auth_plane"},
		"auth-workload-seed":     {"auth_plane"},
		"acr-workload-bootstrap": {"auth_plane"},
	} {
		got := composeNetworks(r.services[n])
		g.expect(equal(got, want), "%s is on networks %v, want %v", n, got, want)
	}
	for _, n := range []string{"auth-keys-init", "acr-workload-volume"} {
		g.expect(str(r.services[n]["network_mode"]) == "none", "%s has network_mode %q, want none (it only writes its volume)", n, str(r.services[n]["network_mode"]))
	}
	members := map[string][]string{}
	for name, s := range r.services {
		for _, net := range composeNetworks(s) {
			members[net] = append(members[net], name)
		}
	}
	for net, want := range map[string][]string{
		"acr_auth_workload": {"acr-api", "auth-service"},
		"auth_plane":        {"acr-workload-bootstrap", "auth-fence-init", "auth-grants", "auth-issuer", "auth-migrate", "auth-owner-grants", "auth-provision", "auth-service", "auth-workload-seed", "postgres"},
	} {
		got := composeSortedStrs(members[net])
		g.expect(equal(got, want), "network %s has members %v, want %v", net, got, want)
	}
}

func composeAuthGoAPIVerifier(g *guard) {
	off := composeMust(g, composeIn{})
	api := off.services["go-api"]
	env := mp(api["environment"])
	g.expect(str(env["AUTH_ACCESS_TOKEN_ISSUER"]) == composeAuthIssuer, "go-api AUTH_ACCESS_TOKEN_ISSUER=%q, want %q", str(env["AUTH_ACCESS_TOKEN_ISSUER"]), composeAuthIssuer)
	g.expect(str(env["AUTH_ACR_WORKLOAD_SUBJECT"]) == "", "go-api AUTH_ACR_WORKLOAD_SUBJECT renders %q with nothing set, want empty (the seed makes the id)", str(env["AUTH_ACR_WORKLOAD_SUBJECT"]))
	const subject = "prn_AAAAAAAAAAAAAAAAAAAAAA"
	on := composeMust(g, composeIn{opsEnv: "AUTH_ACR_WORKLOAD_SUBJECT=" + subject + "\n"})
	got := str(mp(on.services["go-api"]["environment"])["AUTH_ACR_WORKLOAD_SUBJECT"])
	g.expect(got == subject, "go-api AUTH_ACR_WORKLOAD_SUBJECT renders %q from ops/.env, want %q", got, subject)
	g.expect(str(env["SSL_CERT_DIR"]) == composeAuthIssuerCA, "go-api SSL_CERT_DIR=%q, want %s", str(env["SSL_CERT_DIR"]), composeAuthIssuerCA)
	_, file := env["SSL_CERT_FILE"]
	g.expect(!file, "go-api sets SSL_CERT_FILE: it would replace the image's CA file for every outbound call")
	var mounts []string
	for _, v := range list(api["volumes"]) {
		if str(at(v, "target")) == composeAuthIssuerCA {
			mounts = append(mounts, str(at(v, "source")))
			g.expect(at(v, "read_only") == true && at(v, "bind", "create_host_path") == false,
				"go-api mounts %s writable or with create_host_path (a missing dir must fail the start)", composeAuthIssuerCA)
		}
		g.expect(!strings.Contains(str(at(v, "source")), "/key"), "go-api mounts the issuer key directory (%s)", str(at(v, "source")))
	}
	g.expect(len(mounts) == 1 && strings.HasSuffix(mounts[0], "/auth-issuer-tls/ca"), "go-api mounts %v at %s, want one bind of <tls dir>/ca", mounts, composeAuthIssuerCA)
	for d := range mp(api["depends_on"]) {
		g.expect(!strings.HasPrefix(d, "auth"), "go-api depends on %s: go-api must start without the auth venue", d)
	}
}

func composeAuthBootstrap(g *guard) {
	r := composeAuth(g)
	for name, s := range r.services {
		for d := range mp(s["depends_on"]) {
			g.expect(d != "acr-workload-bootstrap", "%s depends on acr-workload-bootstrap: a later up would run bootstrap-create again", name)
		}
	}
	boot := r.services["acr-workload-bootstrap"]
	g.expect(equal(strs(boot["command"]), []string{"workloads", "bootstrap-create", "-workload", "acr-api", "-out", "/run/workload/acr-api.bootstrap", "-apply"}),
		"acr-workload-bootstrap command %v, want the bootstrap-create of the workload acr-api", strs(boot["command"]))
	user := str(r.services["acr-api"]["user"])
	g.expect(str(boot["user"]) == user, "acr-workload-bootstrap writes as %q, acr-api reads as %q: the 0600 file must be the reader's", str(boot["user"]), user)
	vol := strings.Join(strs(r.services["acr-workload-volume"]["command"]), " ")
	uid, _, _ := strings.Cut(user, ":")
	g.expect(strings.Contains(vol, "chown "+user+" /run/workload") && strings.Contains(vol, "= "+uid+":700 ]"),
		"acr-workload-volume does not give /run/workload to %s mode 700: %q", user, vol)
	g.expect(strings.Contains(str(mp(r.services["acr-api"]["environment"])["ACR_AUTH_WORKLOAD_BOOTSTRAP_FILE"]), "/run/workload/"), "acr-api reads its bootstrap file outside the volume")
	for name, s := range r.services {
		for _, v := range list(s["volumes"]) {
			if str(at(v, "source")) != "acr_workload_bootstrap" {
				continue
			}
			ok := name == "acr-api" && at(v, "read_only") == true || name == "acr-workload-bootstrap" || name == "acr-workload-volume"
			g.expect(ok, "%s mounts acr_workload_bootstrap (only the two writers and acr-api, read-only)", name)
		}
	}
}

var composeAuthPinRe = regexp.MustCompile(`^ghcr\.io/full-chaos/dev-health-go-auth-(service|migrate)@sha256:[0-9a-f]{64}$`)

func composeAuthImagesPinned(g *guard) {
	r := composeAuth(g)
	pins := composePinsMap(g.read(composePins))
	for pin, names := range map[string][]string{
		"BIGBOY_AUTH_SERVICE_IMAGE": {"auth-service"},
		"BIGBOY_AUTH_MIGRATE_IMAGE": {"auth-owner-grants", "auth-migrate", "auth-grants", "auth-fence-init", "auth-workload-seed", "acr-workload-bootstrap"},
		"BIGBOY_DHO_IMAGE":          {"auth-keys-init"},
		"BIGBOY_ACR_POSTGRES_IMAGE": {"auth-provision", "acr-workload-volume"},
		"BIGBOY_ACR_NGINX_IMAGE":    {"auth-issuer"},
	} {
		for _, n := range names {
			got := str(r.services[n]["image"])
			g.expect(got == pins[pin], "%s renders image %q, want the %s pin", n, got, pin)
		}
	}
	for _, pin := range composeAuthPins {
		g.expect(composeAuthPinRe.MatchString(pins[pin]), "pin %s=%q is not a ghcr.io/full-chaos auth image by digest", pin, pins[pin])
		out := composeRender(g, composeIn{profiles: []string{"auth"}, pins: composeDropLine(pin + "=")})
		g.expect(strings.Contains(out.err, "required variable "+pin+" is missing a value"),
			"without the %s pin the render does not fail naming it: %q", pin, out.err)
	}
}

func composeAuthHardened(g *guard) {
	r := composeAuth(g)
	for _, n := range composeAuthAll {
		s := r.services[n]
		g.expect(s["read_only"] == true, "%s is not read_only", n)
		g.expect(equal(strs(s["cap_drop"]), []string{"ALL"}), "%s does not drop every capability", n)
		g.expect(equal(strs(s["security_opt"]), []string{"no-new-privileges:true"}), "%s lacks no-new-privileges", n)
		g.expect(len(list(s["ports"])) == 0, "%s publishes a port", n)
		g.expect(!strings.HasPrefix(str(s["user"]), "0:") || n == "acr-workload-volume", "%s runs as root", n)
	}
	g.expect(equal(strs(r.services["acr-workload-volume"]["cap_add"]), []string{"CHOWN"}), "acr-workload-volume adds more than CHOWN")
	svc := r.services["auth-service"]
	env := mp(svc["environment"])
	g.expect(str(env["AUTH_WORKLOAD_BOOTSTRAP"]) == "true", "auth-service AUTH_WORKLOAD_BOOTSTRAP=%q, want true (the Compose subject)", str(env["AUTH_WORKLOAD_BOOTSTRAP"]))
	_, kube := env["AUTH_WORKLOAD_EXCHANGE"]
	g.expect(!kube, "auth-service sets AUTH_WORKLOAD_EXCHANGE (the Kubernetes subject; there is no cluster)")
	g.expect(str(env["AUTH_SIGNING_KEY_FILE"]) == "/run/auth/signing/envelope-private-key.pem", "auth-service does not read its key from the signing volume")
	_, direct := env["AUTH_SIGNING_KEY"]
	g.expect(!direct, "auth-service carries a direct AUTH_SIGNING_KEY value")
	keyID := ""
	for _, a := range strs(r.services["auth-keys-init"]["command"]) {
		if v, ok := strings.CutPrefix(a, "--key-id="); ok {
			keyID = v
		}
	}
	g.expect(keyID != "" && keyID == str(env["AUTH_SIGNING_KEY_ID"]), "auth-keys-init key id %q, auth-service AUTH_SIGNING_KEY_ID %q: they must be one", keyID, str(env["AUTH_SIGNING_KEY_ID"]))
	for _, v := range list(r.services["auth-issuer"]["volumes"]) {
		g.expect(str(at(v, "type")) == "bind" && at(v, "read_only") == true && at(v, "bind", "create_host_path") == false,
			"auth-issuer mount %s is not a read-only bind that fails when the host file is missing", str(at(v, "target")))
	}
	g.expect(str(r.services["auth-issuer"]["user"]) == "1001:1001", "auth-issuer runs as %q, want 1001:1001 (the owner of the 0600 key file)", str(r.services["auth-issuer"]["user"]))
}

func composeAuthStartOrder(g *guard) {
	r := composeAuth(g)
	done, healthy := "service_completed_successfully", "service_healthy"
	for n, want := range map[string]map[string]string{
		"auth-provision":         {"postgres": healthy},
		"auth-owner-grants":      {"auth-provision": done, "migrate": done},
		"auth-migrate":           {"auth-provision": done, "auth-owner-grants": done, "migrate": done},
		"auth-grants":            {"auth-migrate": done, "migrate": done},
		"auth-fence-init":        {"auth-grants": done},
		"auth-workload-seed":     {"auth-grants": done},
		"acr-workload-bootstrap": {"acr-workload-volume": done, "auth-workload-seed": done},
		"auth-service":           {"auth-fence-init": done, "auth-grants": done, "auth-keys-init": done},
		"auth-issuer":            {"auth-service": healthy},
	} {
		got := map[string]string{}
		for d, v := range mp(r.services[n]["depends_on"]) {
			got[d] = str(at(v, "condition"))
		}
		g.expect(equal(got, want), "%s depends_on %v, want %v", n, got, want)
	}
	for _, n := range []string{"acr-api", "go-api"} {
		for d := range mp(r.services[n]["depends_on"]) {
			g.expect(!strings.HasPrefix(d, "auth-"), "%s depends on %s", n, d)
		}
	}
}

func composeAuthReadme(g *guard) {
	readme := g.read("ci/bigboy/README.md")
	start := strings.Index(readme, "## auth venue")
	g.expect(start >= 0, "README has no \"## auth venue\" section")
	if start < 0 {
		return
	}
	section := readme[start:]
	if end := strings.Index(section[3:], "\n## "); end >= 0 {
		section = section[:end+3]
	}
	for _, want := range []string{"run --rm acr-workload-bootstrap", "AUTH_ACR_WORKLOAD_SUBJECT", "openssl req", "/ca/issuer.pem", "/key/issuer.key", "expires_at"} {
		g.expect(strings.Contains(section, want), "README auth venue section lacks %q", want)
	}
	for _, m := range composeReadmeCmdRe.FindAllStringSubmatch(section, -1) {
		g.expect(!composeDownRe.MatchString(m[1]) || !composeDownBadRe.MatchString(m[1]), "README auth venue command removes data: %q", m[1])
	}
}

var composeAuthChecks = []check{
	{"profile_auth_adds_only_the_auth_venue", composeAuthProfileAddsOnlyAuth},
	{"auth_service_go_api_and_acr_api_carry_one_issuer_served_by_the_issuer_origin", composeAuthIssuerIsOne},
	{"the_auth_venue_is_isolated_on_its_own_networks", composeAuthNetworks},
	{"go_api_verifies_the_workload_token_without_a_dependency_on_the_auth_venue", composeAuthGoAPIVerifier},
	{"the_bootstrap_credential_is_written_once_for_the_reader_uid", composeAuthBootstrap},
	{"the_auth_images_are_pinned_and_a_missing_pin_fails_the_render", composeAuthImagesPinned},
	{"the_auth_services_are_hardened_and_keys_are_files", composeAuthHardened},
	{"the_auth_start_order_is_the_compose_yml_order", composeAuthStartOrder},
	{"the_readme_holds_the_auth_venue_procedure", composeAuthReadme},
}

func TestComposeAuthVenueReal(t *testing.T) { runReal(t, composeAuthChecks...) }

func TestComposeAuthVenuePlanted(t *testing.T) {
	readme := []string{"ci/bigboy/README.md"}
	cases := []planted{
		{name: "auth-service in the default stack", mutate: mut(composeFile, "    image: '${BIGBOY_AUTH_SERVICE_IMAGE:?BIGBOY_AUTH_SERVICE_IMAGE is the image pin: pass --env-file _records/images.env}'\n    networks:\n      acr_auth_workload: null\n      auth_plane: null\n    profiles: [auth]\n",
			"    image: '${BIGBOY_AUTH_SERVICE_IMAGE:?BIGBOY_AUTH_SERVICE_IMAGE is the image pin: pass --env-file _records/images.env}'\n    networks:\n      acr_auth_workload: null\n      auth_plane: null\n"),
			check: composeAuthProfileAddsOnlyAuth, want: "profile auth adds"},
		{name: "go-api issuer differs", mutate: mut(composeFile, "    - AUTH_ACCESS_TOKEN_ISSUER=https://auth.bigboy.internal:8443\n", "    - AUTH_ACCESS_TOKEN_ISSUER=https://auth.bigboy.internal\n"),
			check: composeAuthIssuerIsOne, want: "go-api AUTH_ACCESS_TOKEN_ISSUER"},
		{name: "auth-service issuer differs", mutate: mut(composeFile, "      AUTH_TOKEN_ISSUER_URL: https://auth.bigboy.internal:8443\n", "      AUTH_TOKEN_ISSUER_URL: https://auth.fullchaos.dev\n"),
			check: composeAuthIssuerIsOne, want: "auth-service AUTH_TOKEN_ISSUER_URL"},
		{name: "issuer alias lost", mutate: mut(composeFile, "        aliases: [auth.bigboy.internal]\n", "        aliases: [auth-issuer.bigboy.internal]\n"),
			check: composeAuthIssuerIsOne, want: "auth-issuer answers"},
		{name: "issuer serves every path", mutate: mut(composeFile, "          location / { return 404; }\n", "          location / { proxy_pass $$auth_upstream; }\n"),
			check: composeAuthIssuerIsOne, want: "two documents"},
		{name: "issuer plain http", mutate: mut(composeFile, "          listen 8443 ssl;\n", "          listen 8443;\n"),
			check: composeAuthIssuerIsOne, want: "TLS"},
		{name: "acr-api on auth_plane", mutate: mut(composeFile, "      acr_auth_workload: null\n      dho-clickhouse-readers: null\n      dho-query-internal: null\n    profiles: [acr]", "      acr_auth_workload: null\n      auth_plane: null\n      dho-clickhouse-readers: null\n      dho-query-internal: null\n    profiles: [acr]"),
			check: composeAuthNetworks, want: "network auth_plane has members"},
		{name: "auth-service on the ops network", mutate: mut(composeFile, "    networks:\n      acr_auth_workload: null\n      auth_plane: null\n    profiles: [auth]\n", "    networks:\n      acr_auth_workload: null\n      auth_plane: null\n      dev-health: null\n    profiles: [auth]\n"),
			check: composeAuthNetworks, want: "auth-service is on networks"},
		{name: "postgres off auth_plane", mutate: mut(composeFile, "    networks:\n      auth_plane: null\n      dev-health: null\n    ports:\n    - ${POSTGRES_PORT", "    networks:\n      dev-health: null\n    ports:\n    - ${POSTGRES_PORT"),
			check: composeAuthNetworks, want: "network auth_plane has members"},
		{name: "go-api replaces the CA file", mutate: mut(composeFile, "    - SSL_CERT_DIR=/run/auth-issuer-ca\n", "    - SSL_CERT_FILE=/run/auth-issuer-ca/issuer.pem\n"),
			check: composeAuthGoAPIVerifier, want: "SSL_CERT"},
		{name: "go-api subject defaulted", mutate: mut(composeFile, "    - AUTH_ACR_WORKLOAD_SUBJECT=${AUTH_ACR_WORKLOAD_SUBJECT:-}\n", "    - AUTH_ACR_WORKLOAD_SUBJECT=${AUTH_ACR_WORKLOAD_SUBJECT:-prn_BBBBBBBBBBBBBBBBBBBBBB}\n"),
			check: composeAuthGoAPIVerifier, want: "with nothing set"},
		{name: "go-api mounts the whole tls dir", mutate: mut(composeFile, "      source: ${BIGBOY_AUTH_ISSUER_TLS_DIR:-/var/lib/oci-cache/mcp-venue/auth-issuer-tls}/ca\n", "      source: ${BIGBOY_AUTH_ISSUER_TLS_DIR:-/var/lib/oci-cache/mcp-venue/auth-issuer-tls}\n"),
			check: composeAuthGoAPIVerifier, want: "one bind of <tls dir>/ca"},
		{name: "go-api ca dir created when missing", mutate: mut(composeFile, "      target: /run/auth-issuer-ca\n      read_only: true\n      bind:\n        create_host_path: false\n", "      target: /run/auth-issuer-ca\n      read_only: true\n"),
			check: composeAuthGoAPIVerifier, want: "create_host_path"},
		{name: "acr-api waits for the bootstrap", mutate: mut(composeFile, "    depends_on:\n      acr-db-acl:\n        condition: service_completed_successfully\n      acr-postgres:\n        condition: service_healthy\n    deploy:\n      resources:\n        limits:\n          cpus: 1\n          memory: 512M\n        reservations:\n          cpus: 0.1\n          memory: 128M\n    environment:\n      ACR_ADDR: :8080\n",
			"    depends_on:\n      acr-db-acl:\n        condition: service_completed_successfully\n      acr-postgres:\n        condition: service_healthy\n      acr-workload-bootstrap:\n        condition: service_completed_successfully\n    deploy:\n      resources:\n        limits:\n          cpus: 1\n          memory: 512M\n        reservations:\n          cpus: 0.1\n          memory: 128M\n    environment:\n      ACR_ADDR: :8080\n"),
			check: composeAuthBootstrap, want: "depends on acr-workload-bootstrap"},
		{name: "bootstrap written as 65532", mutate: mut(composeFile, "    tmpfs: [/tmp]\n    user: 1001:1001\n    volumes:\n    - type: volume\n      source: acr_workload_bootstrap\n", "    tmpfs: [/tmp]\n    user: 65532:65532\n    volumes:\n    - type: volume\n      source: acr_workload_bootstrap\n"),
			check: composeAuthBootstrap, want: "the 0600 file must be the reader's"},
		{name: "bootstrap for the api workload", mutate: mut(composeFile, "bootstrap-create, -workload, acr-api,", "bootstrap-create, -workload, api,"),
			check: composeAuthBootstrap, want: "bootstrap-create of the workload acr-api"},
		{name: "auth-service mounts the bootstrap", mutate: mut(composeFile, "      read_only: true\n      volume:\n        subpath: private\n", "      read_only: true\n      volume:\n        subpath: private\n    - type: volume\n      source: acr_workload_bootstrap\n      target: /run/workload\n"),
			check: composeAuthBootstrap, want: "auth-service mounts acr_workload_bootstrap"},
		{name: "auth-service pin has a default", mutate: mut(composeFile, "${BIGBOY_AUTH_SERVICE_IMAGE:?BIGBOY_AUTH_SERVICE_IMAGE is the image pin: pass --env-file _records/images.env}", "${BIGBOY_AUTH_SERVICE_IMAGE:-ghcr.io/full-chaos/dev-health-go-auth-service:local}"),
			check: composeAuthImagesPinned, want: "without the BIGBOY_AUTH_SERVICE_IMAGE pin"},
		{name: "auth-migrate pin is a tag", mutate: mut(composePins, "BIGBOY_AUTH_MIGRATE_IMAGE=ghcr.io/full-chaos/dev-health-go-auth-migrate@sha256:", "BIGBOY_AUTH_MIGRATE_IMAGE=ghcr.io/full-chaos/dev-health-go-auth-migrate:sha-"),
			check: composeAuthImagesPinned, want: "pin BIGBOY_AUTH_MIGRATE_IMAGE"},
		{name: "auth-service writable root", mutate: mut(composeFile, "    profiles: [auth]\n    read_only: true\n    restart: unless-stopped\n    security_opt: [no-new-privileges:true]\n    tmpfs: [/tmp]\n    user: 65532:65532\n", "    profiles: [auth]\n    restart: unless-stopped\n    security_opt: [no-new-privileges:true]\n    tmpfs: [/tmp]\n    user: 65532:65532\n"),
			check: composeAuthHardened, want: "auth-service is not read_only"},
		{name: "kubernetes exchange on", mutate: mut(composeFile, "      AUTH_WORKLOAD_BOOTSTRAP: 'true'\n", "      AUTH_WORKLOAD_BOOTSTRAP: 'true'\n      AUTH_WORKLOAD_EXCHANGE: 'true'\n"),
			check: composeAuthHardened, want: "AUTH_WORKLOAD_EXCHANGE"},
		{name: "signing key ids differ", mutate: mut(composeFile, "      AUTH_SIGNING_KEY_ID: auth-signing-bigboy\n", "      AUTH_SIGNING_KEY_ID: auth-signing-local\n"),
			check: composeAuthHardened, want: "they must be one"},
		{name: "issuer key created when missing", mutate: mut(composeFile, "      target: /run/tls/issuer.key\n      read_only: true\n      bind:\n        create_host_path: false\n", "      target: /run/tls/issuer.key\n      read_only: true\n"),
			check: composeAuthHardened, want: "/run/tls/issuer.key"},
		{name: "auth-service before the fence", mutate: mut(composeFile, "    depends_on:\n      auth-fence-init:\n        condition: service_completed_successfully\n      auth-grants:\n        condition: service_completed_successfully\n      auth-keys-init:\n", "    depends_on:\n      auth-grants:\n        condition: service_completed_successfully\n      auth-keys-init:\n"),
			check: composeAuthStartOrder, want: "auth-service depends_on"},
		{name: "readme loses the bootstrap step", copy: readme, mutate: mut("ci/bigboy/README.md", "run --rm acr-workload-bootstrap", "up -d acr-workload-bootstrap"),
			check: composeAuthReadme, want: "run --rm acr-workload-bootstrap"},
	}
	for i := range cases {
		if cases[i].copy == nil {
			cases[i].copy = composeFiles
		}
	}
	runPlanted(t, cases)
}

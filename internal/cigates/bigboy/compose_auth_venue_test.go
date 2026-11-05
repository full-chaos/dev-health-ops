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
	composeAuthAll      = []string{"acr-workload-bootstrap", "acr-workload-volume", "auth-fence-init", "auth-grants", "auth-issuer", "auth-keys-init", "auth-migrate", "auth-owner-grants", "auth-provision", "auth-service", "auth-session-keys-init", "auth-workload-seed", "go-api-workload-bootstrap", "go-api-workload-volume"}
	composeAuthOneShots = []string{"acr-workload-bootstrap", "acr-workload-volume", "auth-fence-init", "auth-grants", "auth-keys-init", "auth-migrate", "auth-owner-grants", "auth-provision", "auth-session-keys-init", "auth-workload-seed", "go-api-workload-bootstrap", "go-api-workload-volume"}
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
		{"query-api", "AUTH_ACCESS_TOKEN_ISSUER"},
		{"web", "AUTH_ACCESS_TOKEN_ISSUER"},
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
	conf := str(mp(front["environment"])["AUTH_ISSUER_NGINX_CONF"])
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
		"auth-service":              {"acr_auth_workload", "auth_plane", "dev-health"},
		"auth-issuer":               {"auth_plane", "dev-health"},
		"auth-provision":            {"auth_plane"},
		"auth-owner-grants":         {"auth_plane"},
		"auth-migrate":              {"auth_plane"},
		"auth-grants":               {"auth_plane"},
		"auth-fence-init":           {"auth_plane"},
		"auth-workload-seed":        {"auth_plane"},
		"acr-workload-bootstrap":    {"auth_plane"},
		"go-api-workload-bootstrap": {"auth_plane"},
	} {
		got := composeNetworks(r.services[n])
		g.expect(equal(got, want), "%s is on networks %v, want %v", n, got, want)
	}
	for _, n := range []string{"auth-keys-init", "auth-session-keys-init", "acr-workload-volume", "go-api-workload-volume"} {
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
		"auth_plane":        {"acr-workload-bootstrap", "auth-fence-init", "auth-grants", "auth-issuer", "auth-migrate", "auth-owner-grants", "auth-provision", "auth-service", "auth-workload-seed", "go-api-workload-bootstrap", "postgres"},
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
			g.expect(d != "acr-workload-bootstrap" && d != "go-api-workload-bootstrap", "%s depends on %s: a later up would run bootstrap-create again", name, d)
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
		"BIGBOY_AUTH_MIGRATE_IMAGE": {"auth-owner-grants", "auth-migrate", "auth-grants", "auth-fence-init", "auth-workload-seed", "acr-workload-bootstrap", "go-api-workload-bootstrap"},
		"BIGBOY_DHO_IMAGE":          {"auth-keys-init"},
		"BIGBOY_ACR_POSTGRES_IMAGE": {"auth-provision", "acr-workload-volume", "auth-session-keys-init", "go-api-workload-volume"},
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
		g.expect(!strings.HasPrefix(str(s["user"]), "0:") || n == "acr-workload-volume" || n == "auth-session-keys-init" || n == "go-api-workload-volume", "%s runs as root", n)
	}
	for _, n := range []string{"acr-workload-volume", "auth-session-keys-init", "go-api-workload-volume"} {
		g.expect(equal(strs(r.services[n]["cap_add"]), []string{"CHOWN"}), "%s adds more than CHOWN", n)
	}
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

// A root container with only CHOWN has no DAC_READ_SEARCH, DAC_OVERRIDE or FOWNER: it cannot chmod a path it gave away, and
// it cannot look inside a dir it gave away (mode 0700) or a dir it never owned. Order: dir to root first, dir to the app uid last.
func composeAuthChownThenChmod(g *guard) {
	r := composeAuth(g)
	for n, s := range r.services {
		if !equal(strs(s["cap_add"]), []string{"CHOWN"}) || !equal(strs(s["cap_drop"]), []string{"ALL"}) || !strings.HasPrefix(str(s["user"]), "0:") {
			continue
		}
		var targets []string
		for _, v := range list(s["volumes"]) {
			targets = append(targets, strings.TrimRight(str(at(v, "target")), "/"))
		}
		for _, cmd := range strs(s["command"]) {
			given := map[string]bool{}
			rootDir := map[string]bool{}
			vars := map[string]string{}
			for _, seg := range regexp.MustCompile(`;|&&|\|\|`).Split(cmd, -1) {
				seg = strings.Trim(strings.TrimSpace(seg), "{} ")
				if k, v, ok := strings.Cut(seg, "="); ok && !strings.ContainsAny(k, " \t") && k != "" {
					vars[k] = v
					continue
				}
				for k, v := range vars {
					for _, ref := range []string{"$$" + k, "${" + k + "}", "$" + k} {
						seg = strings.ReplaceAll(seg, ref, v)
					}
				}
				f := strings.Fields(seg)
				for i := range f {
					f[i] = strings.Trim(f[i], `"'`)
				}
				var paths []string
				for _, w := range f {
					if strings.HasPrefix(w, "/") {
						paths = append(paths, w)
					}
				}
				ref := func(p string) {
					for _, d := range targets {
						if !strings.HasPrefix(p, d+"/") {
							continue
						}
						g.expect(!given[d], "%s references %s after chown of %s to a non-root uid; root cannot search it (chown 0:0 %s first)", n, p, d, d)
						g.expect(rootDir[d], "%s references %s before %s is chowned 0:0; root has no DAC_READ_SEARCH (chown 0:0 %s first)", n, p, d, d)
					}
				}
				if len(f) < 2 || f[0] != "chown" {
					for _, p := range paths {
						ref(p)
					}
				}
				if len(f) < 2 {
					continue
				}
				switch f[0] {
				case "chown":
					root := f[1] == "0:0" || f[1] == "0"
					for _, p := range f[2:] {
						ref(p)
						given[p] = !root
						if root {
							rootDir[p] = true
						}
					}
				case "chmod":
					for _, p := range f[2:] {
						g.expect(!given[p], "%s runs chmod on %s after a chown of it to a non-root uid; CHOWN alone has no CAP_FOWNER (chown 0:0 first)", n, p)
					}
				}
			}
		}
	}
}

func composeAuthStartOrder(g *guard) {
	r := composeAuth(g)
	done, healthy := "service_completed_successfully", "service_healthy"
	for n, want := range map[string]map[string]string{
		"auth-provision":            {"postgres": healthy},
		"auth-owner-grants":         {"auth-provision": done, "migrate": done},
		"auth-migrate":              {"auth-provision": done, "auth-owner-grants": done, "migrate": done},
		"auth-grants":               {"auth-migrate": done, "migrate": done},
		"auth-fence-init":           {"auth-grants": done},
		"auth-workload-seed":        {"auth-grants": done},
		"acr-workload-bootstrap":    {"acr-workload-volume": done, "auth-workload-seed": done},
		"go-api-workload-bootstrap": {"go-api-workload-volume": done, "auth-workload-seed": done},
		"auth-service":              {"auth-fence-init": done, "auth-grants": done, "auth-keys-init": done, "auth-session-keys-init": done},
		"auth-issuer":               {"auth-service": healthy},
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

// The eddsa class and its phase-2 wiring (CHAOS-12978): auth-service mints eddsa, joins dev-health so go-api reaches it at
// AUTH_PROXY_URL, reads its session successor key from a generated 0600 file, and the verifiers trust the venue CA.
func composeAuthEddsaWiring(g *guard) {
	r := composeAuth(g)
	env := func(svc string) map[string]any { return mp(r.services[svc]["environment"]) }
	want := map[string]map[string]string{
		"auth-service": {"AUTH_ACCESS_TOKEN_CLASS": "eddsa", "AUTH_JWT_AUDIENCE": "dev-health-api", "AUTH_SESSION_SUCCESSOR_KEY_FILE": "/run/auth/session/successor-key"},
		"go-api":       {"AUTH_PROXY_SESSIONS": "true", "AUTH_PROXY_URL": "http://auth-service:8095", "AUTH_SESSION_ISSUE_CREDENTIAL": "workload", "AUTH_WORKLOAD_BOOTSTRAP_FILE": "/run/workload/api.bootstrap", "AUTH_WORKLOAD_ISSUER": composeAuthIssuer},
		"query-api":    {"AUTH_PROXY_SESSIONS": "true", "SSL_CERT_DIR": composeAuthIssuerCA},
		"web":          {"AUTH_ACCESS_TOKEN_CLASS": "eddsa", "NODE_EXTRA_CA_CERTS": composeAuthIssuerCA + "/issuer.pem"},
		"auth-migrate": {"AUTH_SESSION_READER_ROLES": "devhealth_api,devhealth_query_api"},
		"auth-grants":  {"AUTH_SESSION_READER_ROLES": "devhealth_api,devhealth_query_api"},
	}
	for svc, kv := range want {
		for k, v := range kv {
			g.expect(str(env(svc)[k]) == v, "%s %s=%q, want %q", svc, k, str(env(svc)[k]), v)
		}
	}
	for _, n := range composeAuthAll {
		if n == "auth-migrate" || n == "auth-grants" {
			continue
		}
		_, has := env(n)["AUTH_SESSION_READER_ROLES"]
		g.expect(!has, "%s sets AUTH_SESSION_READER_ROLES: only auth-migrate and auth-grants grant the session read", n)
	}
	for _, c := range []struct{ svc, key string }{{"auth-service", "AUTH_SESSION_SUCCESSOR_KEY"}, {"go-api", "AUTH_SESSION_ISSUE_TOKEN"}, {"web", "SSL_CERT_FILE"}, {"query-api", "SSL_CERT_FILE"}} {
		_, has := env(c.svc)[c.key]
		g.expect(!has, "%s carries %s: a secret is a _FILE and the CA is a dir or NODE_EXTRA_CA_CERTS", c.svc, c.key)
	}
	for _, svc := range []string{"query-api", "web"} {
		var mounts []string
		for _, v := range list(r.services[svc]["volumes"]) {
			if str(at(v, "target")) == composeAuthIssuerCA {
				mounts = append(mounts, str(at(v, "source")))
				g.expect(at(v, "read_only") == true && at(v, "bind", "create_host_path") == false, "%s mounts %s writable or with create_host_path", svc, composeAuthIssuerCA)
			}
		}
		g.expect(len(mounts) == 1 && strings.HasSuffix(mounts[0], "/auth-issuer-tls/ca"), "%s mounts %v at %s, want one bind of <tls dir>/ca", svc, mounts, composeAuthIssuerCA)
		g.expect(composeHas(composeNetworks(r.services[svc]), "dev-health"), "%s is not on dev-health: it cannot reach the issuer alias", svc)
	}
	g.expect(composeHas(composeNetworks(r.services["go-api"]), "dev-health") && composeHas(composeNetworks(r.services["auth-service"]), "dev-health"),
		"go-api and auth-service do not share dev-health: AUTH_PROXY_URL http://auth-service:8095 is unreachable")
	// go-api reads its workload bootstrap file from a read-only volume only it mounts; the api bootstrap is written for uid 65532.
	for name, s := range r.services {
		for _, v := range list(s["volumes"]) {
			if str(at(v, "source")) != "go_api_workload_bootstrap" {
				continue
			}
			ok := name == "go-api" && at(v, "target") == "/run/workload" && at(v, "read_only") == true || name == "go-api-workload-bootstrap" || name == "go-api-workload-volume"
			g.expect(ok, "%s mounts go_api_workload_bootstrap (only the two writers and go-api, read-only)", name)
		}
	}
	boot := r.services["go-api-workload-bootstrap"]
	g.expect(equal(strs(boot["command"]), []string{"workloads", "bootstrap-create", "-workload", "api", "-out", "/run/workload/api.bootstrap", "-apply"}), "go-api-workload-bootstrap command %v, want the bootstrap-create of the workload api", strs(boot["command"]))
	g.expect(str(boot["user"]) == "65532:65532", "go-api-workload-bootstrap writes as %q: the 0600 file must be uid 65532's, go-api's", str(boot["user"]))
	g.expect(strings.Contains(strings.Join(strs(r.services["go-api-workload-volume"]["command"]), " "), "= 65532:700 ]"), "go-api-workload-volume does not give /run/workload to 65532 mode 700")
	_, subjectSet := mp(r.services["auth-service"]["environment"])["AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT"]
	g.expect(subjectSet, "auth-service has no AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT: it refuses every go-api workload token")
	const subjectProbe = "prn_probeprobeprobeprobe0"
	withSubject := composeMust(g, composeIn{profiles: []string{"auth"}, opsEnv: "AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT=" + subjectProbe + "\n"})
	authEnv := mp(withSubject.services["auth-service"]["environment"])
	g.expect(str(authEnv["AUTH_AUTHORIZE_CALLER_SUBJECTS"]) == subjectProbe && str(authEnv["AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT"]) == subjectProbe, "auth-service AUTH_AUTHORIZE_CALLER_SUBJECTS is not the go-api workload subject of AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT: its /v1/authorize refuses go-api")
	const probe = "probe-jwt-key-not-a-secret-0123456789abcdef"
	withKey := composeMust(g, composeIn{profiles: []string{"auth"}, opsEnv: "JWT_SECRET_KEY=" + probe + "\n"})
	got := str(mp(withKey.services["auth-service"]["environment"])["AUTH_JWT_SECRET_KEY"])
	g.expect(got == probe, "auth-service AUTH_JWT_SECRET_KEY is not go-api's JWT_SECRET_KEY from ops/.env (one source, no copy)")
	init := r.services["auth-session-keys-init"]
	cmd := strings.Join(strs(init["command"]), " ")
	for _, w := range []string{"umask 077", "head -c 32 /dev/urandom", "base64", "chmod 0600", "/run/auth/session/successor-key"} {
		g.expect(strings.Contains(cmd, w), "auth-session-keys-init command lacks %q", w)
	}
	for name, s := range r.services {
		for _, v := range list(s["volumes"]) {
			if str(at(v, "source")) != "auth_session_keys" {
				continue
			}
			ok := name == "auth-session-keys-init" || name == "auth-service" && at(v, "read_only") == true
			g.expect(ok, "%s mounts auth_session_keys (the init writes it, auth-service reads it read-only)", name)
		}
	}
}

func composeHas(list []string, want string) bool {
	for _, s := range list {
		if s == want {
			return true
		}
	}
	return false
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
	{"a_chown_only_root_init_never_chmods_a_path_it_gave_away", composeAuthChownThenChmod},
	{"the_auth_start_order_is_the_compose_yml_order", composeAuthStartOrder},
	{"auth_service_mints_eddsa_and_the_verifiers_and_session_wiring_follow", composeAuthEddsaWiring},
	{"the_readme_holds_the_auth_venue_procedure", composeAuthReadme},
}

func TestComposeAuthVenueReal(t *testing.T) { runReal(t, composeAuthChecks...) }

func TestComposeAuthVenuePlanted(t *testing.T) {
	readme := []string{"ci/bigboy/README.md"}
	cases := []planted{
		{name: "auth-service in the default stack", mutate: mut(composeFile, "    image: '${BIGBOY_AUTH_SERVICE_IMAGE:?BIGBOY_AUTH_SERVICE_IMAGE is the image pin: pass --env-file _records/images.env}'\n    networks:\n      acr_auth_workload: null\n      auth_plane: null\n      dev-health: null\n    profiles: [auth]\n",
			"    image: '${BIGBOY_AUTH_SERVICE_IMAGE:?BIGBOY_AUTH_SERVICE_IMAGE is the image pin: pass --env-file _records/images.env}'\n    networks:\n      acr_auth_workload: null\n      auth_plane: null\n      dev-health: null\n"),
			check: composeAuthProfileAddsOnlyAuth, want: "profile auth adds"},
		{name: "go-api issuer differs", mutate: mut(composeFile, "    - AUTH_ACCESS_TOKEN_ISSUER=https://auth.bigboy.internal:8443\n", "    - AUTH_ACCESS_TOKEN_ISSUER=https://auth.bigboy.internal\n"),
			check: composeAuthIssuerIsOne, want: "go-api AUTH_ACCESS_TOKEN_ISSUER"},
		{name: "auth-service issuer differs", mutate: mut(composeFile, "      AUTH_TOKEN_ISSUER_URL: https://auth.bigboy.internal:8443\n", "      AUTH_TOKEN_ISSUER_URL: https://auth.fullchaos.dev\n"),
			check: composeAuthIssuerIsOne, want: "auth-service AUTH_TOKEN_ISSUER_URL"},
		{name: "issuer alias lost", mutate: mut(composeFile, "        aliases: [auth.bigboy.internal]\n", "        aliases: [auth-issuer.bigboy.internal]\n"),
			check: composeAuthIssuerIsOne, want: "auth-issuer answers"},
		{name: "issuer serves every path", mutate: mut(composeFile, "            location / { return 404; }\n", "            location / { proxy_pass $$auth_upstream; }\n"),
			check: composeAuthIssuerIsOne, want: "two documents"},
		{name: "issuer plain http", mutate: mut(composeFile, "            listen 8443 ssl;\n", "            listen 8443;\n"),
			check: composeAuthIssuerIsOne, want: "TLS"},
		{name: "acr-api on auth_plane", mutate: mut(composeFile, "      acr_auth_workload: null\n      dho-clickhouse-readers: null\n      dho-query-internal: null\n    profiles: [acr]", "      acr_auth_workload: null\n      auth_plane: null\n      dho-clickhouse-readers: null\n      dho-query-internal: null\n    profiles: [acr]"),
			check: composeAuthNetworks, want: "network auth_plane has members"},
		{name: "auth-service off the ops network", mutate: mut(composeFile, "    networks:\n      acr_auth_workload: null\n      auth_plane: null\n      dev-health: null\n    profiles: [auth]\n    read_only: true\n    restart: unless-stopped", "    networks:\n      acr_auth_workload: null\n      auth_plane: null\n    profiles: [auth]\n    read_only: true\n    restart: unless-stopped"),
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
		{name: "query-api issuer differs", mutate: mut(composeFile, "    - AUTH_ACCESS_TOKEN_ISSUER=https://auth.bigboy.internal:8443\n    - AUTH_PROXY_SESSIONS=true\n    - CLICKHOUSE_URI", "    - AUTH_ACCESS_TOKEN_ISSUER=https://auth.fullchaos.dev\n    - AUTH_PROXY_SESSIONS=true\n    - CLICKHOUSE_URI"),
			check: composeAuthIssuerIsOne, want: "query-api AUTH_ACCESS_TOKEN_ISSUER"},
		{name: "web issuer differs", mutate: mut(composeFile, "    - AUTH_ACCESS_TOKEN_ISSUER=https://auth.bigboy.internal:8443\n    - AUTH_SECRET", "    - AUTH_ACCESS_TOKEN_ISSUER=https://auth-bigboy.fullchaos.dev\n    - AUTH_SECRET"),
			check: composeAuthIssuerIsOne, want: "web AUTH_ACCESS_TOKEN_ISSUER"},
		{name: "auth-service class dropped", mutate: mut(composeFile, "      AUTH_ACCESS_TOKEN_CLASS: eddsa\n", ""),
			check: composeAuthEddsaWiring, want: "AUTH_ACCESS_TOKEN_CLASS"},
		{name: "web class dropped", mutate: mut(composeFile, "    - AUTH_ACCESS_TOKEN_CLASS=eddsa\n", ""),
			check: composeAuthEddsaWiring, want: "web AUTH_ACCESS_TOKEN_CLASS"},
		{name: "go-api sessions off", mutate: mut(composeFile, "    - AUTH_PROXY_SESSIONS=true\n    - AUTH_PROXY_URL=http://auth-service:8095\n", "    - AUTH_PROXY_URL=http://auth-service:8095\n"),
			check: composeAuthEddsaWiring, want: "go-api AUTH_PROXY_SESSIONS"},
		{name: "go-api proxy url wrong", mutate: mut(composeFile, "    - AUTH_PROXY_URL=http://auth-service:8095\n", "    - AUTH_PROXY_URL=http://auth-service:8096\n"),
			check: composeAuthEddsaWiring, want: "go-api AUTH_PROXY_URL"},
		{name: "query-api sessions off", mutate: mut(composeFile, "    - AUTH_ACCESS_TOKEN_ISSUER=https://auth.bigboy.internal:8443\n    - AUTH_PROXY_SESSIONS=true\n    - CLICKHOUSE_URI", "    - AUTH_ACCESS_TOKEN_ISSUER=https://auth.bigboy.internal:8443\n    - CLICKHOUSE_URI"),
			check: composeAuthEddsaWiring, want: "query-api AUTH_PROXY_SESSIONS"},
		{name: "query-api without the venue CA", mutate: mut(composeFile, "    - SETTINGS_ENCRYPTION_KEY=${SETTINGS_ENCRYPTION_KEY}\n    - SSL_CERT_DIR=/run/auth-issuer-ca\n", "    - SETTINGS_ENCRYPTION_KEY=${SETTINGS_ENCRYPTION_KEY}\n"),
			check: composeAuthEddsaWiring, want: "query-api SSL_CERT_DIR"},
		{name: "web without the CA file", mutate: mut(composeFile, "    - NODE_EXTRA_CA_CERTS=/run/auth-issuer-ca/issuer.pem\n", ""),
			check: composeAuthEddsaWiring, want: "web NODE_EXTRA_CA_CERTS"},
		{name: "auth-grants without the session readers", mutate: mut(composeFile, "      <<: *auth-migration-env\n      AUTH_SESSION_READER_ROLES: devhealth_api,devhealth_query_api\n    image: '${BIGBOY_AUTH_MIGRATE_IMAGE:?BIGBOY_AUTH_MIGRATE_IMAGE is the image pin: pass --env-file _records/images.env}'\n    networks:\n      auth_plane: null\n    profiles: [auth]\n    read_only: true\n    restart: 'no'\n    security_opt: [no-new-privileges:true]\n    tmpfs: [/tmp]\n    user: 65532:65532\n  auth-fence-init:", "      <<: *auth-migration-env\n    image: '${BIGBOY_AUTH_MIGRATE_IMAGE:?BIGBOY_AUTH_MIGRATE_IMAGE is the image pin: pass --env-file _records/images.env}'\n    networks:\n      auth_plane: null\n    profiles: [auth]\n    read_only: true\n    restart: 'no'\n    security_opt: [no-new-privileges:true]\n    tmpfs: [/tmp]\n    user: 65532:65532\n  auth-fence-init:"),
			check: composeAuthEddsaWiring, want: "auth-grants AUTH_SESSION_READER_ROLES"},
		{name: "successor key world readable", mutate: mut(composeFile, "chmod 0600 \"$$f\"", "chmod 0644 \"$$f\""),
			check: composeAuthEddsaWiring, want: "chmod 0600"},
		{name: "successor key direct value", mutate: mut(composeFile, "      AUTH_SESSION_SUCCESSOR_KEY_FILE: /run/auth/session/successor-key\n", "      AUTH_SESSION_SUCCESSOR_KEY: AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA\n"),
			check: composeAuthEddsaWiring, want: "AUTH_SESSION_SUCCESSOR_KEY"},
		{name: "go-api session credential is the service token", mutate: mut(composeFile, "    - AUTH_SESSION_ISSUE_CREDENTIAL=workload\n", "    - AUTH_SESSION_ISSUE_CREDENTIAL=session_compat\n"),
			check: composeAuthEddsaWiring, want: "go-api AUTH_SESSION_ISSUE_CREDENTIAL"},
		{name: "go-api bootstrap file path differs", mutate: mut(composeFile, "    - AUTH_WORKLOAD_BOOTSTRAP_FILE=/run/workload/api.bootstrap\n", "    - AUTH_WORKLOAD_BOOTSTRAP_FILE=/run/workload/acr-api.bootstrap\n"),
			check: composeAuthEddsaWiring, want: "go-api AUTH_WORKLOAD_BOOTSTRAP_FILE"},
		{name: "go-api bootstrap written as 1001", mutate: mut(composeFile, "    tmpfs: [/tmp]\n    user: 65532:65532\n    volumes:\n    - type: volume\n      source: go_api_workload_bootstrap\n", "    tmpfs: [/tmp]\n    user: 1001:1001\n    volumes:\n    - type: volume\n      source: go_api_workload_bootstrap\n"),
			check: composeAuthEddsaWiring, want: "go-api's"},
		{name: "go-api bootstrap volume writable in go-api", mutate: mut(composeFile, "      source: go_api_workload_bootstrap\n      target: /run/workload\n      read_only: true\n", "      source: go_api_workload_bootstrap\n      target: /run/workload\n"),
			check: composeAuthEddsaWiring, want: "mounts go_api_workload_bootstrap"},
		{name: "auth-service without the api workload subject", mutate: mut(composeFile, "      AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT: ${AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT:-}\n", ""),
			check: composeAuthEddsaWiring, want: "AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT"},
		{name: "auth-service without the authorize caller subjects", mutate: mut(composeFile, "      AUTH_AUTHORIZE_CALLER_SUBJECTS: ${AUTH_SESSION_ISSUE_WORKLOAD_SUBJECT:-}\n", ""),
			check: composeAuthEddsaWiring, want: "AUTH_AUTHORIZE_CALLER_SUBJECTS"},
		{name: "auth-service jwt key dropped", mutate: mut(composeFile, "      AUTH_JWT_SECRET_KEY: ${JWT_SECRET_KEY}\n", ""),
			check: composeAuthEddsaWiring, want: "AUTH_JWT_SECRET_KEY"},
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

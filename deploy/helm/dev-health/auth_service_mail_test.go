package devhealth_test

import (
	"fmt"
	"strings"
	"testing"
)

var authServiceMailNames = []string{"EMAIL_PROVIDER", "EMAIL_FROM_ADDRESS", "EMAIL_API_KEY", "RESEND_API_KEY", "APP_BASE_URL", "AUTH_MAIL_ALLOW_CONSOLE"}

const mailPeers = `authService.networkPolicy.egress.mail.to=[{"podSelector":{"matchLabels":{"app":"mail-relay"}}}]`

// mailSets is what a users-owned render needs for the mail setting: the Secret the keys come from and the
// peers the mail provider is reached at.
func mailSets(extra ...string) []string {
	return append([]string{"--set", "authService.mail.secretName=a-mail", "--set-json", mailPeers}, extra...)
}

func withoutMailSets(args []string) []string {
	var out []string
	for i := 0; i < len(args); i++ {
		if i+1 < len(args) && (strings.HasPrefix(args[i+1], "authService.mail.secretName=") || strings.HasPrefix(args[i+1], "authService.networkPolicy.egress.mail.")) {
			i++
			continue
		}
		if args[i] == "--set" || args[i] == "--set-json" {
			if i+1 < len(args) {
				out = append(out, args[i], args[i+1])
				i++
				continue
			}
		}
		out = append(out, args[i])
	}
	return out
}

func authServiceContainerEnv(docs []map[string]any) map[string]string {
	out := map[string]string{}
	for _, doc := range docs {
		if doc["kind"] != "Deployment" || !strings.HasSuffix(fmt.Sprint(at(doc, "metadata", "name")), "-auth-service") {
			continue
		}
		for _, container := range podContainers(doc) {
			env, _ := at(container, "env").([]any)
			for _, entry := range env {
				out[fmt.Sprint(at(entry, "name"))] = envEntryValue(entry)
			}
		}
	}
	return out
}

func TestAuthServiceMailEnvAtPhase4(t *testing.T) {
	env := authServiceContainerEnv(renderPhase(t, 4, mailSets()...))
	want := map[string]string{
		"EMAIL_PROVIDER":     "secret:a-mail/EMAIL_PROVIDER",
		"EMAIL_FROM_ADDRESS": "secret:a-mail/EMAIL_FROM_ADDRESS",
		"APP_BASE_URL":       "secret:a-mail/APP_BASE_URL",
		"EMAIL_API_KEY":      "secret:a-mail/EMAIL_API_KEY?",
		"RESEND_API_KEY":     "secret:a-mail/RESEND_API_KEY?",
	}
	for name, value := range want {
		if env[name] != value {
			t.Fatalf("auth-service %s = %q; want %q", name, env[name], value)
		}
	}
}

func TestAuthServiceMailKeyNamesAreSettable(t *testing.T) {
	env := authServiceContainerEnv(renderPhase(t, 4, mailSets("--set", "authService.mail.keys.apiKey=RESEND_KEY", "--set", "authService.mail.keys.appBaseURL=WEB_ORIGIN")...))
	if env["EMAIL_API_KEY"] != "secret:a-mail/RESEND_KEY?" || env["APP_BASE_URL"] != "secret:a-mail/WEB_ORIGIN" {
		t.Fatalf("key names not honoured: %q %q", env["EMAIL_API_KEY"], env["APP_BASE_URL"])
	}
}

func TestAuthServiceNoMailEnvBelowPhase4(t *testing.T) {
	for phase := 0; phase <= 3; phase++ {
		env := authServiceContainerEnv(renderPhase(t, phase, mailSets()...))
		for _, name := range authServiceMailNames {
			if _, found := env[name]; found {
				t.Fatalf("phase %d: auth-service renders %s; the mail settings start at phase 4", phase, name)
			}
		}
	}
}

func TestAuthServiceNeverRendersTheConsoleOverride(t *testing.T) {
	for phase := 0; phase <= 4; phase++ {
		if _, found := authServiceContainerEnv(renderPhase(t, phase, mailSets()...))["AUTH_MAIL_ALLOW_CONSOLE"]; found {
			t.Fatalf("phase %d: the chart renders AUTH_MAIL_ALLOW_CONSOLE", phase)
		}
	}
}

func TestAuthServiceExtraEnvRefusesTheMailNames(t *testing.T) {
	for _, name := range authServiceMailNames {
		for _, spelled := range []string{name, strings.ToLower(name)} {
			t.Run(spelled, func(t *testing.T) {
				extra := fmt.Sprintf(`authService.extraEnv=[{"name":%q,"value":"x"}]`, spelled)
				_, out, err := renderAuth(t, phaseArgs(4, append(mailSets(), "--set-json", extra)...)...)
				if err == nil || !strings.Contains(out, "authService.extraEnv must not set") {
					t.Fatalf("extraEnv %s was not refused: %v\n%.300s", spelled, err, out)
				}
			})
		}
	}
}

func TestAuthServicePhase4NeedsTheMailSecret(t *testing.T) {
	_, out, err := renderAuth(t, withoutMailSets(phaseArgs(4))...)
	if err == nil || !strings.Contains(out, "authService.mail.secretName") {
		t.Fatalf("phase 4 without authService.mail.secretName rendered: %v\n%.300s", err, out)
	}
	if _, out, err := renderAuth(t, withoutMailSets(phaseArgs(3))...); err != nil {
		t.Fatalf("phase 3 needs no mail Secret: %v\n%.300s", err, out)
	}
}

func TestAuthServiceMailKeyNamesAreValidated(t *testing.T) {
	for _, bad := range []string{"..x", "a b", "a/b"} {
		_, out, err := renderAuth(t, phaseArgs(4, mailSets("--set", "authService.mail.keys.provider="+bad)...)...)
		if err == nil {
			t.Fatalf("key name %q accepted\n%.200s", bad, out)
		}
	}
}

func authServiceEgressTargets(docs []map[string]any) []any {
	policy := authDoc(docs, "NetworkPolicy", "a-dev-health-auth-service")
	rules, _ := at(policy, "spec", "egress").([]any)
	return rules
}

func TestAuthServiceMailEgressNeedsPeers(t *testing.T) {
	noPeers := []string{"--set", "authService.mail.secretName=a-mail"}
	bare := func(phase int, extra ...string) []string { return append(withoutMailSets(phaseArgs(phase)), extra...) }
	_, out, err := renderAuth(t, bare(4, noPeers...)...)
	if err == nil || !strings.Contains(out, "authService.networkPolicy.egress.mail.to") {
		t.Fatalf("mail Secret set, policy on, no mail peers: %v\n%.300s", err, out)
	}
	if _, out, err := renderAuth(t, bare(3, noPeers...)...); err != nil {
		t.Fatalf("phase 3 needs no mail peers: %v\n%.300s", err, out)
	}
}

func TestAuthServiceMailEgressRuleRendersOnlyWithMailPeers(t *testing.T) {
	with := authServiceEgressTargets(renderPhase(t, 4, mailSets()...))
	var found bool
	for _, rule := range with {
		if strings.Contains(fmt.Sprint(at(rule, "to")), "mail-relay") {
			found = true
			if got := fmt.Sprint(at(rule, "ports")); !strings.Contains(got, "443") || !strings.Contains(got, "TCP") {
				t.Fatalf("mail egress ports = %s; want TCP 443", got)
			}
		}
	}
	if !found {
		t.Fatalf("no mail egress rule at phase 4: %v", with)
	}
	for phase := 1; phase <= 3; phase++ {
		for _, rule := range authServiceEgressTargets(renderPhase(t, phase, mailSets()...)) {
			if strings.Contains(fmt.Sprint(at(rule, "to")), "mail-relay") {
				t.Fatalf("phase %d renders the mail egress rule", phase)
			}
		}
	}
}

func TestAuthServiceMailEgressRefusesAWholeInternetPeer(t *testing.T) {
	for _, cidr := range []string{"0.0.0.0/0", "::/0", "0.0.0.0/1", "128.0.0.0/1", "0.0.0.0/7", "::/31", "::ffff:0:0/96", "::ffff:10.0.0.0/104", "64:ff9b::/96", "10.0.0.0", "10.0.0.0/a", "10.0.0.0/8/8"} {
		peers := fmt.Sprintf(`authService.networkPolicy.egress.mail.to=[{"ipBlock":{"cidr":%q}}]`, cidr)
		_, out, err := renderAuth(t, phaseArgs(4, "--set", "authService.mail.secretName=a-mail", "--set-json", peers)...)
		if err == nil || !strings.Contains(out, "authService.networkPolicy.egress.mail.to") {
			t.Fatalf("cidr %q accepted: %v\n%.200s", cidr, err, out)
		}
	}
	for _, cidr := range []string{"10.1.2.0/24", "203.0.113.7/32", "8.0.0.0/8", "2001:db8::/32"} {
		peers := fmt.Sprintf(`authService.networkPolicy.egress.mail.to=[{"ipBlock":{"cidr":%q}}]`, cidr)
		if _, out, err := renderAuth(t, phaseArgs(4, "--set", "authService.mail.secretName=a-mail", "--set-json", peers)...); err != nil {
			t.Fatalf("cidr %q refused: %v\n%.200s", cidr, err, out)
		}
	}
}

func TestAuthServiceMailEgressRefusesAnEmptyPeer(t *testing.T) {
	_, out, err := renderAuth(t, phaseArgs(4, "--set", "authService.mail.secretName=a-mail", "--set-json", `authService.networkPolicy.egress.mail.to=[{}]`)...)
	if err == nil || !strings.Contains(out, "authService.networkPolicy.egress.mail.to") {
		t.Fatalf("an empty peer was accepted: %v\n%.200s", err, out)
	}
}

var (
	mailV4Except = []string{"10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "100.64.0.0/10", "169.254.0.0/16", "127.0.0.0/8"}
	mailV6Except = []string{"fc00::/7", "fe80::/10", "::1/128", "::ffff:0:0/96", "64:ff9b::/96"}
)

func mailPeer(cidr string, except []string) string {
	quoted := make([]string, len(except))
	for i, e := range except {
		quoted[i] = fmt.Sprintf("%q", e)
	}
	return fmt.Sprintf(`{"ipBlock":{"cidr":%q,"except":[%s]}}`, cidr, strings.Join(quoted, ","))
}

func mailRender(t *testing.T, peers []string, extra ...string) (string, error) {
	t.Helper()
	args := append([]string{"--set", "authService.mail.secretName=a-mail", "--set-json", "authService.networkPolicy.egress.mail.to=[" + strings.Join(peers, ",") + "]"}, extra...)
	_, out, err := renderAuth(t, phaseArgs(4, args...)...)
	return out, err
}

func without(list []string, drop string) []string {
	var out []string
	for _, e := range list {
		if e != drop {
			out = append(out, e)
		}
	}
	return out
}

func TestAuthServiceMailEgressAcceptsTheWholeInternetOnlyInTheExactShape(t *testing.T) {
	v4, v6 := mailPeer("0.0.0.0/0", mailV4Except), mailPeer("::/0", mailV6Except)
	for name, peers := range map[string][]string{"v4": {v4}, "v6": {v6}, "both": {v4, v6}} {
		if out, err := mailRender(t, peers); err != nil {
			t.Fatalf("%s: the exact shape was refused: %v\n%.300s", name, err, out)
		}
	}
	extraEntries := append(append([]string{}, mailV4Except...), "203.0.113.0/24")
	if out, err := mailRender(t, []string{mailPeer("0.0.0.0/0", extraEntries)}); err != nil {
		t.Fatalf("a longer except list was refused: %v\n%.300s", err, out)
	}
}

func TestAuthServiceMailEgressRefusesAWholeInternetPeerMissingAnExceptEntry(t *testing.T) {
	for _, drop := range mailV4Except {
		if out, err := mailRender(t, []string{mailPeer("0.0.0.0/0", without(mailV4Except, drop))}); err == nil || !strings.Contains(out, "authService.networkPolicy.egress.mail.to") {
			t.Fatalf("v4 /0 without except %s accepted: %v\n%.200s", drop, err, out)
		}
	}
	for _, drop := range mailV6Except {
		if out, err := mailRender(t, []string{mailPeer("::/0", without(mailV6Except, drop))}); err == nil || !strings.Contains(out, "authService.networkPolicy.egress.mail.to") {
			t.Fatalf("v6 /0 without except %s accepted: %v\n%.200s", drop, err, out)
		}
	}
	if out, err := mailRender(t, []string{`{"ipBlock":{"cidr":"0.0.0.0/0"}}`}); err == nil {
		t.Fatalf("a /0 with no except list accepted\n%.200s", out)
	}
}

func TestAuthServiceMailEgressRefusesAWholeInternetPeerOnAnyOtherPort(t *testing.T) {
	v4 := mailPeer("0.0.0.0/0", mailV4Except)
	for name, ports := range map[string]string{
		"port 80":    `[{"protocol":"TCP","port":80}]`,
		"port 25":    `[{"protocol":"TCP","port":25}]`,
		"extra port": `[{"protocol":"TCP","port":443},{"protocol":"TCP","port":25}]`,
		"udp 443":    `[{"protocol":"UDP","port":443}]`,
		"no ports":   `[]`,
		"port range": `[{"protocol":"TCP","port":443,"endPort":465}]`,
	} {
		out, err := mailRender(t, []string{v4}, "--set-json", "authService.networkPolicy.egress.mail.ports="+ports)
		if err == nil || !strings.Contains(out, "authService.networkPolicy.egress.mail") {
			t.Fatalf("%s: a /0 peer accepted: %v\n%.200s", name, err, out)
		}
	}
	if out, err := mailRender(t, []string{mailPeer("10.1.2.0/24", nil)}, "--set-json", `authService.networkPolicy.egress.mail.ports=[{"protocol":"TCP","port":587}]`); err != nil {
		t.Fatalf("a narrow peer on 587 was refused: %v\n%.200s", err, out)
	}
}

func TestAuthServiceMailEgressRefusesAnotherSlashZeroForm(t *testing.T) {
	for _, cidr := range []string{"1.2.3.4/0", "0:0:0:0:0:0:0:0/0", "::1/0"} {
		except := mailV4Except
		if strings.Contains(cidr, ":") {
			except = mailV6Except
		}
		if out, err := mailRender(t, []string{mailPeer(cidr, except)}); err == nil {
			t.Fatalf("%q accepted\n%.200s", cidr, out)
		}
	}
}

// chartSecretMail points the mail setting at the Secret the chart creates (secrets.create is the default),
// whose content the render can read.
func chartSecretMail(data map[string]string) []string {
	args := []string{"--set", "authService.mail.secretName=a-dev-health-secrets"}
	for key, value := range data {
		args = append(args, "--set", fmt.Sprintf("secrets.data.%s=%s", key, value))
	}
	return args
}

var resendMail = map[string]string{
	"EMAIL_PROVIDER": "resend", "EMAIL_FROM_ADDRESS": "noreply@example.com", "APP_BASE_URL": "https://app.example.com", "EMAIL_API_KEY": "k",
}

func mailWith(base map[string]string, set map[string]string, drop ...string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for k, v := range set {
		out[k] = v
	}
	for _, k := range drop {
		delete(out, k)
	}
	return out
}

func TestAuthServiceChartSecretMailRendersWithAUsableSetting(t *testing.T) {
	for name, data := range map[string]map[string]string{
		"email key":  resendMail,
		"resend key": mailWith(resendMail, map[string]string{"RESEND_API_KEY": "k"}, "EMAIL_API_KEY"),
	} {
		if _, out, err := renderAuth(t, phaseArgs(4, chartSecretMail(data)...)...); err != nil {
			t.Fatalf("%s: a usable setting was refused: %v\n%.300s", name, err, out)
		}
	}
}

func TestAuthServiceChartSecretMailRefusesAnUnusableSetting(t *testing.T) {
	cases := map[string]struct {
		data map[string]string
		want string
	}{
		"no from address":    {mailWith(resendMail, nil, "EMAIL_FROM_ADDRESS"), "EMAIL_FROM_ADDRESS"},
		"empty from address": {mailWith(resendMail, map[string]string{"EMAIL_FROM_ADDRESS": ""}), "EMAIL_FROM_ADDRESS"},
		"no base url":        {mailWith(resendMail, nil, "APP_BASE_URL"), "APP_BASE_URL"},
		"no provider":        {mailWith(resendMail, nil, "EMAIL_PROVIDER"), "EMAIL_PROVIDER"},
		"console provider":   {mailWith(resendMail, map[string]string{"EMAIL_PROVIDER": "console"}), "console"},
		"upper console":      {mailWith(resendMail, map[string]string{"EMAIL_PROVIDER": "CONSOLE"}), "console"},
		"smtp provider":      {mailWith(resendMail, map[string]string{"EMAIL_PROVIDER": "smtp"}), "the chart wires resend only"},
		"unknown provider":   {mailWith(resendMail, map[string]string{"EMAIL_PROVIDER": "pigeon"}), "EMAIL_PROVIDER"},
		"resend with no key": {mailWith(resendMail, nil, "EMAIL_API_KEY"), "EMAIL_API_KEY"},
	}
	for name, tc := range cases {
		t.Run(strings.ReplaceAll(name, " ", "_"), func(t *testing.T) {
			_, out, err := renderAuth(t, phaseArgs(4, chartSecretMail(tc.data)...)...)
			if err == nil || !strings.Contains(out, tc.want) || !strings.Contains(out, "secrets.data") {
				t.Fatalf("%s: err=%v want %q\n%.300s", name, err, tc.want, out)
			}
		})
	}
}

func TestAuthServiceChartSecretMailCheckIsScoped(t *testing.T) {
	bad := mailWith(resendMail, map[string]string{"EMAIL_PROVIDER": "console"})
	if _, out, err := renderAuth(t, phaseArgs(3, chartSecretMail(bad)...)...); err != nil {
		t.Fatalf("phase 3 is not checked: %v\n%.300s", err, out)
	}
	if _, out, err := renderAuth(t, phaseArgs(4, append(chartSecretMail(bad), "--set", "authService.mail.secretName=a-other")...)...); err != nil {
		t.Fatalf("another Secret is not checked: %v\n%.300s", err, out)
	}
	if _, out, err := renderAuth(t, phaseArgs(4, append(chartSecretMail(bad), "--set", "secrets.create=false", "--set", "secrets.externalSecretName=a-dev-health-secrets", "--set", "hs256Secret.externalSecretName=a-hs256")...)...); err != nil {
		t.Fatalf("an external Secret is not checked: %v\n%.700s", err, out)
	}
}

// A literal under a *_KEY name is refused by the secret-like rule whatever the name list says; a secretKeyRef is
// refused only by the list, so this is the test of the list.
func TestAuthServiceExtraEnvRefusesTheMailNamesAsSecretReferences(t *testing.T) {
	for _, name := range authServiceMailNames {
		extra := fmt.Sprintf(`authService.extraEnv=[{"name":%q,"valueFrom":{"secretKeyRef":{"name":"x","key":"y"}}}]`, name)
		_, out, err := renderAuth(t, phaseArgs(4, "--set-json", extra)...)
		if err == nil || !strings.Contains(out, "authService.extraEnv must not set") {
			t.Fatalf("extraEnv secretKeyRef %s was not refused: %v\n%.300s", name, err, out)
		}
	}
}

func TestAuthServiceMailEgressRefusesAWholeInternetPeerBesideAnyOtherPeer(t *testing.T) {
	v4, v6 := mailPeer("0.0.0.0/0", mailV4Except), mailPeer("::/0", mailV6Except)
	for name, peers := range map[string][]string{
		"narrow cidr":  {v4, mailPeer("10.1.2.0/24", nil)},
		"pod selector": {v4, `{"podSelector":{"matchLabels":{"app":"mail-relay"}}}`},
		"both and one": {v4, v6, mailPeer("203.0.113.7/32", nil)},
		"v6 and pod":   {`{"podSelector":{"matchLabels":{"app":"mail-relay"}}}`, v6},
	} {
		out, err := mailRender(t, peers)
		if err == nil || !strings.Contains(out, "authService.networkPolicy.egress.mail.to") {
			t.Fatalf("%s: a mixed list was accepted: %v\n%.200s", name, err, out)
		}
	}
	if out, err := mailRender(t, []string{v4, v6}); err != nil {
		t.Fatalf("the v4 and v6 pair alone was refused: %v\n%.200s", err, out)
	}
}

func TestAuthServiceMailEgressRefusesAPrefixThatContainsTheMappedOrNAT64Range(t *testing.T) {
	for _, cidr := range []string{"::/32", "0:0::/64", "64:ff9b::/32", "::/33", "0::/48", "::ffff:0:0/80", "64:ff9b::/40", "0:0:0:0:0:ffff::/96", "64:ff9b:1::/48", "::ffff:10.0.0.0/104"} {
		peers := fmt.Sprintf(`authService.networkPolicy.egress.mail.to=[{"ipBlock":{"cidr":%q}}]`, cidr)
		_, out, err := renderAuth(t, phaseArgs(4, "--set", "authService.mail.secretName=a-mail", "--set-json", peers)...)
		if err == nil || !strings.Contains(out, "authService.networkPolicy.egress.mail.to") {
			t.Fatalf("cidr %q accepted: %v\n%.200s", cidr, err, out)
		}
	}
}

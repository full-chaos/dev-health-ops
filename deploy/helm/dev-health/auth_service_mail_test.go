package devhealth_test

import (
	"fmt"
	"strings"
	"testing"
)

var authServiceMailNames = []string{"EMAIL_PROVIDER", "EMAIL_FROM_ADDRESS", "EMAIL_API_KEY", "APP_BASE_URL", "AUTH_MAIL_ALLOW_CONSOLE"}

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
	_, out, err := renderAuth(t, phaseArgs(4, noPeers...)...)
	if err == nil || !strings.Contains(out, "authService.networkPolicy.egress.mail.to") {
		t.Fatalf("mail Secret set, policy on, no mail peers: %v\n%.300s", err, out)
	}
	if _, out, err := renderAuth(t, phaseArgs(4, append(noPeers, "--set", "authService.networkPolicy.enabled=false")...)...); err != nil {
		t.Fatalf("policy off needs no peers: %v\n%.300s", err, out)
	}
	if _, out, err := renderAuth(t, phaseArgs(3, noPeers...)...); err != nil {
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

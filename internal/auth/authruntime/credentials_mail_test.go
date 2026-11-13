package authruntime

import (
	"bytes"
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"fmt"
	"log/slog"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/full-chaos/dev-health-ops/internal/mail"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

var (
	authruntimeTestIssuerCert *x509.Certificate
	authruntimeTestIssuerTLS  tls.Certificate
)

var mailEnvNames = []string{
	"EMAIL_PROVIDER", "EMAIL_FROM_ADDRESS", "EMAIL_API_KEY", "RESEND_API_KEY", "SMTP_HOST", "SMTP_PORT",
	"APP_BASE_URL", "AUTH_MAIL_ALLOW_CONSOLE", "SMTP_USE_TLS", "SMTP_TLS_CA_FILE", "SMTP_TLS_SERVER_NAME",
	"SMTP_USERNAME", "SMTP_PASSWORD",
}

// mailEnv sets exactly the given variables; every other mail variable is unset (not empty) for the test.
func mailEnv(t *testing.T, set map[string]string) {
	t.Helper()
	for _, name := range mailEnvNames {
		previous, had := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() {
			if had {
				_ = os.Setenv(name, previous)
			} else {
				_ = os.Unsetenv(name)
			}
		})
	}
	for name, value := range set {
		t.Setenv(name, value)
	}
}

func smtpMail() map[string]string {
	return map[string]string{
		"EMAIL_PROVIDER": "smtp", "EMAIL_FROM_ADDRESS": "noreply@example.com",
		"SMTP_HOST": "smtp.example.com", "APP_BASE_URL": "https://app.example.com",
	}
}

func without(base map[string]string, drop ...string) map[string]string {
	out := map[string]string{}
	for k, v := range base {
		out[k] = v
	}
	for _, name := range drop {
		delete(out, name)
	}
	return out
}

func with(base map[string]string, k, v string) map[string]string {
	out := without(base)
	out[k] = v
	return out
}

func ownedRoutes(t *testing.T, logs *bytes.Buffer) error {
	t.Helper()
	pool, err := pgxpool.New(context.Background(), "postgres://auth@127.0.0.1:1/devhealth")
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	schema, err := authschema.NewValidatedIdentifier("auth")
	if err != nil {
		t.Fatal(err)
	}
	logger := slog.New(slog.NewTextHandler(logs, nil))
	_, components, err := credentialRoutes(credentialConfig(t, "true"), logger, pool, schema, mustLimits(t), &stubSigner{})
	for _, c := range components {
		_ = c.Shutdown(context.Background())
	}
	return err
}

func wantRefusal(t *testing.T, env map[string]string, name string, sentinels ...string) {
	t.Helper()
	mailEnv(t, env)
	var logs bytes.Buffer
	err := ownedRoutes(t, &logs)
	if err == nil {
		t.Fatalf("users owned started without a usable mail setting; want a refusal naming %s", name)
	}
	if !strings.Contains(err.Error(), name) {
		t.Fatalf("refusal %q does not name %s", err, name)
	}
	for _, sentinel := range sentinels {
		if strings.Contains(err.Error(), sentinel) || strings.Contains(logs.String(), sentinel) {
			t.Fatalf("refusal or log carries the value %q", sentinel)
		}
	}
}

func TestOwnedUsersRefuseTheConsoleProviderWhenNoneIsSet(t *testing.T) {
	wantRefusal(t, without(smtpMail(), "EMAIL_PROVIDER", "SMTP_HOST"), "EMAIL_PROVIDER")
}

func TestOwnedUsersRefuseTheExplicitConsoleProvider(t *testing.T) {
	wantRefusal(t, with(smtpMail(), "EMAIL_PROVIDER", "console"), "EMAIL_PROVIDER")
}

func TestOwnedUsersRefuseTheOverrideUnlessExactlyTrue(t *testing.T) {
	env := with(with(smtpMail(), "EMAIL_PROVIDER", "console"), "AUTH_MAIL_ALLOW_CONSOLE", "yes")
	wantRefusal(t, env, "EMAIL_PROVIDER")
}

func TestOwnedUsersRefuseResendWithoutAKey(t *testing.T) {
	wantRefusal(t, with(without(smtpMail(), "SMTP_HOST"), "EMAIL_PROVIDER", "resend"), "EMAIL_API_KEY")
}

func TestOwnedUsersRefuseSMTPWithoutAHost(t *testing.T) {
	wantRefusal(t, without(smtpMail(), "SMTP_HOST"), "SMTP_HOST")
}

func TestOwnedUsersRefuseAnAbsentFromAddress(t *testing.T) {
	wantRefusal(t, without(smtpMail(), "EMAIL_FROM_ADDRESS"), "EMAIL_FROM_ADDRESS")
}

func TestOwnedUsersRefuseAnEmptyFromAddress(t *testing.T) {
	wantRefusal(t, with(smtpMail(), "EMAIL_FROM_ADDRESS", ""), "EMAIL_FROM_ADDRESS")
}

func TestOwnedUsersRefuseAnAbsentAppBaseURL(t *testing.T) {
	wantRefusal(t, without(smtpMail(), "APP_BASE_URL"), "APP_BASE_URL")
}

func TestOwnedUsersRefuseAnEmptyAppBaseURL(t *testing.T) {
	wantRefusal(t, with(smtpMail(), "APP_BASE_URL", ""), "APP_BASE_URL")
}

func TestOwnedUsersRefuseAnAppBaseURLThatIsNotAnAbsoluteHTTPURL(t *testing.T) {
	for name, value := range map[string]string{
		"ftp scheme":   "ftp://sentinel-host.invalid",
		"relative":     "/sentinel/path",
		"no scheme":    "sentinel-host.invalid",
		"no host":      "https://",
		"scheme only":  "http:sentinel",
		"unparseable":  "http://sentinel host.invalid",
		"blank spaces": "   ",
	} {
		t.Run(name, func(t *testing.T) {
			wantRefusal(t, with(smtpMail(), "APP_BASE_URL", value), "APP_BASE_URL", "sentinel")
		})
	}
}

func TestOwnedUsersStartWithARealProviderAndNoWarning(t *testing.T) {
	mailEnv(t, smtpMail())
	var logs bytes.Buffer
	if err := ownedRoutes(t, &logs); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(logs.String(), "AUTH_MAIL_ALLOW_CONSOLE") {
		t.Fatalf("a real provider logged the override warning: %s", logs.String())
	}
}

func TestOwnedUsersHonourTheConsoleOverrideWithOneWarning(t *testing.T) {
	mailEnv(t, with(with(smtpMail(), "EMAIL_PROVIDER", "console"), "AUTH_MAIL_ALLOW_CONSOLE", "true"))
	var logs bytes.Buffer
	if err := ownedRoutes(t, &logs); err != nil {
		t.Fatal(err)
	}
	if got := strings.Count(logs.String(), "AUTH_MAIL_ALLOW_CONSOLE"); got != 1 {
		t.Fatalf("override warnings = %d; want exactly 1: %s", got, logs.String())
	}
	if !strings.Contains(logs.String(), "level=WARN") {
		t.Fatalf("the override line is not a WARN: %s", logs.String())
	}
}

func TestOwnedUsersStillNeedAppBaseURLUnderTheConsoleOverride(t *testing.T) {
	env := with(with(without(smtpMail(), "APP_BASE_URL"), "EMAIL_PROVIDER", "console"), "AUTH_MAIL_ALLOW_CONSOLE", "true")
	wantRefusal(t, env, "APP_BASE_URL")
}

// TestMain gives every test in the package a real mail provider setting, so a users-owned start-up reaches the
// code under test; the mail tests above replace it with mailEnv.
func TestMain(m *testing.M) {
	for name, value := range smtpMail() {
		_ = os.Setenv(name, value)
	}
	// The service initialises tracing from the process environment; a test
	// that wants spans turns it on and points it at its own receiver.
	if _, set := os.LookupEnv("OTEL_ENABLED"); !set {
		_ = os.Setenv("OTEL_ENABLED", "false")
	}
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		panic(fmt.Errorf("generate test issuer key: %w", err))
	}
	now := time.Now()
	template := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		NotBefore:    now.Add(-time.Minute),
		NotAfter:     now.Add(24 * time.Hour),
		KeyUsage:     x509.KeyUsageDigitalSignature,
		ExtKeyUsage:  []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		IPAddresses:  []net.IP{net.ParseIP("127.0.0.1")},
		DNSNames:     []string{"localhost"},
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		panic(fmt.Errorf("create test issuer certificate: %w", err))
	}
	authruntimeTestIssuerCert, err = x509.ParseCertificate(der)
	if err != nil {
		panic(fmt.Errorf("parse test issuer certificate: %w", err))
	}
	authruntimeTestIssuerTLS = tls.Certificate{Certificate: [][]byte{der}, PrivateKey: key, Leaf: authruntimeTestIssuerCert}
	dir, err := os.MkdirTemp("", "authruntime-issuer-")
	if err != nil {
		panic(fmt.Errorf("create test issuer directory: %w", err))
	}
	certFile := filepath.Join(dir, "issuer.pem")
	if err := os.WriteFile(certFile, pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: der}), 0o600); err != nil {
		panic(fmt.Errorf("write test issuer certificate: %w", err))
	}
	// SSL_CERT_FILE supplies the JWKS client's shared transport with trust through the lazily loaded system pool.
	if err := os.Setenv("SSL_CERT_FILE", certFile); err != nil {
		panic(fmt.Errorf("set test issuer trust: %w", err))
	}
	pool, err := x509.SystemCertPool()
	if err != nil {
		fmt.Fprintln(os.Stderr, "issuer trust not seen by the system pool")
		os.Exit(1)
	}
	if _, err := authruntimeTestIssuerCert.Verify(x509.VerifyOptions{Roots: pool, DNSName: "127.0.0.1"}); err != nil {
		fmt.Fprintln(os.Stderr, "issuer trust not seen by the system pool")
		os.Exit(1)
	}
	code := m.Run()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func TestOwnedUsersRefuseAnHTTPAppBaseURLWithoutTheOverride(t *testing.T) {
	wantRefusal(t, with(smtpMail(), "APP_BASE_URL", "http://app.example.com"), "APP_BASE_URL")
}

func TestOwnedUsersAcceptAnHTTPAppBaseURLUnderTheOverride(t *testing.T) {
	mailEnv(t, with(with(with(smtpMail(), "EMAIL_PROVIDER", "console"), "AUTH_MAIL_ALLOW_CONSOLE", "true"), "APP_BASE_URL", "http://app.example.com"))
	var logs bytes.Buffer
	if err := ownedRoutes(t, &logs); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedUsersRefuseAnAppBaseURLWithUserinfo(t *testing.T) {
	wantRefusal(t, with(smtpMail(), "APP_BASE_URL", "https://sentinel-user:sentinel-pass@app.example.com"), "APP_BASE_URL", "sentinel")
}

// capturedDefaultLog routes the process-wide slog default (where internal/mail logs) into a buffer.
func capturedDefaultLog(t *testing.T) *bytes.Buffer {
	t.Helper()
	var buf bytes.Buffer
	previous := slog.Default()
	slog.SetDefault(slog.New(slog.NewTextHandler(&buf, nil)))
	t.Cleanup(func() { slog.SetDefault(previous) })
	return &buf
}

func wantValueFreeRefusal(t *testing.T, env map[string]string, name, planted string) {
	t.Helper()
	mailEnv(t, env)
	global := capturedDefaultLog(t)
	var logs bytes.Buffer
	err := ownedRoutes(t, &logs)
	if err == nil {
		t.Fatalf("started with a bad %s", name)
	}
	if !strings.Contains(err.Error(), name) {
		t.Fatalf("error %q does not name %s", err, name)
	}
	for label, text := range map[string]string{"error": err.Error(), "logger": logs.String(), "default log": global.String()} {
		if strings.Contains(text, planted) {
			t.Fatalf("%s holds the planted value: %s", label, text)
		}
	}
}

func TestOwnedUsersRefuseAPaddedAppBaseURL(t *testing.T) {
	for _, v := range []string{" https://x.test", "https://x.test ", " https://x.test ", "https://x.test\n", "\thttps://x.test"} {
		wantRefusal(t, with(smtpMail(), "APP_BASE_URL", v), "APP_BASE_URL")
	}
}

func TestOwnedUsersBuildTheExactLinkFromACleanAppBaseURL(t *testing.T) {
	mailEnv(t, with(smtpMail(), "APP_BASE_URL", "https://x.test"))
	var logs bytes.Buffer
	if err := ownedRoutes(t, &logs); err != nil {
		t.Fatal(err)
	}
}

func TestOwnedUsersUnsupportedProviderNeverReachesTheConstructorLog(t *testing.T) {
	wantValueFreeRefusal(t, with(smtpMail(), "EMAIL_PROVIDER", "sentinel-provider"), "EMAIL_PROVIDER", "sentinel-provider")
}

func TestOwnedUsersBadSMTPPortIsValueFree(t *testing.T) {
	wantValueFreeRefusal(t, with(smtpMail(), "SMTP_PORT", "sentinel-port"), "SMTP_PORT", "sentinel-port")
}

func TestOwnedUsersBadCAFileIsValueFree(t *testing.T) {
	env := with(with(smtpMail(), "SMTP_USE_TLS", "true"), "SMTP_TLS_CA_FILE", "/nonexistent/sentinel-ca.pem")
	wantValueFreeRefusal(t, env, "SMTP_TLS_CA_FILE", "sentinel-ca")
}

func TestOwnedUsersAnyOtherConstructorFaultIsGeneric(t *testing.T) {
	env := with(smtpMail(), "SMTP_TLS_SERVER_NAME", "")
	wantValueFreeRefusal(t, env, "the mail settings do not load", "sentinel")
}

func TestTheConsoleOverrideNeverLogsTheSenderAddress(t *testing.T) {
	mailEnv(t, with(with(smtpMail(), "EMAIL_PROVIDER", "console"), "AUTH_MAIL_ALLOW_CONSOLE", "true"))
	t.Setenv("EMAIL_FROM_ADDRESS", "sentinel-sender@example.com")
	global := capturedDefaultLog(t)
	provider, err := ownedUsersMailProvider(slog.New(slog.NewTextHandler(&bytes.Buffer{}, nil)))
	if err != nil {
		t.Fatal(err)
	}
	if err := quietProvider(provider).Send(context.Background(), mail.Message{To: "sentinel-rcpt@example.com", Subject: "s"}); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(global.String(), "sentinel") || !strings.Contains(global.String(), "console provider") {
		t.Fatalf("console log: %s", global.String())
	}
}

func TestOwnedUsersRefuseAnAppBaseURLWithAQueryFragmentOrPath(t *testing.T) {
	for _, v := range []string{"https://x.test/?a=1", "https://x.test/#f", "https://x.test/p?q", "https://x.test?", "https://x.test/#", "https://x.test/app", "https://x.test/app/", "https://x.test//"} {
		wantRefusal(t, with(smtpMail(), "APP_BASE_URL", v), "APP_BASE_URL", "x.test")
	}
}

func TestOwnedUsersStartWithAnOriginOnlyAppBaseURL(t *testing.T) {
	for _, v := range []string{"https://x.test", "https://x.test/", "https://x.test:8443"} {
		mailEnv(t, with(smtpMail(), "APP_BASE_URL", v))
		var logs bytes.Buffer
		if err := ownedRoutes(t, &logs); err != nil {
			t.Fatalf("%q: %v", v, err)
		}
	}
}

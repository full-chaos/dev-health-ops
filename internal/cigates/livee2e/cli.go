package livee2e

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/signingkey"
)

const usage = `usage: cigate live-e2e <verb>
  assert-health FILE | assert-meta FILE | assert-home FILE
  assert-login-token FILE ISSUER KID     (FILE holds the login answer; prints one proof line)
  edge-request DOCUMENTS OPERATION ORG
  mint-token                             (env E2E_SESSION_ID, E2E_ISSUER_URL, E2E_ISSUER_KEY_FILE; optional E2E_USER_ID, E2E_ORG_ID, E2E_ISSUER_KID)
  mint-license PUBLIC_FILE LICENSE_FILE   (throwaway enterprise licence; files must not exist)
  seed-sso-provider EMAIL PROTOCOL [flags]   (TEST-ONLY stand-in for the missing provider create route, CHAOS-13636; PROTOCOL oidc|saml; env PG*, SETTINGS_ENCRYPTION_KEY, SSO_CLIENT_SECRET (oidc); prints the provider id only)
  valkey-ping                            (env REDIS_URL)
  subject-jws FILE                       (test subject token for the fake TokenReview; FILE must not exist)
  reviewer-token FILE                    (random reviewer bearer for the fake TokenReview; FILE must not exist)
  smtp-sink ADDR DIR PORTFILE             (loopback SMTP capture; blocks)
  mail-token DIR EMAIL KIND              (KIND reset|verify; prints only the token)
  tokenreview-serve ADDR CERT KEY SUBJECT_FILE REVIEWER_FILE   (env TR_NAMESPACE, TR_SERVICE_ACCOUNT, TR_UID; blocks)
`

// Run is `cigate live-e2e`. Exit codes: 0 ok, 1 a check or step failed, 2 usage.
func Run(args []string, getenv func(string) string, stdout, stderr io.Writer) int {
	if len(args) == 0 {
		fmt.Fprint(stderr, usage)
		return 2
	}
	timeout := 2 * time.Minute
	if args[0] == "tokenreview-serve" || args[0] == "smtp-sink" {
		timeout = 24 * time.Hour
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	fail := func(err error) int {
		fmt.Fprintf(stderr, "live-e2e %s: %s\n", args[0], err)
		return 1
	}
	need := func(n int) bool {
		if len(args) != n+1 {
			fmt.Fprint(stderr, usage)
			return false
		}
		return true
	}
	switch args[0] {
	case "assert-health", "assert-meta", "assert-home":
		if !need(1) {
			return 2
		}
		payload, err := os.ReadFile(args[1])
		if err != nil {
			return fail(err)
		}
		switch args[0] {
		case "assert-health":
			err = AssertHealth(payload)
		case "assert-meta":
			err = AssertMeta(payload)
		default:
			err = AssertHome(payload, getenv("FIXTURE_PROVIDER"))
		}
		if err != nil {
			return fail(err)
		}
		return 0
	case "assert-login-token":
		if !need(3) {
			return 2
		}
		payload, err := os.ReadFile(args[1])
		if err != nil {
			return fail(err)
		}
		line, err := AssertLoginToken(payload, args[2], args[3], time.Now())
		if err != nil {
			return fail(err)
		}
		fmt.Fprintln(stdout, line)
		return 0
	case "edge-request":
		if !need(3) {
			return 2
		}
		documents, err := os.ReadFile(args[1])
		if err != nil {
			return fail(err)
		}
		body, err := EdgeRequest(documents, args[2], args[3])
		if err != nil {
			return fail(err)
		}
		_, _ = stdout.Write(body)
		return 0
	case "mint-token":
		if !need(0) {
			return 2
		}
		userID, err := uuidEnv(getenv, "E2E_USER_ID", E2EUserID)
		if err != nil {
			return fail(err)
		}
		orgID, err := uuidEnv(getenv, "E2E_ORG_ID", DefaultOrgID)
		if err != nil {
			return fail(err)
		}
		sessionID, err := uuidEnv(getenv, "E2E_SESSION_ID", "")
		if err != nil {
			return fail(err)
		}
		issuer := getenv("E2E_ISSUER_URL")
		if issuer == "" {
			return fail(errors.New("E2E_ISSUER_URL is not set"))
		}
		kid := getenv("E2E_ISSUER_KID")
		if kid == "" {
			kid = DefaultIssuerKeyID
		}
		key, err := signingkey.LoadPrivateKeyFromFile(getenv("E2E_ISSUER_KEY_FILE"))
		if err != nil {
			return fail(err)
		}
		token, err := MintToken(key, kid, issuer, userID, orgID, sessionID, time.Now().UTC(), uuid.NewString())
		if err != nil {
			return fail(err)
		}
		fmt.Fprintln(stdout, token)
		return 0
	case "seed-sso-provider":
		if len(args) < 3 {
			fmt.Fprint(stderr, usage)
			return 2
		}
		if err := seedSSOProvider(ctx, args[1:], getenv, stdout, openSSODB); err != nil {
			return fail(err)
		}
		return 0
	case "mint-license":
		if !need(2) {
			return 2
		}
		if err := mintLicense(args[1], args[2]); err != nil {
			return fail(err)
		}
		return 0
	case "subject-jws", "reviewer-token":
		if !need(1) {
			return 2
		}
		value, err := newTestSecret(args[0])
		if err != nil {
			return fail(err)
		}
		if err := WriteNewFile(args[1], value); err != nil {
			return fail(err)
		}
		return 0
	case "tokenreview-serve":
		if !need(5) {
			return 2
		}
		cfg := TokenReviewConfig{
			Audience: ReviewAudience, Namespace: getenv("TR_NAMESPACE"), ServiceAccount: getenv("TR_SERVICE_ACCOUNT"), UID: getenv("TR_UID"),
		}
		if cfg.Namespace == "" || cfg.ServiceAccount == "" || cfg.UID == "" {
			return fail(errors.New("TR_NAMESPACE, TR_SERVICE_ACCOUNT and TR_UID are required"))
		}
		if err := ServeTokenReview(ctx, args[1], args[2], args[3], args[4], args[5], cfg); err != nil {
			return fail(err)
		}
		return 0
	case "smtp-sink":
		if !need(3) {
			return 2
		}
		ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
		defer stop()
		if err := ServeSMTPSink(ctx, args[1], args[2], args[3], stderr); err != nil {
			return fail(err)
		}
		return 0
	case "mail-token":
		if !need(3) {
			return 2
		}
		token, err := readMailToken(args[1], args[2], args[3])
		if err != nil {
			return fail(err)
		}
		fmt.Fprintln(stdout, token)
		return 0
	case "valkey-ping":
		if !need(0) {
			return 2
		}
		if err := ValkeyPing(ctx, getenv("REDIS_URL")); err != nil {
			return fail(err)
		}
		return 0
	}
	fmt.Fprint(stderr, usage)
	return 2
}

// uuidEnv reads name as a UUID; an unset name takes fallback, and an empty fallback makes it required.
func uuidEnv(getenv func(string) string, name, fallback string) (uuid.UUID, error) {
	raw := getenv(name)
	if raw == "" {
		raw = fallback
	}
	if raw == "" {
		return uuid.UUID{}, fmt.Errorf("%s is not set", name)
	}
	id, err := uuid.Parse(raw)
	if err != nil {
		return uuid.UUID{}, fmt.Errorf("%s is not a UUID", name)
	}
	return id, nil
}

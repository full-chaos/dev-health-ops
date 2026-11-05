package apiservice

import (
	"context"
	"encoding/base64"
	"fmt"
	"log/slog"
	"regexp"
	"strings"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/apiservice/acr"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

const (
	envAccessTokenIssuer   = "AUTH_ACCESS_TOKEN_ISSUER"
	accessTokenAudienceAPI = "dev-health-api"
	// envACRWorkloadSubject pins the one workload principal (prn_) the
	// internal acr routes accept. An interim pin: the workload registry
	// replaces it (CHAOS-10980).
	envACRWorkloadSubject = "AUTH_ACR_WORKLOAD_SUBJECT"
)

// buildAccessVerify constructs the profile-routed access-token validator over
// the FULL go-api endpoint-profile row table. AUTH_ACCESS_TOKEN_ISSUER unset:
// no capability (nil), and go-api's access-token path is unchanged.
//
// It also returns the JWKS client it built, so the workload verifier of the
// internal acr routes reads the same keys (nil when the issuer is unset).
func buildAccessVerify(lookup func(string) (string, bool), auth *policy.Authenticator, logger *slog.Logger) (*accessverify.Validator, authverify.KeySource, error) {
	issuer, _ := lookup(envAccessTokenIssuer)
	issuer = strings.TrimSpace(issuer)
	if issuer == "" {
		return nil, nil, nil
	}
	keys, err := authverify.NewJWKSClient(authverify.JWKSClientConfig{Issuer: issuer})
	if err != nil {
		return nil, nil, err
	}
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: issuer, Audience: accessTokenAudienceAPI, Keys: keys})
	if err != nil {
		return nil, nil, err
	}
	rows, err := goAPIAccessRows()
	if err != nil {
		return nil, nil, err
	}
	cfg := accessverify.Config{
		Rows:   rows,
		EdDSA:  verifier,
		Meter:  otel.Meter("github.com/full-chaos/dev-health-ops/internal/auth/accessverify"),
		Logger: logger,
	}
	if auth != nil {
		cfg.Legacy = legacyVerifier(auth.AuthenticateLegacy)
	}
	validator, err := accessverify.New(cfg)
	return validator, keys, err
}

// principalIDPattern is the principal id of the credential contracts
// (contracts/auth/v1/jsonschema/credential-entry.v1.schema.json).
var principalIDPattern = regexp.MustCompile(`^prn_[A-Za-z0-9_-]{1,64}$`)

// acrWorkloadSubjectInvalidMetric is the gauge of a configured AUTH_ACR_WORKLOAD_SUBJECT that is not in the session
// wire form (CHAOS-12402): 1 when it is not, else 0. Tests replace the recorder.
const acrWorkloadSubjectInvalidMetric = "dev_health_api_acr_workload_subject_config_invalid"

var recordACRWorkloadSubjectInvalid = newACRWorkloadSubjectRecorder(otel.Meter("github.com/full-chaos/dev-health-ops/internal/apiservice/accessverify"))

func newACRWorkloadSubjectRecorder(meter metric.Meter) func(context.Context, bool) {
	gauge, err := meter.Int64Gauge(acrWorkloadSubjectInvalidMetric,
		metric.WithDescription("1 when AUTH_ACR_WORKLOAD_SUBJECT is set but not in the session wire form (prn_ and 22 base64url characters), else 0"))
	if err != nil {
		return func(context.Context, bool) {}
	}
	return func(ctx context.Context, invalid bool) {
		var v int64
		if invalid {
			v = 1
		}
		gauge.Record(ctx, v)
	}
}

// buildACRWorkload builds the workload-token verifier of the internal acr
// routes: the access-token issuer and keys, audience dev-health-api only (a
// token for any other audience is refused), leeway 0. No issuer or no
// AUTH_ACR_WORKLOAD_SUBJECT means no workload path (nil): the routes then
// refuse every workload token as not_configured and svc_acr_ is unchanged.
func buildACRWorkload(lookup func(string) (string, bool), keys authverify.KeySource, logger *slog.Logger) (*acr.WorkloadAuth, error) {
	if logger == nil {
		logger = slog.Default()
	}
	issuer, _ := lookup(envAccessTokenIssuer)
	issuer = strings.TrimSpace(issuer)
	subject, _ := lookup(envACRWorkloadSubject)
	subject = strings.TrimSpace(subject)
	if issuer == "" || keys == nil || subject == "" {
		return nil, nil
	}
	if !principalIDPattern.MatchString(subject) {
		return nil, fmt.Errorf("%s is not a valid principal id", envACRWorkloadSubject)
	}
	// A workload token's sub is the session wire form. Any other accepted shape (the UUID text form) starts and never
	// matches a token: an ERROR line and a gauge, never a refusal to start (D6465). The value is never logged.
	invalid := !isACRWorkloadSubjectSessionForm(subject)
	if invalid {
		logger.ErrorContext(context.Background(), "api acr workload subject is not the session wire form: no workload token will match",
			slog.String("variable", envACRWorkloadSubject))
	}
	recordACRWorkloadSubjectInvalidSafe(invalid)
	verifier, err := authverify.NewWorkloadVerifier(authverify.WorkloadVerifierConfig{Issuer: issuer, Audience: accessTokenAudienceAPI, Keys: keys})
	if err != nil {
		return nil, err
	}
	return &acr.WorkloadAuth{Verifier: verifier, Subject: subject}, nil
}

// legacyVerifier maps the HS256 path's errors (acp D3): a refusal and a claim
// of the wrong type are refusals (401); any other store failure is
// unavailable (503), never a pass and never a refusal.
func legacyVerifier(authenticate func(context.Context, string) (*policy.User, error)) accessverify.LegacyVerifier {
	return func(ctx context.Context, token string) (any, error) {
		user, err := authenticate(ctx, token)
		switch {
		case err == nil:
			return user, nil
		case policy.IsRefusal(err), policy.IsClaimTypeError(err):
			return nil, err
		default:
			return nil, fmt.Errorf("%w: %w", accessverify.ErrUnavailable, err)
		}
	}
}

func recordACRWorkloadSubjectInvalidSafe(invalid bool) {
	defer func() { _ = recover() }()
	if recordACRWorkloadSubjectInvalid != nil {
		recordACRWorkloadSubjectInvalid(context.Background(), invalid)
	}
}

// isACRWorkloadSubjectSessionForm reports whether subject is session.WirePrincipalID of one id: "prn_" and the 22
// base64url characters of a 16-byte id, canonical (no stray trailing bits).
func isACRWorkloadSubjectSessionForm(subject string) bool {
	raw, ok := strings.CutPrefix(subject, "prn_")
	if !ok || len(raw) != 22 {
		return false
	}
	b, err := base64.RawURLEncoding.DecodeString(raw)
	return err == nil && len(b) == 16 && base64.RawURLEncoding.EncodeToString(b) == raw
}

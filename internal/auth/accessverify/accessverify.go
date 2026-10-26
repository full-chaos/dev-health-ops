// Package accessverify picks the access-token validator from the route's
// endpoint-profile row and the token's JOSE header alg. It never tries
// several validators.
package accessverify

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"

	"github.com/full-chaos/dev-health-go/authverify"
)

const (
	AcceptedMetric = "devhealth_auth_access_token_accepted_total"
	RefusedMetric  = "devhealth_auth_access_token_refused_total"

	unknownLabel = "unknown"

	ClassUser   = "user_access_token"
	ClassLegacy = "ops_access_token_hs256"

	ReasonNoProfile     = "no_profile"
	ReasonAlgNotAllowed = "alg_not_allowed"
	ReasonKidUnknown    = "kid_unknown"
	ReasonSignature     = "signature"
	ReasonExpired       = "expired"
	ReasonMalformed     = "malformed"
	ReasonClaims        = "claims"
	ReasonLegacy        = "legacy_rejected"
	ReasonNotConfigured = "not_configured"
	ReasonUnavailable   = "unavailable"
)

// Reasons is the closed refusal label set.
var Reasons = []string{ReasonNoProfile, ReasonAlgNotAllowed, ReasonKidUnknown, ReasonSignature, ReasonExpired, ReasonMalformed, ReasonClaims, ReasonLegacy, ReasonNotConfigured, ReasonUnavailable}

// ErrUnavailable is returned when a validator could not decide because a
// dependency failed; callers answer 503, not 401. A LegacyVerifier wraps it.
var ErrUnavailable = errors.New("accessverify: validator unavailable")

// ErrRefused is returned for every refusal; callers answer 401 with a generic body.
var ErrRefused = errors.New("accessverify: access token refused")

// Row is the part of an endpoint-profile row the validator needs. Profile is
// the route template ("METHOD /path/{param}"), never a concrete path.
type Row struct {
	Service     string
	Profile     string
	AcceptsUser bool
	LegacyHS256 bool
}

// EdDSAVerifier is satisfied by *authverify.Verifier.
type EdDSAVerifier interface {
	Verify(token string, now time.Time) (*authverify.AccessClaims, error)
}

// LegacyVerifier validates an HS256 access token. Nil: HS256 is never accepted.
type LegacyVerifier func(ctx context.Context, token string) (any, error)

// Result is an accepted token. Exactly one of Claims and Legacy is set.
type Result struct {
	Class  string
	Claims *authverify.AccessClaims
	Legacy any
}

type key struct{ service, profile string }

// Config builds a Validator.
type Config struct {
	Rows   []Row
	EdDSA  EdDSAVerifier
	Legacy LegacyVerifier
	Meter  metric.Meter
	Logger *slog.Logger
	Now    func() time.Time
}

// Validator is safe for concurrent use.
type Validator struct {
	rows     map[key]Row
	eddsa    EdDSAVerifier
	legacy   LegacyVerifier
	accepted metric.Int64Counter
	refused  metric.Int64Counter
	logger   *slog.Logger
	now      func() time.Time
}

// New builds a Validator and pre-registers every accepted and refused series at 0.
func New(cfg Config) (*Validator, error) {
	if cfg.Meter == nil {
		return nil, errors.New("accessverify: meter is required")
	}
	accepted, err := cfg.Meter.Int64Counter(AcceptedMetric,
		metric.WithDescription("Access tokens accepted, by service, route template and credential class."))
	if err != nil {
		return nil, err
	}
	refused, err := cfg.Meter.Int64Counter(RefusedMetric,
		metric.WithDescription("Access tokens refused, by service, route template and a closed reason."))
	if err != nil {
		return nil, err
	}
	v := &Validator{rows: make(map[key]Row, len(cfg.Rows)), eddsa: cfg.EdDSA, legacy: cfg.Legacy,
		accepted: accepted, refused: refused, logger: cfg.Logger, now: cfg.Now}
	if v.logger == nil {
		v.logger = slog.New(slog.DiscardHandler)
	}
	if v.now == nil {
		v.now = time.Now
	}
	ctx := context.Background()
	refused.Add(ctx, 0, metric.WithAttributes(attribute.String("service", unknownLabel), attribute.String("profile", unknownLabel), attribute.String("reason", ReasonNoProfile)))
	for _, row := range cfg.Rows {
		if row.Service == "" || row.Profile == "" {
			return nil, errors.New("accessverify: row needs a service and a profile")
		}
		v.rows[key{row.Service, row.Profile}] = row
		if row.AcceptsUser {
			accepted.Add(ctx, 0, metric.WithAttributes(attribute.String("service", row.Service), attribute.String("profile", row.Profile), attribute.String("class", ClassUser)))
		}
		if row.LegacyHS256 {
			accepted.Add(ctx, 0, metric.WithAttributes(attribute.String("service", row.Service), attribute.String("profile", row.Profile), attribute.String("class", ClassLegacy)))
		}
		for _, reason := range Reasons {
			refused.Add(ctx, 0, metric.WithAttributes(attribute.String("service", row.Service), attribute.String("profile", row.Profile), attribute.String("reason", reason)))
		}
	}
	return v, nil
}

func (v *Validator) Validate(ctx context.Context, service, profile, bearer string) (*Result, error) {
	row, ok := v.rows[key{service, profile}]
	if !ok {
		// Unknown input never becomes a label value.
		return nil, v.refuse(ctx, unknownLabel, unknownLabel, ReasonNoProfile)
	}
	alg, ok := headerAlg(bearer)
	if !ok {
		return nil, v.refuse(ctx, row.Service, row.Profile, ReasonMalformed)
	}
	switch alg {
	case "EdDSA":
		if !row.AcceptsUser {
			return nil, v.refuse(ctx, row.Service, row.Profile, ReasonAlgNotAllowed)
		}
		if v.eddsa == nil {
			return nil, v.refuse(ctx, row.Service, row.Profile, ReasonNotConfigured)
		}
		claims, err := v.eddsa.Verify(bearer, v.now())
		if errors.Is(err, authverify.ErrKeySourceUnavailable) {
			return nil, v.unavailable(ctx, row.Service, row.Profile, fmt.Errorf("%w: %w", ErrUnavailable, authverify.ErrKeySourceUnavailable))
		}
		if err != nil {
			return nil, v.refuse(ctx, row.Service, row.Profile, eddsaReason(err))
		}
		v.accepted.Add(ctx, 1, metric.WithAttributes(attribute.String("service", row.Service), attribute.String("profile", row.Profile), attribute.String("class", ClassUser)))
		return &Result{Class: ClassUser, Claims: claims}, nil
	case "HS256":
		if !row.LegacyHS256 {
			return nil, v.refuse(ctx, row.Service, row.Profile, ReasonAlgNotAllowed)
		}
		if v.legacy == nil {
			return nil, v.refuse(ctx, row.Service, row.Profile, ReasonNotConfigured)
		}
		principal, err := v.legacy(ctx, bearer)
		if errors.Is(err, ErrUnavailable) {
			return nil, v.unavailable(ctx, row.Service, row.Profile, ErrUnavailable)
		}
		if err != nil {
			return nil, v.refuse(ctx, row.Service, row.Profile, ReasonLegacy)
		}
		v.accepted.Add(ctx, 1, metric.WithAttributes(attribute.String("service", row.Service), attribute.String("profile", row.Profile), attribute.String("class", ClassLegacy)))
		return &Result{Class: ClassLegacy, Legacy: principal}, nil
	default:
		return nil, v.refuse(ctx, row.Service, row.Profile, ReasonAlgNotAllowed)
	}
}

func (v *Validator) unavailable(ctx context.Context, service, profile string, err error) error {
	v.refused.Add(ctx, 1, metric.WithAttributes(attribute.String("service", service), attribute.String("profile", profile), attribute.String("reason", ReasonUnavailable)))
	v.logger.WarnContext(ctx, "access token validator unavailable",
		slog.String("service", service), slog.String("profile", profile))
	return err
}

func (v *Validator) refuse(ctx context.Context, service, profile, reason string) error {
	v.refused.Add(ctx, 1, metric.WithAttributes(attribute.String("service", service), attribute.String("profile", profile), attribute.String("reason", reason)))
	v.logger.DebugContext(ctx, "access token refused",
		slog.String("service", service), slog.String("profile", profile), slog.String("reason", reason))
	return ErrRefused
}

func eddsaReason(err error) string {
	switch authverify.ReasonOf(err) {
	case authverify.ReasonUnknownKid:
		return ReasonKidUnknown
	case authverify.ReasonSignature:
		return ReasonSignature
	case authverify.ReasonExpired:
		return ReasonExpired
	case authverify.ReasonContract:
		return ReasonMalformed
	default:
		return ReasonClaims
	}
}

// headerAlg reads only the alg member of the JOSE header. Only a header that
// is one JSON object with a string alg and no duplicate alg member passes.
func headerAlg(token string) (string, bool) {
	head, _, found := strings.Cut(token, ".")
	if !found || head == "" {
		return "", false
	}
	raw, err := base64.RawURLEncoding.Strict().DecodeString(head)
	if err != nil {
		return "", false
	}
	dec := json.NewDecoder(strings.NewReader(string(raw)))
	if tok, err := dec.Token(); err != nil || tok != json.Delim('{') {
		return "", false
	}
	alg, seen := "", false
	for dec.More() {
		nameTok, err := dec.Token()
		name, isString := nameTok.(string)
		if err != nil || !isString {
			return "", false
		}
		if name == "alg" {
			var value any
			if err := dec.Decode(&value); err != nil || seen {
				return "", false
			}
			s, isString := value.(string)
			if !isString {
				return "", false
			}
			alg, seen = s, true
			continue
		}
		var skip json.RawMessage
		if err := dec.Decode(&skip); err != nil {
			return "", false
		}
	}
	if tok, err := dec.Token(); err != nil || tok != json.Delim('}') || dec.More() {
		return "", false
	}
	return alg, seen
}

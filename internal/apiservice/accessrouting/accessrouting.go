// Package accessrouting is the go-api access-token routing: the endpoint-profile row table of the api
// listener and the adapter that lets policy.Authenticator decide a bearer by the route's row.
package accessrouting

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"

	"github.com/full-chaos/dev-health-go/authverify"
	"go.opentelemetry.io/otel/metric"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

// Service is the service name of the api listener's endpoint-profile rows.
const Service = "dev-health-ops-api"

// Rows is the FULL go-api row table for the access-token
// validator: every dev-health-ops-api row, keyed by the route template the
// mux registers it under.
func Rows() ([]accessverify.Row, error) {
	return ParseRows(authv1.GoAPIEndpointProfiles)
}

// ParseRows reads the access-token rows of the api service from an endpoint-profile document.
func ParseRows(raw []byte) ([]accessverify.Row, error) {
	var document struct {
		Rows []struct {
			Service         string   `json:"service"`
			Method          string   `json:"method"`
			Route           string   `json:"route"`
			AcceptedClasses []string `json:"accepted_credential_classes"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("go-api endpoint profiles: %w", err)
	}
	seen := map[string]bool{}
	var rows []accessverify.Row
	for _, entry := range document.Rows {
		if entry.Service != Service {
			continue
		}
		if entry.Method == "" || entry.Route == "" {
			return nil, errors.New("go-api endpoint profiles: a row has no method or route")
		}
		profile := entry.Method + " " + entry.Route
		if seen[profile] {
			return nil, fmt.Errorf("go-api endpoint profiles: %s has two rows", profile)
		}
		seen[profile] = true
		rows = append(rows, accessverify.Row{
			Service:     Service,
			Profile:     profile,
			AcceptsUser: slices.Contains(entry.AcceptedClasses, accessverify.ClassUser),
		})
	}
	if len(rows) == 0 {
		return nil, errors.New("go-api endpoint profiles: no row for the api service")
	}
	return rows, nil
}

// Adapter adapts the accessverify validator to policy's routed
// validator: an unavailable validator is policy.ErrUnavailable (503), any
// other error a refusal (401).
type Adapter struct{ Validator *accessverify.Validator }

var errUnexpectedAccessResult = errors.New("access verify: result of an unexpected class")

func (r Adapter) Validate(ctx context.Context, service, profile, bearer string) (*policy.RoutedResult, error) {
	result, err := r.Validator.Validate(ctx, service, profile, bearer)
	switch {
	case errors.Is(err, authverify.ErrKeySourceUnavailable):
		return nil, fmt.Errorf("%w: %w", policy.ErrKeySourceUnavailable, err)
	case errors.Is(err, accessverify.ErrUnavailable):
		return nil, fmt.Errorf("%w: %w", policy.ErrUnavailable, err)
	case err != nil:
		return nil, err
	}
	if result.Class == accessverify.ClassUser && result.Claims != nil {
		claims := result.Claims
		return &policy.RoutedResult{Claims: &policy.AccessTokenClaims{Subject: claims.Subject, SessionID: claims.SessionID, OrgID: claims.OrgID, Actor: claims.ActorSubject}}, nil
	}
	return nil, errUnexpectedAccessResult
}

// RouteUserTokens routes auth over the full row table with the user access token (EdDSA, verifier) as the
// only class it verifies: a legacy HS256 bearer is refused on every row.
func RouteUserTokens(auth *policy.Authenticator, verifier accessverify.EdDSAVerifier, meter metric.Meter, logger *slog.Logger) error {
	if auth == nil || verifier == nil || meter == nil {
		return errors.New("accessrouting: routing needs an authenticator, a verifier and a meter")
	}
	rows, err := Rows()
	if err != nil {
		return err
	}
	validator, err := accessverify.New(accessverify.Config{Rows: rows, EdDSA: verifier, Meter: meter, Logger: logger})
	if err != nil {
		return err
	}
	return auth.RouteAccess(Adapter{validator}, Service)
}

package apiservice

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"slices"

	"github.com/full-chaos/dev-health-go/authverify"

	authv1 "github.com/full-chaos/dev-health-ops/contracts/auth/v1"
	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
)

// goAPIAccessService is the service name of the api listener's
// endpoint-profile rows.
const goAPIAccessService = "dev-health-ops-api"

// goAPIAccessRows is the FULL go-api row table for the access-token
// validator: every dev-health-ops-api row, keyed by the route template the
// mux registers it under.
func goAPIAccessRows() ([]accessverify.Row, error) {
	return parseAccessRows(authv1.GoAPIEndpointProfiles)
}

func parseAccessRows(raw []byte) ([]accessverify.Row, error) {
	var document struct {
		Rows []struct {
			Service               string   `json:"service"`
			Method                string   `json:"method"`
			Route                 string   `json:"route"`
			AcceptedClasses       []string `json:"accepted_credential_classes"`
			AccessTokenValidation *struct {
				LegacyHS256 bool `json:"legacy_hs256"`
			} `json:"access_token_validation"`
		} `json:"rows"`
	}
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("go-api endpoint profiles: %w", err)
	}
	seen := map[string]bool{}
	var rows []accessverify.Row
	for _, entry := range document.Rows {
		if entry.Service != goAPIAccessService {
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
			Service:     goAPIAccessService,
			Profile:     profile,
			AcceptsUser: slices.Contains(entry.AcceptedClasses, accessverify.ClassUser),
			LegacyHS256: entry.AccessTokenValidation != nil && entry.AccessTokenValidation.LegacyHS256,
		})
	}
	if len(rows) == 0 {
		return nil, errors.New("go-api endpoint profiles: no row for the api service")
	}
	return rows, nil
}

// routedAccessVerify adapts the accessverify validator to policy's routed
// validator: an unavailable validator is policy.ErrUnavailable (503), any
// other error a refusal (401).
type routedAccessVerify struct{ validator *accessverify.Validator }

var errUnexpectedAccessResult = errors.New("access verify: result of an unexpected class")

func (r routedAccessVerify) Validate(ctx context.Context, service, profile, bearer string) (*policy.RoutedResult, error) {
	result, err := r.validator.Validate(ctx, service, profile, bearer)
	switch {
	case errors.Is(err, authverify.ErrKeySourceUnavailable):
		return nil, fmt.Errorf("%w: %w", policy.ErrKeySourceUnavailable, err)
	case errors.Is(err, accessverify.ErrUnavailable):
		return nil, fmt.Errorf("%w: %w", policy.ErrUnavailable, err)
	case err != nil:
		return nil, err
	}
	switch {
	case result.Class == accessverify.ClassLegacy:
		if user, ok := result.Legacy.(*policy.User); ok && user != nil {
			return &policy.RoutedResult{Legacy: user}, nil
		}
	case result.Class == accessverify.ClassUser && result.Claims != nil:
		claims := result.Claims
		return &policy.RoutedResult{Claims: &policy.AccessTokenClaims{Subject: claims.Subject, SessionID: claims.SessionID, OrgID: claims.OrgID}}, nil
	}
	return nil, errUnexpectedAccessResult
}

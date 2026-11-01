package authapi

import (
	"errors"
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/api/policy"
	"github.com/full-chaos/dev-health-ops/internal/auth/httpapi"
)

// PatternCompatValidate is go-api's token check, POST /api/v1/auth/validate
// (internal/api/session validate).
const PatternCompatValidate = "/api/v1/auth/validate"

// ValidateConfig mounts POST /api/v1/auth/validate (CompatDeps.Validate).
type ValidateConfig struct {
	// Sessions is go-api's own session read (policy.PGStore over the auth
	// schema): the row go-api's sid check reads.
	Sessions policy.SessionStore
	// IssuerURL and Keys add the EdDSA user access token of audience
	// dev-health-api. Without both the route judges the HS256 token only.
	IssuerURL string
	Keys      KeySetSource
}

// validateLimit and validateKey are declared for the cells; the change
// builds them.
var validateLimit httpapi.Limit

var errValidateNotBuilt = errors.New("authapi: the validate route is not built")

func validateKey(*http.Request, string) (string, error) { return "", errValidateNotBuilt }

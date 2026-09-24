package licensing

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// GraceDays is licensing.types.GRACE_DAYS; ok is false for a tier outside
// LicenseTier.
func GraceDays(tier string) (int64, bool) {
	switch tier {
	case "community":
		return 0, true
	case "team":
		return 14, true
	case "enterprise":
		return 30, true
	}
	return 0, false
}

// backfillDays is LicenseLimits.backfill_days of DEFAULT_LIMITS[tier]: nil
// is None (unlimited).
func backfillDays(tier string) pyjson.Value {
	switch tier {
	case "community":
		return int64(30)
	case "team":
		return int64(90)
	}
	return nil
}

// LicenseRequest is sign_license's arguments as the billing webhook passes
// them: org, tier, the issue time and the license id (the defaults Python
// fills in from the clock and uuid4, passed explicitly here so a caller
// and a test control them). Features, limits and grace days are the
// tier's defaults; the duration is 365 days; org_name and contact_email
// are None.
type LicenseRequest struct {
	OrgID     string
	Tier      string
	IssuedAt  int64
	LicenseID string
}

// licenseDurationDays is sign_license's duration_days default.
const licenseDurationDays = 365

// SignLicense is licensing.generator.sign_license with its defaults:
// base64(payload JSON).base64(Ed25519 signature of that JSON), the payload
// being LicensePayload.model_dump_json() -- pydantic-core's compact JSON in
// field order. Ed25519 signatures are deterministic, so the same key and
// payload give the same license on both planes.
func SignLicense(privateKeyB64 string, request LicenseRequest) (string, error) {
	tier := strings.ToLower(request.Tier)
	if _, ok := TierRank(tier); !ok {
		return "", fmt.Errorf("Invalid tier %q. Must be one of: community, team, enterprise", request.Tier)
	}
	seed, err := pythonB64Decode(privateKeyB64)
	if err != nil {
		return "", fmt.Errorf("Invalid private key: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return "", fmt.Errorf("Invalid private key: the seed must be exactly %d bytes long", ed25519.SeedSize)
	}
	limits, _ := DefaultLimits(tier)
	grace, _ := GraceDays(tier)
	features := pyjson.NewObject()
	for _, feature := range FeaturesForTier(tier) {
		features.Set(feature.Key, feature.Enabled)
	}
	limitObject := pyjson.NewObject()
	limitObject.Set("users", limits.Users)
	limitObject.Set("repos", limits.Repos)
	limitObject.Set("api_rate", limits.APIRate)
	limitObject.Set("backfill_days", backfillDays(tier))
	payload := pyjson.NewObject()
	payload.Set("iss", "fullchaos.studio")
	payload.Set("sub", request.OrgID)
	payload.Set("iat", request.IssuedAt)
	payload.Set("exp", request.IssuedAt+licenseDurationDays*86400)
	payload.Set("tier", tier)
	payload.Set("features", features)
	payload.Set("limits", limitObject)
	payload.Set("grace_days", grace)
	payload.Set("org_name", nil)
	payload.Set("contact_email", nil)
	payload.Set("license_id", request.LicenseID)
	body, err := pyjson.MarshalModel(payload)
	if err != nil {
		return "", err
	}
	signature := ed25519.Sign(ed25519.NewKeyFromSeed(seed), body)
	return base64.StdEncoding.EncodeToString(body) + "." + base64.StdEncoding.EncodeToString(signature), nil
}

// pythonB64Decode is base64.b64decode(text) with its default
// validate=False: characters outside the base64 alphabet are discarded,
// the rest must form whole, correctly padded quanta. Named limit: the
// less common shapes binascii's lenient mode also accepts (data after the
// padding) are refused here.
func pythonB64Decode(text string) ([]byte, error) {
	var kept strings.Builder
	for _, r := range text {
		if (r >= 'A' && r <= 'Z') || (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') || r == '+' || r == '/' || r == '=' {
			kept.WriteRune(r)
		}
	}
	decoded, err := base64.StdEncoding.DecodeString(kept.String())
	if err != nil {
		return nil, errors.New("Incorrect padding")
	}
	return decoded, nil
}

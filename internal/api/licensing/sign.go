package licensing

import (
	"crypto/ed25519"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
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
	// DurationDays is sign_license's duration_days; nil is its default of 365.
	// A value that is not positive is refused, as Python refuses it. Python's
	// integers are unbounded, so is this one (the expiry becomes a big integer).
	DurationDays *big.Int
	// OrgName and ContactEmail are the payload's org_name and contact_email;
	// nil is None.
	OrgName, ContactEmail *string
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
		return "", fmt.Errorf("Invalid tier %s. Must be one of: community, team, enterprise", pythonparity.StrRepr(request.Tier))
	}
	duration := big.NewInt(licenseDurationDays)
	if request.DurationDays != nil {
		duration = request.DurationDays
	}
	if duration.Sign() <= 0 {
		return "", errors.New("duration_days must be positive")
	}
	expiry := new(big.Int).Mul(duration, big.NewInt(86400))
	expiry.Add(expiry, big.NewInt(request.IssuedAt))
	seed, err := pythonB64Decode(privateKeyB64)
	if err != nil {
		return "", fmt.Errorf("Invalid private key: %w", err)
	}
	if len(seed) != ed25519.SeedSize {
		return "", fmt.Errorf("Invalid private key: The seed must be exactly %d bytes long", ed25519.SeedSize)
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
	payload.Set("exp", pyjson.Int{Int: expiry})
	payload.Set("tier", tier)
	payload.Set("features", features)
	payload.Set("limits", limitObject)
	payload.Set("grace_days", grace)
	payload.Set("org_name", optionalString(request.OrgName))
	payload.Set("contact_email", optionalString(request.ContactEmail))
	payload.Set("license_id", request.LicenseID)
	body, err := pyjson.MarshalModel(payload)
	if err != nil {
		return "", err
	}
	signature := ed25519.Sign(ed25519.NewKeyFromSeed(seed), body)
	return base64.StdEncoding.EncodeToString(body) + "." + base64.StdEncoding.EncodeToString(signature), nil
}

func optionalString(value *string) pyjson.Value {
	if value == nil {
		return nil
	}
	return *value
}

// pythonB64Decode is base64.b64decode(text) with its defaults (validate=False),
// a port of binascii.a2b_base64's non-strict loop: text that is not ASCII is a
// ValueError; characters outside the alphabet (and \r, \n, spaces) are skipped;
// an "=" is ignored until enough of them close a partly filled quantum (then
// the rest of the text is ignored); what is left over is either one stray
// character (named with the count of data characters) or "Incorrect padding".
func pythonB64Decode(text string) ([]byte, error) {
	for _, r := range text {
		if r > 0x7f {
			return nil, errors.New("string argument should contain only ASCII characters")
		}
	}
	const alphabet = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/"
	var out []byte
	quadPos, pads := 0, 0
	var left byte
	for index := 0; index < len(text); index++ {
		ch := text[index]
		if ch == '=' {
			if quadPos >= 2 {
				pads++
			}
			continue
		}
		value := strings.IndexByte(alphabet, ch)
		if value < 0 {
			continue
		}
		pads = 0
		switch quadPos {
		case 0:
			quadPos, left = 1, byte(value)
		case 1:
			quadPos = 2
			out = append(out, left<<2|byte(value)>>4)
			left = byte(value) & 0x0f
		case 2:
			quadPos = 3
			out = append(out, left<<4|byte(value)>>2)
			left = byte(value) & 0x03
		case 3:
			quadPos = 0
			out = append(out, left<<6|byte(value))
			left = 0
		}
	}
	if quadPos == 1 {
		return nil, fmt.Errorf("Invalid base64-encoded string: number of data characters (%d) cannot be 1 more than a multiple of 4", len(out)/3*4+1)
	}
	if quadPos != 0 && quadPos+pads < 4 {
		return nil, errors.New("Incorrect padding")
	}
	return out, nil
}

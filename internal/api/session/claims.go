package session

import (
	"errors"
	"math"
	"strconv"

	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// errClaimType is a claim value of a JSON type the platform never mints
// where Python would raise on it (str() of a container, uuid.UUID() of a
// number): the bare 500.
var errClaimType = errors.New("session: token claim has a JSON type the api does not mint")

// truthy is bool(value) for a claim value decoded by encoding/json.
func truthy(value any) bool {
	switch typed := value.(type) {
	case nil:
		return false
	case bool:
		return typed
	case string:
		return typed != ""
	case float64:
		return typed != 0
	case []any:
		return len(typed) > 0
	case map[string]any:
		return len(typed) > 0
	}
	return true
}

// pyStrClaim is str(value) for a scalar claim value. A JSON number is read
// as encoding/json reads it (float64), so an integral value is written as
// Python writes the int PyJWT would have decoded.
func pyStrClaim(value any) (string, error) {
	switch typed := value.(type) {
	case string:
		return typed, nil
	case nil:
		return "None", nil
	case bool:
		if typed {
			return "True", nil
		}
		return "False", nil
	case float64:
		if typed == math.Trunc(typed) && math.Abs(typed) < 1<<53 {
			return strconv.FormatInt(int64(typed), 10), nil
		}
		return pythonparity.Repr(typed), nil
	}
	return "", errClaimType
}

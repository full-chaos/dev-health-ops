package admin

import (
	"encoding/json"
	"errors"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// storedPagerDutyTokenJSON is OAuthTokens.model_dump_json() for a renewed
// token, in the form both planes read.
func storedPagerDutyTokenJSON(access, refresh string, expiresAt time.Time, scopes []string) ([]byte, error) {
	var refreshPtr *string
	if refresh != "" {
		refreshPtr = &refresh
	}
	return json.Marshal(storedPagerDutyTokens{AccessToken: access, RefreshToken: refreshPtr, ExpiresAt: expiresAt, GrantedScopes: scopes})
}

var errServiceModel = errors.New("pagerduty service does not validate")

// validatePagerDutyService is models.Service.model_validate for one page item:
// PagerDutyModel's fields (id required, the text and datetime fields typed,
// unknown keys ignored) plus name and status. Any failure is a pydantic
// ValidationError, which the route does not catch.
func validatePagerDutyService(value pyjson.Value) (pagerDutyService, error) {
	object, ok := value.(*pyjson.Object)
	if !ok {
		return pagerDutyService{}, errServiceModel
	}
	id, err := validatePagerDutyModelFields(object)
	if err != nil {
		return pagerDutyService{}, err
	}
	name, err := optionalServiceString(object, "name")
	if err != nil {
		return pagerDutyService{}, err
	}
	status, err := optionalServiceString(object, "status")
	if err != nil {
		return pagerDutyService{}, err
	}
	if policy, present := object.Get("escalation_policy"); present && policy != nil {
		nested, isObject := policy.(*pyjson.Object)
		if !isObject {
			return pagerDutyService{}, errServiceModel
		}
		if _, err := validatePagerDutyModelFields(nested); err != nil {
			return pagerDutyService{}, err
		}
	}
	out := pagerDutyService{id: id, status: status}
	if name != nil && pythonparity.Strip(*name) != "" {
		out.displayName, out.nameResolved = pythonparity.Strip(*name), true
	} else {
		out.displayName = "PagerDuty service " + id
	}
	return out, nil
}

func optionalServiceString(object *pyjson.Object, key string) (*string, error) {
	raw, present := object.Get(key)
	if !present || raw == nil {
		return nil, nil
	}
	text, isString := raw.(string)
	if !isString {
		return nil, errServiceModel
	}
	return &text, nil
}

// validatePagerDutyModelFields checks PagerDutyModel's own fields (both the
// alias "self" and the field name "self_url" are accepted).
func validatePagerDutyModelFields(object *pyjson.Object) (string, error) {
	raw, present := object.Get("id")
	id, isString := raw.(string)
	if !present || !isString {
		return "", errServiceModel
	}
	for _, key := range []string{"type", "summary", "self", "self_url", "html_url"} {
		if _, err := optionalServiceString(object, key); err != nil {
			return "", err
		}
	}
	for _, key := range []string{"created_at", "updated_at"} {
		raw, present := object.Get(key)
		if !present || raw == nil {
			continue
		}
		if _, failure := pybody.PydanticDatetime(raw); failure != nil {
			return "", errServiceModel
		}
	}
	return id, nil
}

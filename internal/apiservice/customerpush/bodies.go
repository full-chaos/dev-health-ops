package customerpush

import (
	"math/big"
	"sort"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/pybody"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/pythonparity"
)

// webhookModes is IngestWebhookMode's values, sorted as the schema's
// messages print them.
var webhookModes = []string{"customer_relay", "disabled", "fullchaos_hosted"}

// tokenScopes is IngestTokenScope's values, sorted.
var tokenScopes = []string{"ingest:status", "ingest:write", "schema:read"}

// sourceCreate is a valid IngestSourceCreate.
type sourceCreate struct {
	System, Instance, EntityFamily string
	DisplayName                    *string
	Mode, WebhookMode              string
}

// sourcePatch is a valid IngestSourcePatch; nil is an unset field.
type sourcePatch struct {
	DisplayName, Mode, WebhookMode *string
	Enabled                        *bool
}

// tokenCreate is a valid IngestTokenCreate.
type tokenCreate struct {
	Name      string
	Scopes    []string
	ExpiresAt *pytime.DateTime
}

// pyListRepr is repr() of a list of str.
func pyListRepr(values []string) string {
	quoted := make([]string, len(values))
	for index, value := range values {
		quoted[index] = pythonparity.StrRepr(value)
	}
	return "[" + strings.Join(quoted, ", ") + "]"
}

// valueError is the FastAPI rendering of a ValueError raised in a
// field_validator: ctx.error is the exception, which jsonable_encoder
// renders as {}.
func valueError(loc []pyjson.Value, input pyjson.Value, message string) pybody.Error {
	ctx := pyjson.NewObject()
	ctx.Set("error", pyjson.NewObject())
	return pybody.Error{Type: "value_error", Loc: loc, Msg: "Value error, " + message, Input: input, Ctx: ctx}
}

func bodyLoc(name string) []pyjson.Value { return []pyjson.Value{"body", name} }

// checkWebhookMode is _validate_webhook_mode for a present str value.
func checkWebhookMode(errs *pybody.Errors, value string) bool {
	for _, mode := range webhookModes {
		if value == mode {
			return true
		}
	}
	*errs = append(*errs, valueError(bodyLoc("webhook_mode"), value, "webhook_mode must be one of "+pyListRepr(webhookModes)))
	return false
}

// parseSourceCreate validates body as IngestSourceCreate, in field order.
func parseSourceCreate(body pybody.Body) (sourceCreate, []pybody.Error) {
	var errs pybody.Errors
	object, ok := errs.Object(body)
	if !ok {
		return sourceCreate{}, errs
	}
	out := sourceCreate{EntityFamily: "legacy", Mode: "customer_push", WebhookMode: "disabled"}
	if system, ok := errs.RequiredString(object, "system", 1, 0); ok {
		out.System = system
	}
	if instance, ok := errs.RequiredString(object, "instance", 1, 0); ok {
		if normalized := pythonparity.Strip(instance); normalized == "" {
			errs = append(errs, valueError(bodyLoc("instance"), instance, "instance must not be blank"))
		} else {
			out.Instance = normalized
		}
	}
	if raw, present := object.Get("entity_family"); present {
		if text, isString := raw.(string); isString && (text == "legacy" || text == "operational") {
			out.EntityFamily = text
		} else {
			ctx := pyjson.NewObject()
			ctx.Set("expected", "'legacy' or 'operational'")
			errs = append(errs, pybody.Error{Type: "literal_error", Loc: bodyLoc("entity_family"),
				Msg: "Input should be 'legacy' or 'operational'", Input: raw, Ctx: ctx})
		}
	}
	if displayName, ok := errs.OptionalString(object, "display_name", 0, 0); ok {
		out.DisplayName = &displayName
	}
	if mode, ok := errs.DefaultedString(object, "mode", 0, 0); ok {
		out.Mode = mode
	}
	if webhookMode, ok := errs.DefaultedString(object, "webhook_mode", 0, 0); ok && checkWebhookMode(&errs, webhookMode) {
		out.WebhookMode = webhookMode
	}
	return out, errs
}

// parseSourcePatch validates body as IngestSourcePatch.
func parseSourcePatch(body pybody.Body) (sourcePatch, []pybody.Error) {
	var errs pybody.Errors
	object, ok := errs.Object(body)
	if !ok {
		return sourcePatch{}, errs
	}
	var out sourcePatch
	if value, ok := errs.OptionalString(object, "display_name", 0, 0); ok {
		out.DisplayName = &value
	}
	if value, ok := errs.OptionalString(object, "mode", 0, 0); ok {
		out.Mode = &value
	}
	if value, ok := errs.OptionalBool(object, "enabled"); ok {
		out.Enabled = &value
	}
	if value, ok := errs.OptionalString(object, "webhook_mode", 0, 0); ok && checkWebhookMode(&errs, value) {
		out.WebhookMode = &value
	}
	return out, errs
}

// parseTokenCreate validates body as IngestTokenCreate.
func parseTokenCreate(body pybody.Body) (tokenCreate, []pybody.Error) {
	var errs pybody.Errors
	object, ok := errs.Object(body)
	if !ok {
		return tokenCreate{}, errs
	}
	var out tokenCreate
	if name, ok := errs.RequiredString(object, "name", 1, 0); ok {
		out.Name = name
	}
	if scopes, ok := parseScopes(&errs, object); ok {
		out.Scopes = scopes
	}
	if raw, present := object.Get("expires_at"); present && raw != nil {
		var input any = raw
		switch typed := raw.(type) {
		case pyjson.Int:
			input = new(big.Int).Set(typed.Int)
		case pyjson.Float:
			input = float64(typed)
		}
		parsed, failure := pytime.ParseDatetime(input)
		if failure != nil {
			errs = append(errs, pybody.DatetimeError(bodyLoc("expires_at"), raw, failure))
		} else {
			out.ExpiresAt = &parsed
		}
	}
	return out, errs
}

// parseScopes is `scopes: list[str] = Field(..., min_length=1)` and its
// _check_scopes validator: item errors first (no length check then), then
// the length, then the unknown-scope check.
func parseScopes(errs *pybody.Errors, object *pyjson.Object) ([]string, bool) {
	loc := bodyLoc("scopes")
	raw, present := object.Get("scopes")
	if !present {
		*errs = append(*errs, pybody.Error{Type: "missing", Loc: loc, Msg: "Field required", Input: object})
		return nil, false
	}
	list, isList := raw.([]pyjson.Value)
	if !isList {
		*errs = append(*errs, pybody.Error{Type: "list_type", Loc: loc, Msg: "Input should be a valid list", Input: raw})
		return nil, false
	}
	scopes := make([]string, 0, len(list))
	valid := true
	for index, item := range list {
		text, isString := item.(string)
		if !isString {
			*errs = append(*errs, pybody.Error{Type: "string_type", Loc: []pyjson.Value{"body", "scopes", int64(index)},
				Msg: "Input should be a valid string", Input: item})
			valid = false
			continue
		}
		scopes = append(scopes, text)
	}
	if !valid {
		return nil, false
	}
	if len(scopes) < 1 {
		ctx := pyjson.NewObject()
		ctx.Set("field_type", "List")
		ctx.Set("min_length", int64(1))
		ctx.Set("actual_length", int64(len(scopes)))
		*errs = append(*errs, pybody.Error{Type: "too_short", Loc: loc,
			Msg: "List should have at least 1 item after validation, not 0", Input: raw, Ctx: ctx})
		return nil, false
	}
	unknown := map[string]bool{}
	for _, scope := range scopes {
		known := false
		for _, valid := range tokenScopes {
			if scope == valid {
				known = true
				break
			}
		}
		if !known {
			unknown[scope] = true
		}
	}
	if len(unknown) > 0 {
		sorted := make([]string, 0, len(unknown))
		for scope := range unknown {
			sorted = append(sorted, scope)
		}
		sort.Strings(sorted)
		*errs = append(*errs, valueError(loc, raw,
			"Unknown scope(s) "+pyListRepr(sorted)+"; valid scopes are "+pyListRepr(tokenScopes)))
		return nil, false
	}
	return scopes, true
}

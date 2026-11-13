package authapi

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
	"github.com/full-chaos/dev-health-ops/internal/api/pytime"
	"github.com/full-chaos/dev-health-ops/internal/auth/ssoprovider"
)

// The SSO provider answers are byte-equal to what go-api's providerResponse renders: the times in pydantic's form, the config as
// stored (key order kept) and a created config stored in the legacy model's field order. go-api passes the config on as it is.

func pydanticTime(t time.Time) string { return pytime.Pydantic(pytime.UTC(t)) }

func ssoAnswer(p ssoprovider.Provider) ssoProviderAnswer {
	return ssoProviderAnswer{ID: p.ID, OrgID: p.OrgID, Name: p.Name, Protocol: p.Protocol, Status: p.Status, IsDefault: p.IsDefault,
		AllowIdpInitiated: p.AllowIdpInitiated, AutoProvisionUsers: p.AutoProvision, DefaultRole: p.DefaultRole, Config: p.Config,
		AllowedDomains: p.AllowedDomains, LastMetadataSyncAt: ssoTime(p.LastMetadataSyncAt), LastLoginAt: ssoTime(p.LastLoginAt),
		LastError: p.LastError, LastErrorAt: ssoTime(p.LastErrorAt), CreatedAt: pydanticTime(p.CreatedAt),
		UpdatedAt: pydanticTime(p.UpdatedAt)}
}

// configMember is one member of a legacy config model: its name in field order, and what an absent or null member stores.
type configMember struct {
	name     string
	fallback func() pyjson.Value
}

func nullMember(name string) configMember {
	return configMember{name, func() pyjson.Value { return nil }}
}

func textMember(name, text string) configMember {
	return configMember{name, func() pyjson.Value { return text }}
}

func emptyObjectMember(name string) configMember {
	return configMember{name, func() pyjson.Value { return pyjson.NewObject() }}
}

func listMember(name string, items ...string) configMember {
	return configMember{name, func() pyjson.Value {
		out := make([]pyjson.Value, len(items))
		for i, item := range items {
			out[i] = item
		}
		return out
	}}
}

var samlConfigMembers = []configMember{nullMember("entity_id"), nullMember("sso_url"), nullMember("certificate"), nullMember("slo_url"),
	textMember("name_id_format", "urn:oasis:names:tc:SAML:1.1:nameid-format:emailAddress"), emptyObjectMember("attribute_mapping")}

var oidcConfigMembers = []configMember{nullMember("client_id"), nullMember("issuer"), nullMember("authorization_endpoint"),
	nullMember("token_endpoint"), nullMember("userinfo_endpoint"), nullMember("jwks_uri"), listMember("scopes", "openid", "profile", "email"),
	emptyObjectMember("claim_mapping")}

// orderedConfig is the stored config: the model dump with the members in the model's field order, a sent member as sent (a
// nested object in the order it was sent), an absent or null one as its default. raw has passed the member checks already.
func orderedConfig(raw []byte, members []configMember) (*pyjson.Object, error) {
	decoded, err := pyjson.DecodeString(string(raw))
	if err != nil {
		return nil, errProvisioningBody
	}
	sent, ok := decoded.(*pyjson.Object)
	if !ok {
		return nil, errProvisioningBody
	}
	out := pyjson.NewObject()
	for _, member := range members {
		value, present := sent.Get(member.name)
		if !present || value == nil {
			value = member.fallback()
		}
		out.Set(member.name, value)
	}
	return out, nil
}

// writeProviderJSON is writeJSON without Go's HTML escaping: the config member is already written in go-api's response form, and
// go-api passes it on as it is, so a "<" or "&" in it must reach go-api as itself.
func writeProviderJSON(w http.ResponseWriter, status int, body any) {
	var buffer bytes.Buffer
	encoder := json.NewEncoder(&buffer)
	encoder.SetEscapeHTML(false)
	if err := encoder.Encode(body); err != nil {
		slog.Default().Error("auth sso provider route: body could not be serialized", slog.Int("status", status))
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	raw := bytes.TrimSuffix(buffer.Bytes(), []byte("\n"))
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(raw)))
	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Pragma", "no-cache")
	w.WriteHeader(status)
	_, _ = io.Copy(w, bytes.NewReader(raw))
}

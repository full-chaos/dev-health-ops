package apiservice

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"

	"github.com/full-chaos/dev-health-go/authverify"

	"github.com/full-chaos/dev-health-ops/internal/auth/accessverify"
	"github.com/full-chaos/dev-health-ops/internal/auth/contracts"
	"github.com/full-chaos/dev-health-ops/internal/auth/sessionid"
)

const (
	classHS256   = "ops_access_token_hs256"
	classUserTok = "user_access_token"
	classRefresh = "ops_refresh_token"
)

var (
	sampleRows = []string{
		"GET /api/v1/auth/me",
		"POST /api/v1/telemetry/opt-in",
		"POST /api/v1/auth/logout",
	}
	wantSelector = map[string]any{"kind": "jwt_header_alg", "name": "EdDSA"}
)

func rowClasses(row map[string]any) []string {
	var out []string
	for _, class := range row["accepted_credential_classes"].([]any) {
		out = append(out, class.(string))
	}
	return out
}

func rowProfile(row map[string]any) string {
	return row["method"].(string) + " " + row["route"].(string)
}

func apiRows(t *testing.T) []map[string]any {
	t.Helper()
	var out []map[string]any
	for _, row := range loadGoAPIRows(t) {
		if row["service"] == goAPIAccessService {
			out = append(out, row)
		}
	}
	return out
}

// Every go-api row that opts in to the HS256 access token also accepts the
// platform EdDSA access token, selected by the JWT header alg.
func TestEveryHS256RowAlsoAcceptsTheUserAccessToken(t *testing.T) {
	hs, plain := 0, 0
	for _, row := range apiRows(t) {
		classes := rowClasses(row)
		validation, _ := row["access_token_validation"].(map[string]any)
		if validation == nil || validation["legacy_hs256"] != true {
			plain++
			if slices.Contains(classes, classUserTok) {
				t.Errorf("%s: a row without the access-token opt-in accepts %s", row["id"], classUserTok)
			}
			continue
		}
		hs++
		if !slices.Contains(classes, classHS256) || !slices.Contains(classes, classUserTok) {
			t.Errorf("%s: classes %v, want both %s and %s", row["id"], classes, classHS256, classUserTok)
		}
		if selector, _ := row["class_disambiguator"].(map[string]any); selector["kind"] != wantSelector["kind"] || selector["name"] != wantSelector["name"] {
			t.Errorf("%s: class_disambiguator %v, want %v", row["id"], row["class_disambiguator"], wantSelector)
		}
	}
	// CHAOS-11813 retired the 6 legacy ingest rows, which had no HS256 opt-in (48 before).
	if hs != 196 || plain != 42 {
		t.Errorf("rows with the HS256 opt-in %d, without %d; want 196 and 42", hs, plain)
	}
}

// The widened rows pass the profile selector gate. The logout row also
// accepts ops_refresh_token, which has no request-head marker, so the gate
// cannot pick it: a limit that predates this widening.
func TestWidenedRowsPassTheClassSelectorGate(t *testing.T) {
	registry, err := contracts.LoadRegistryClasses("../..")
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, row := range apiRows(t) {
		classes := rowClasses(row)
		if !slices.Contains(classes, classUserTok) || !slices.Contains(classes, classHS256) {
			continue
		}
		if slices.Contains(classes, classRefresh) {
			if rowProfile(row) != "POST /api/v1/auth/logout" {
				t.Errorf("%s: unexpected refresh row", row["id"])
			}
			continue
		}
		if err := contracts.CheckClassSelector(row, registry); err != nil {
			t.Errorf("%s: %v", row["id"], err)
		}
		checked++
	}
	if checked != 195 {
		t.Errorf("checked %d rows, want 195", checked)
	}
}

func realValidator(t *testing.T) (*accessverify.Validator, *eddsaMinter) {
	t.Helper()
	minter := newEdDSAMinter(t)
	verifier, err := authverify.NewVerifier(authverify.VerifierConfig{Issuer: probeIssuer, Audience: accessTokenAudienceAPI, Keys: minter.keys})
	if err != nil {
		t.Fatal(err)
	}
	validator, err := accessverify.New(accessverify.Config{
		Rows:   realRows(t),
		EdDSA:  verifier,
		Legacy: func(context.Context, string) (any, error) { return nil, errors.New("hs256 not used here") },
		Meter:  sdkmetric.NewMeterProvider().Meter("test"),
		Logger: quietLogger(),
		Now:    func() time.Time { return probeNow },
	})
	if err != nil {
		t.Fatal(err)
	}
	return validator, minter
}

// mintTyp signs a valid access-token.v1 payload under the given JWT typ.
func (m *eddsaMinter) mintTyp(typ string) string {
	claims := map[string]any{
		"iss": probeIssuer, "aud": accessTokenAudienceAPI, "sub": wirePrincipal(uuid.New()), "principal_type": "human",
		"sid": sessionid.Wire(probeLiveSID), "jti": strings.Repeat("j", 24),
		"iat": probeNow.Unix() - 60, "nbf": probeNow.Unix() - 60, "exp": probeNow.Unix() + 600,
		"org_id": probeOrg.String(), "scope": "api:access",
	}
	header, _ := json.Marshal(map[string]any{"alg": "EdDSA", "typ": typ, "kid": "probe-kid"})
	payload, _ := json.Marshal(claims)
	signing := base64.RawURLEncoding.EncodeToString(header) + "." + base64.RawURLEncoding.EncodeToString(payload)
	return signing + "." + base64.RawURLEncoding.EncodeToString(ed25519.Sign(m.priv, []byte(signing)))
}

// With the real row table, a valid user access token is accepted on a plain
// row, an X-Org-Id row and the logout row.
func TestRealRowTableAcceptsAValidUserAccessToken(t *testing.T) {
	validator, minter := realValidator(t)
	token := minter.mint(uuid.New(), nil)
	for _, profile := range sampleRows {
		result, err := validator.Validate(context.Background(), goAPIAccessService, profile, token)
		if err != nil {
			t.Errorf("%s: refused: %v", profile, err)
			continue
		}
		if result.Class != accessverify.ClassUser {
			t.Errorf("%s: class %s", profile, result.Class)
		}
	}
}

// Every row outside the widened 196 (the 48 api rows without the opt-in and
// the 3 billing-edge rows) accepts no access-token class and keeps refusing a
// user access token: an api row by its row, a billing-edge row because the
// access-token table holds api rows only.
func TestRowsWithoutTheAccessTokenOptInStillRefuseAUserAccessToken(t *testing.T) {
	validator, minter := realValidator(t)
	token := minter.mint(uuid.New(), nil)
	unchanged := 0
	for _, row := range loadGoAPIRows(t) {
		if validation, _ := row["access_token_validation"].(map[string]any); validation != nil {
			continue
		}
		if slices.Contains(rowClasses(row), classUserTok) {
			t.Errorf("%s: accepts %s without access_token_validation", row["id"], classUserTok)
		}
		service, _ := row["service"].(string)
		if _, err := validator.Validate(context.Background(), service, rowProfile(row), token); !errors.Is(err, accessverify.ErrRefused) {
			t.Errorf("%s: err %v, want a refusal", row["id"], err)
		}
		unchanged++
	}
	if unchanged != 45 {
		t.Errorf("checked %d rows, want 45 (42 api + 3 billing-edge; CHAOS-11813 retired 6 legacy ingest rows)", unchanged)
	}
}

// A workload token (EdDSA, typ workload+jwt) is not a user access token on
// any widened row.
func TestAWorkloadTokenIsRefusedOnWidenedRows(t *testing.T) {
	validator, minter := realValidator(t)
	token := minter.mintTyp("workload+jwt")
	for _, profile := range sampleRows {
		if _, err := validator.Validate(context.Background(), goAPIAccessService, profile, token); !errors.Is(err, accessverify.ErrRefused) {
			t.Errorf("%s: err %v, want a refusal", profile, err)
		}
	}
}

// Every widened row carries its exact class list: the HS256 and user access
// tokens, plus org_context_header on the four X-Org-Id telemetry rows and
// ops_refresh_token on logout.
func TestWidenedRowsCarryTheirExactClassList(t *testing.T) {
	extra := map[string]string{
		"GET /api/v1/telemetry/status":   "org_context_header",
		"POST /api/v1/telemetry/opt-in":  "org_context_header",
		"POST /api/v1/telemetry/opt-out": "org_context_header",
		"POST /api/v1/telemetry/report":  "org_context_header",
		"POST /api/v1/auth/logout":       classRefresh,
	}
	widened, withExtra := 0, 0
	for _, row := range apiRows(t) {
		if validation, _ := row["access_token_validation"].(map[string]any); validation == nil {
			continue
		}
		want := []string{classHS256, classUserTok}
		if more, ok := extra[rowProfile(row)]; ok {
			want = append(want, more)
			withExtra++
		}
		slices.Sort(want)
		if got := rowClasses(row); !slices.Equal(got, want) {
			t.Errorf("%s: classes %v, want %v", row["id"], got, want)
		}
		widened++
	}
	if widened != 196 || withExtra != len(extra) {
		t.Errorf("checked %d widened rows (%d with an extra class), want 196 and %d", widened, withExtra, len(extra))
	}
}

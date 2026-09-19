package goapiproof

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
)

// ReferencePrincipalPath is the Python app's own "who am I" route: it
// answers with the org its authentication resolved the credential to.
const ReferencePrincipalPath = "/api/v1/auth/me"

// ErrReferencePrincipal is returned when the Python app does not confirm,
// before any case is sent, that the baseline credential is scoped to the
// named org with no impersonation session in force.
var ErrReferencePrincipal = errors.New("goapiproof: baseline_principal_is_impersonating_or_names_another_org")

// VerifyReferencePrincipal asks the Python app, with the credential the
// baseline legs will carry, which org it resolves that credential to. It
// refuses the run unless the answer is a 200 from the Python app (`server:
// uvicorn`, no query-api build header) with no impersonation stamp and an
// `org_id` equal to org. It is Python's own answer naming the org, read
// once before the first case; every leg after it is still checked for the
// impersonation stamp, because a session can start during the run.
func VerifyReferencePrincipal(ctx context.Context, client *LegClient, baseURL string, credential *Credential, org string) error {
	target := strings.TrimRight(baseURL, "/") + ReferencePrincipalPath
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
	if err != nil {
		return fmt.Errorf("%w: build request: %w", ErrReferencePrincipal, err)
	}
	if err := credential.Apply(ctx, request); err != nil {
		return fmt.Errorf("%w: %w", ErrReferencePrincipal, err)
	}
	legResponse, err := client.Do(request)
	if err != nil {
		return fmt.Errorf("%w: %w", ErrReferencePrincipal, transportError(target, err))
	}
	response := legResponse.Response
	defer func() { _ = response.Body.Close() }()
	body, err := io.ReadAll(io.LimitReader(response.Body, 1<<20))
	if err != nil {
		return fmt.Errorf("%w: read %s: %w", ErrReferencePrincipal, EndpointLabel(target), err)
	}
	switch {
	case response.StatusCode != http.StatusOK:
		return fmt.Errorf("%w: %s answered HTTP %d", ErrReferencePrincipal, EndpointLabel(target), response.StatusCode)
	case ServedUnderImpersonation(response.Header):
		return fmt.Errorf("%w: %s carried the %s header: the baseline principal holds an impersonation session, so the Python legs would answer for its target org", ErrReferencePrincipal, EndpointLabel(target), impersonationHeader)
	case !strings.EqualFold(strings.TrimSpace(response.Header.Get("Server")), ReferencePlaneServer):
		return fmt.Errorf("%w: %s did not answer as the Python app (no `server: %s` header)", ErrReferencePrincipal, EndpointLabel(target), ReferencePlaneServer)
	case response.Header.Get(buildHeader) != "":
		return fmt.Errorf("%w: %s answered with query-api's %s header", ErrReferencePrincipal, EndpointLabel(target), buildHeader)
	}
	var me struct {
		OrgID *string `json:"org_id"`
	}
	if err := json.Unmarshal(body, &me); err != nil {
		return fmt.Errorf("%w: %s did not answer a JSON object", ErrReferencePrincipal, EndpointLabel(target))
	}
	if me.OrgID == nil || *me.OrgID != org {
		return fmt.Errorf("%w: %s resolves the baseline credential to a different org than -org", ErrReferencePrincipal, EndpointLabel(target))
	}
	return nil
}

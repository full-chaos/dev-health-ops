package githubapp

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/full-chaos/dev-health-ops/internal/api/credentials"
	"github.com/full-chaos/dev-health-ops/internal/api/pyjson"
)

// verdict is the outcome of the GitHub side of the callback: a Python
// HTTPException (status and detail), or an error Python would have let
// escape as a 500.
type verdict struct {
	status int
	detail string
	err    error
}

const authorizationNotVerified = "GitHub user authorization could not be verified"

// verifyInstallerAccess is _verify_installer_installation_access: exchange the
// OAuth code for the installer's user token, then find the installation among
// the installations that user can access.
func (h handlers) verifyInstallerAccess(ctx context.Context, installationID *big.Int, code string) (*pyjson.Object, verdict) {
	if h.Config.ClientID == "" || h.Config.ClientSecret == "" {
		return nil, verdict{status: http.StatusInternalServerError, detail: "GitHub App OAuth client credentials are required"}
	}
	form := url.Values{}
	form.Set("client_id", h.Config.ClientID)
	form.Set("client_secret", h.Config.ClientSecret)
	form.Set("code", code)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, h.GitHubURL+"/login/oauth/access_token", strings.NewReader(form.Encode()))
	if err != nil {
		return nil, verdict{err: err}
	}
	request.Header.Set("Accept", "application/json")
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	status, payload, err := h.fetchJSON(request)
	if err != nil {
		return nil, verdict{err: err}
	}
	if status >= 400 {
		return nil, verdict{status: http.StatusBadRequest, detail: authorizationNotVerified}
	}
	tokenObject, ok := payload.(*pyjson.Object)
	if !ok {
		return nil, verdict{err: errors.New("token response is not a JSON object")}
	}
	rawToken, _ := tokenObject.Get("access_token")
	accessToken, isString := rawToken.(string)
	if !isString || accessToken == "" {
		return nil, verdict{status: http.StatusBadRequest, detail: authorizationNotVerified}
	}
	return h.findAccessibleInstallation(ctx, accessToken, installationID)
}

// findAccessibleInstallation is _find_accessible_installation: page through
// GET /user/installations (100 a page) until the installation appears.
func (h handlers) findAccessibleInstallation(ctx context.Context, accessToken string, installationID *big.Int) (*pyjson.Object, verdict) {
	for page := 1; ; page++ {
		request, err := http.NewRequestWithContext(ctx, http.MethodGet,
			h.GitHubAPIURL+"/user/installations?per_page=100&page="+strconv.Itoa(page), nil)
		if err != nil {
			return nil, verdict{err: err}
		}
		request.Header.Set("Authorization", "Bearer "+accessToken)
		request.Header.Set("Accept", "application/vnd.github+json")
		request.Header.Set("X-GitHub-Api-Version", "2022-11-28")
		status, payload, err := h.fetchJSON(request)
		if err != nil {
			return nil, verdict{err: err}
		}
		if status >= 400 {
			return nil, verdict{status: http.StatusBadRequest, detail: authorizationNotVerified}
		}
		object, ok := payload.(*pyjson.Object)
		if !ok {
			return nil, verdict{err: errors.New("installations response is not a JSON object")}
		}
		rawList, _ := object.Get("installations")
		installations, isList := rawList.([]pyjson.Value)
		if !isList {
			return nil, verdict{status: http.StatusBadRequest, detail: authorizationNotVerified}
		}
		for _, item := range installations {
			installation, isObject := item.(*pyjson.Object)
			if !isObject {
				continue
			}
			id, _ := installation.Get("id")
			if credentials.PyEqual(id, pyjson.Int{Int: installationID}) {
				return installation, verdict{}
			}
		}
		if len(installations) < 100 {
			break
		}
	}
	return nil, verdict{status: http.StatusForbidden, detail: "installer does not have access to this GitHub App installation"}
}

// fetchJSON performs request and decodes a 2xx-or-error body the way
// httpx's response.json() does for the statuses Python reads it for: the body
// is decoded only when the status is below 400 (a 4xx/5xx answer is refused
// before its body is looked at).
func (h handlers) fetchJSON(request *http.Request) (int, pyjson.Value, error) {
	response, err := h.HTTPClient.Do(request)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	raw, err := io.ReadAll(response.Body)
	if err != nil {
		return 0, nil, err
	}
	if response.StatusCode >= 400 {
		return response.StatusCode, nil, nil
	}
	value, err := pyjson.Decode(raw)
	if err != nil {
		return 0, nil, fmt.Errorf("response body is not JSON: %w", err)
	}
	return response.StatusCode, value, nil
}

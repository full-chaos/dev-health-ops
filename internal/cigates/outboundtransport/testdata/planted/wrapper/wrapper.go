package wrapper

import (
	"net/http"

	"github.com/full-chaos/dev-health-ops/internal/atlassianteams"
)

func OverParameter(base http.RoundTripper) *http.Client {
	return &http.Client{Transport: atlassianteams.CompletePagesOnly(base)}
}

func OverNil() *http.Client {
	return &http.Client{Transport: atlassianteams.CompletePagesOnly(nil)}
}

func OverLiteral() *http.Client {
	return &http.Client{Transport: atlassianteams.CompletePagesOnly(&http.Transport{})}
}

func OverFallback(base http.RoundTripper) *http.Client {
	if base == nil {
		base = &http.Transport{}
	}
	return &http.Client{Transport: atlassianteams.CompletePagesOnly(base)}
}

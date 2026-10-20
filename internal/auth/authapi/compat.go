package authapi

import (
	"net/http"
)

// Compatibility route patterns: go-api's own paths.
const (
	PatternCompatLogin       = "/api/v1/auth/login"
	PatternCompatRefresh     = "/api/v1/auth/refresh"
	PatternCompatLogout      = "/api/v1/auth/logout"
	PatternCompatSocialLogin = "/api/v1/auth/social-login"
)

const opSocialLogin = "social_login"

func notBuilt(w http.ResponseWriter) { w.WriteHeader(http.StatusNotImplemented) }

func (h handlers) compatRefresh(w http.ResponseWriter, _ *http.Request) { notBuilt(w) }

func (h handlers) compatLogout(w http.ResponseWriter, _ *http.Request) { notBuilt(w) }

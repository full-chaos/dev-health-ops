// Package nilfield is a fixture of the site walker: literals of a replaced-module type that leave its *http.Client field unset.
package nilfield

import (
	nethttp "net/http"

	"github.com/full-chaos/dev-health-ops/internal/httpguard/testdata/redirectsites/outside"
)

func Unset() *outside.Config { return &outside.Config{Name: "a"} }

func New() *outside.Config { return new(outside.Config) }

func Set(c *nethttp.Client) *outside.Config { return &outside.Config{HTTPClient: c} }

func Positional(c *nethttp.Client) outside.Config { return outside.Config{c, "x"} }

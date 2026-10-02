// Package negative is a fixture of the site walker: shapes that are NOT sites.
package negative

import (
	"net/http"
	"net/url"
)

func build() *http.Client { return nil }

func NotSites(c *http.Client, r *http.Request) {
	_ = build()
	_, _ = url.Parse("")
	_ = c.Timeout
	_, _ = c.Do(r)
	_, _ = c.Get("")
}

type shapeless struct{ n int }

// Conversions that are not between a client and a type of its shape.
func Conversions(c *http.Client, n int) {
	_ = (*http.Client)(c)
	_ = int64(n)
	_ = shapeless(struct{ n int }{n})
}

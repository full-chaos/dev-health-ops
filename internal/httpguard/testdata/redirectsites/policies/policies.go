// Package policies is a fixture of the site walker: the class derived from the code of a function.
package policies

import (
	nethttp "net/http"

	"github.com/full-chaos/dev-health-ops/internal/httpguard"
	"github.com/full-chaos/dev-health-ops/internal/providerfoundation"
)

func refuse(*nethttp.Request, []*nethttp.Request) error { return nethttp.ErrUseLastResponse }

func Guarded(c *nethttp.Client) *nethttp.Client { return httpguard.NoRedirects(c) }

func GuardedNew() *nethttp.Client { return httpguard.NewClient(0) }

func RefuseLiteral() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(*nethttp.Request, []*nethttp.Request) error { return nethttp.ErrUseLastResponse }}
}

func RefuseFunction() *nethttp.Client { return &nethttp.Client{CheckRedirect: refuse} }

func RefuseAssign(c *nethttp.Client) { c.CheckRedirect = refuse }

func Drop() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: providerfoundation.DropCredentialsOnHostChange}
}

func Custom() *nethttp.Client {
	return &nethttp.Client{CheckRedirect: func(*nethttp.Request, []*nethttp.Request) error { return nil }}
}

func Mixed(c *nethttp.Client, d *nethttp.Client) {
	c.CheckRedirect = refuse
	d.CheckRedirect = providerfoundation.DropCredentialsOnHostChange
}

func Bare() *nethttp.Client { return &nethttp.Client{} }

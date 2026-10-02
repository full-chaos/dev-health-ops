// Package assign is a fixture of the site walker: an assignment to CheckRedirect, direct and through an embedded client.
package assign

import nethttp "net/http"

type wrapper struct{ *nethttp.Client }

func Direct(c *nethttp.Client) { c.CheckRedirect = nil }

func Promoted(w wrapper) { w.CheckRedirect = nil }

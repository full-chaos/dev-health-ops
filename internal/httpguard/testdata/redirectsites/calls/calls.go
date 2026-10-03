// Package calls is a fixture of the site walker: the package-level request helpers, called and as a method value.
package calls

import nethttp "net/http"

func Call() {
	_, _ = nethttp.Get("")
	_, _ = nethttp.Post("", "", nil)
	_, _ = nethttp.PostForm("", nil)
	_, _ = nethttp.Head("")
	get := nethttp.Get
	_ = get
}

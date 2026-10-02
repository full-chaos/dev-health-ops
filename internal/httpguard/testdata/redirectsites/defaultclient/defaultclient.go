// Package defaultclient is a fixture of the site walker: http.DefaultClient through an alias.
package defaultclient

import nethttp "net/http"

func Use() { _ = nethttp.DefaultClient }

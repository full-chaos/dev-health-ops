// Package sub is a sub-package of the replaced module: its types are replaced-module types too.
package sub

import nethttp "net/http"

type Config struct {
	HTTPClient *nethttp.Client
}

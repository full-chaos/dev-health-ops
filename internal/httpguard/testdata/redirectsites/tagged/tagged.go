// Package tagged is a fixture of the site walker: files outside the linux builds are a stated limit, not a silent one.
package tagged

import nethttp "net/http"

func Linux() *nethttp.Client { return nil }

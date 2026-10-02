package tagged

import nethttp "net/http"

func Arm() *nethttp.Client { return &nethttp.Client{} }

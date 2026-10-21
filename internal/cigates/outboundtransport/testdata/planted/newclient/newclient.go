package newclient

import nethttp "net/http"

func New() *nethttp.Client { return new(nethttp.Client) }

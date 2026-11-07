package main

import (
	"io"
	"net/http"
	"os"
	"time"
)

// Hosts: skeleton.
var Hosts = map[string]string{
	"api.stripe.com":         "stripe",
	"api.pagerduty.com":      "pagerduty",
	"identity.pagerduty.com": "pagerduty",
	"github.com":             "github",
	"api.github.com":         "github",
}

type hitLog struct {
	w   io.Writer
	now func() time.Time
}

func openLog(path string) (*os.File, error) {
	return os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL|os.O_APPEND, 0o600)
}

// handler: skeleton, no behaviour yet.
func handler(log *hitLog) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusNotImplemented) })
}

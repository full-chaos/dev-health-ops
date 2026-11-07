package shadowvenue

import "strings"

// StubHosts: skeleton.
var StubHosts = []string{"api.github.com", "api.pagerduty.com", "api.stripe.com", "github.com", "identity.pagerduty.com"}

// EgressRead: skeleton.
type EgressRead struct {
	Name   string
	Fields map[string]string
	Err    error
}

// ParseEgress: skeleton, no checks yet.
func ParseEgress(name string, raw []byte) EgressRead {
	read := EgressRead{Name: name, Fields: map[string]string{}}
	for _, line := range strings.Split(strings.TrimRight(string(raw), "\n"), "\n") {
		key, value, _ := strings.Cut(line, "=")
		read.Fields[key] = value
	}
	return read
}

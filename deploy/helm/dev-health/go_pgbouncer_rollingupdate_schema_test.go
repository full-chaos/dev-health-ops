package devhealth_test

import (
	"strings"
	"testing"
)

const schemaRefusal = "values don't meet the specifications of the schema"

func TestTransactionRollingUpdateSchemaRefusesSurgeZeroWithoutUnavailable(t *testing.T) {
	bad := map[string]string{
		"surge 0 alone":  `{"maxSurge":0}`,
		"both zero":      `{"maxSurge":0,"maxUnavailable":0}`,
		"surge 0 with %": `{"maxSurge":0,"maxUnavailable":"1%"}`,
		"percentage":     `{"maxSurge":"25%"}`,
	}
	for name, v := range bad {
		set := []string{"--set-json", txnSurgeKey + "=" + v}
		_, out, err := renderAuth(t, authWith(set...)...)
		if err == nil || !strings.Contains(out, schemaRefusal) || !strings.Contains(out, "rollingUpdate") {
			t.Errorf("%s: the SCHEMA must refuse (budget off, helper not reached): err=%v out=%.400s", name, err, out)
		}
	}
}

func TestTransactionRollingUpdateSchemaAcceptsSurgeZeroWithUnavailable(t *testing.T) {
	ok := map[string]string{
		"surge 0 unavailable 1": `{"maxSurge":0,"maxUnavailable":1}`,
		"surge 1 unavailable 0": `{"maxSurge":1,"maxUnavailable":0}`,
		"unavailable 0 alone":   `{"maxUnavailable":0}`,
		"surge 2":               `{"maxSurge":2}`,
		"empty (default)":       `{}`,
	}
	for name, v := range ok {
		set := []string{"--set-json", txnSurgeKey + "=" + v}
		if _, out, err := renderAuth(t, authWith(set...)...); err != nil {
			t.Errorf("%s must render: %v\n%.400s", name, err, out)
		}
	}
}

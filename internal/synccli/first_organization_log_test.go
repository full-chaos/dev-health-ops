package synccli

import (
	"context"
	"strings"
	"testing"
)

func TestFirstOrganizationLogsAnUnreachableDatabaseWithoutTheDSN(t *testing.T) {
	log := captureLookupLog(t)
	id, found := firstOrganization(context.Background(), "postgresql://dho_user:s3cret-pw@127.0.0.1:1/dho?connect_timeout=2")
	if found || id != "" {
		t.Fatalf("firstOrganization = %q, %t; want no organization", id, found)
	}
	for _, want := range []string{`"msg":"synccli.github_credential_lookup_failed"`, `"reason":"database_connect_failed"`, `"org_id":""`, `"error_class":`, `"error_type":`} {
		if !strings.Contains(log.String(), want) {
			t.Fatalf("log lacks %q: %s", want, log.String())
		}
	}
	for _, secret := range []string{"s3cret-pw", "dho_user", "127.0.0.1"} {
		if strings.Contains(log.String(), secret) {
			t.Fatalf("log carries %q: %s", secret, log.String())
		}
	}
}

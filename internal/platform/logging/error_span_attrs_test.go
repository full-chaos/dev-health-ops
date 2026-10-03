package logging

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestErrorSpanAttributesCarryIdentityNeverText(t *testing.T) {
	err := fmt.Errorf("dial https://u:planted-userinfo@host.example.test/?x=planted-query-7936: %w", errors.New("planted-cause"))
	got := map[string]string{}
	for _, attr := range ErrorSpanAttributes(err) {
		got[string(attr.Key)] = attr.Value.Emit()
	}
	if got["error.class"] != "other" || got["error.type"] != "*errors.errorString" {
		t.Fatalf("attributes = %v, want class other and the innermost type", got)
	}
	for key, value := range got {
		if !strings.HasPrefix(key, "error.") || strings.Contains(value, "planted") || strings.Contains(value, "host.example.test") {
			t.Fatalf("attribute %s = %q", key, value)
		}
	}
}

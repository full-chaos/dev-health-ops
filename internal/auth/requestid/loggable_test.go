package requestid

import (
	"strings"
	"testing"

	"github.com/google/uuid"
)

func TestLoggableWireKeepsOnlyTheWireForm(t *testing.T) {
	minted := WireOf(uuid.New())
	if got := LoggableWire(minted); got != minted {
		t.Fatalf("LoggableWire(%q) = %q, want it unchanged", minted, got)
	}
	if got := LoggableWire(""); got != "" {
		t.Fatalf("LoggableWire(\"\") = %q, want \"\"", got)
	}
	for _, value := range []string{
		"req_" + strings.Repeat("A", 25),
		"req_" + strings.Repeat("A", 27),
		"req_" + strings.Repeat("a", 26),
		"req_" + strings.Repeat("A", 25) + "8",
		"req_" + strings.Repeat("A", 25) + "-",
		"REQ_" + strings.Repeat("A", 26),
		"req_" + strings.Repeat("A", 24) + "\r\n",
		"req_" + strings.Repeat("A", 23) + "\xed\xa0\x80",
	} {
		if got := LoggableWire(value); got != "invalid" {
			t.Errorf("LoggableWire(%q) = %q, want \"invalid\"", value, got)
		}
	}
}

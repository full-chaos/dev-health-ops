package shadowvenue

import (
	"io"
	"time"
)

// StubServices: skeleton.
var StubServices = []string{"stripe", "pagerduty", "github", "mail"}

// VerdictOutboundDiffers: skeleton.
const VerdictOutboundDiffers = "MISMATCH_OUTBOUND"

// StubHit: skeleton.
type StubHit struct {
	At                          time.Time
	Service, Method, Host, Path string
}

// ParseStubLog: skeleton, no behaviour yet.
func ParseStubLog(raw []byte) ([]StubHit, error) { return nil, nil }

// Outbound: skeleton.
type Outbound struct {
	Logged bool
	Hits   []StubHit
	Ext    map[string]string
}

// CheckRowKeys: skeleton, no behaviour yet.
func CheckRowKeys(slots []Slot, rows []Row) error { return nil }

// OutboundSlot: skeleton.
type OutboundSlot struct {
	Row, Caller string
	K           int
	Key         string
	Status      int
	Hits        int
}

// WriteOutbound: skeleton, no behaviour yet.
func (r Report) WriteOutbound(w io.Writer) error { return nil }

// ParseOutbound: skeleton, no behaviour yet.
func ParseOutbound(raw []byte) ([]OutboundSlot, error) { return nil, nil }

func throwawayOnly(row Row, throwaway string) error { return nil }

package policy

import (
	"errors"
	"log/slog"
	"net/http"
	"sync"

	"github.com/full-chaos/dev-health-go/pep"
)

// Closed label value sets of the shadow counters.
var (
	shadowNamespaces = []string{"admin", "billing", "integrations", "license", "members", "settings", "teams"}
	shadowCallers    = []string{"member", "admin", "superuser"}
	shadowClasses    = []string{"legacy_bug", "new_bug", "unresolved", "intentional"}
	shadowResults    = []string{"match", "mismatch", "error", "dropped", "skipped"}
)

// ShadowRow is one go-api endpoint-profile row that names a catalog action.
type ShadowRow struct {
	Action       string
	Namespace    string
	ResourceType string
	HighRisk     bool
}

// ParseShadowRows reads the rows of service that name a catalog action.
func ParseShadowRows(raw []byte, service string) (map[string]ShadowRow, error) {
	return nil, errors.New("policy: shadow rows are not implemented")
}

// ShadowClassRule is one row of the mismatch class table.
type ShadowClassRule struct {
	Legacy, PDP, Reason, Namespace, Class string
}

// LoadShadowClasses parses a class table.
func LoadShadowClasses(raw []byte) ([]ShadowClassRule, error) {
	return nil, errors.New("policy: shadow classes are not implemented")
}

// ShadowConfig builds a Shadow.
type ShadowConfig struct {
	Decider       pep.Decider
	Rows          map[string]ShadowRow
	Classes       []byte
	HighRisk      bool
	SamplePercent int
	MaxInFlight   int
	Logger        *slog.Logger
}

// Shadow asks the policy decision point beside the legacy guard.
type Shadow struct {
	wg sync.WaitGroup
}

// NewShadow checks cfg.
func NewShadow(cfg ShadowConfig) (*Shadow, error) { return &Shadow{}, nil }

// Observe never changes the response.
func (s *Shadow) Observe(r *http.Request, user *User, legacyAllowed bool) {}

// SetShadow installs s beside the guard. Call it once, before the guard serves.
func (g *Guard) SetShadow(s *Shadow) {}

var shadowClassTable []byte

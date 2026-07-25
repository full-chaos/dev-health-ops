package rivercompat

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	// ContractVersion is the only payload version accepted by the Phase 0 probe.
	ContractVersion = 1
	// JobKind is shared with the Python riverqueue compatibility producer.
	JobKind = "chaos3034_compat_v1"
)

// JobArgs is the versioned, language-neutral payload used by the compatibility
// probe. Keep this envelope aligned with python_enqueue.py and its fixtures.
type JobArgs struct {
	ContractVersion int    `json:"contract_version"`
	Marker          string `json:"marker"`
	Source          string `json:"source"`
}

func (JobArgs) Kind() string { return JobKind }

// Validate rejects payload drift before the probe records a successful Go
// execution. Production job contracts will have their own version registry.
func (a JobArgs) Validate() error {
	if a.ContractVersion != ContractVersion {
		return fmt.Errorf("unsupported contract version %d", a.ContractVersion)
	}
	if strings.TrimSpace(a.Marker) == "" {
		return errors.New("marker is required")
	}
	if a.Source != "go" && a.Source != "python" {
		return fmt.Errorf("unsupported source %q", a.Source)
	}
	return nil
}

// DecodeJobArgs uses a strict decoder so fixture changes cannot silently add
// fields that one language ignores.
func DecodeJobArgs(data []byte) (JobArgs, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var args JobArgs
	if err := decoder.Decode(&args); err != nil {
		return JobArgs{}, fmt.Errorf("decode job args: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return JobArgs{}, fmt.Errorf("decode job args: %w", err)
	}
	if err := args.Validate(); err != nil {
		return JobArgs{}, fmt.Errorf("validate job args: %w", err)
	}
	return args, nil
}

package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
)

const (
	contractVersion = 1
	jobKind         = "chaos3034_compat_v1"
)

// jobArgs is the exact cross-version and cross-language compatibility
// envelope. The kind is supplied through Kind and is not part of the JSON.
type jobArgs struct {
	ContractVersion int    `json:"contract_version"`
	Marker          string `json:"marker"`
	Source          string `json:"source"`
}

func (jobArgs) Kind() string { return jobKind }

func (a jobArgs) validate() error {
	if a.ContractVersion != contractVersion {
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

func decodeJobArgs(data []byte) (jobArgs, error) {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()

	var args jobArgs
	if err := decoder.Decode(&args); err != nil {
		return jobArgs{}, fmt.Errorf("decode job args: %w", err)
	}
	if err := decoder.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return jobArgs{}, fmt.Errorf("decode job args: %w", err)
	}
	if err := args.validate(); err != nil {
		return jobArgs{}, fmt.Errorf("validate job args: %w", err)
	}
	return args, nil
}

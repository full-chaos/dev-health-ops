package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

func TestSharedFixturesDecode(t *testing.T) {
	t.Parallel()

	for _, test := range []struct {
		name   string
		marker string
		source string
	}{
		{name: "python_job_v1.json", marker: "python-fixture", source: "python"},
		{name: "go_job_v1.json", marker: "go-fixture", source: "go"},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			data, err := os.ReadFile(filepath.Join("..", "fixtures", test.name))
			if err != nil {
				t.Fatalf("read shared fixture: %v", err)
			}
			args, err := decodeJobArgs(data)
			if err != nil {
				t.Fatalf("decodeJobArgs() error = %v", err)
			}
			if args.ContractVersion != contractVersion || args.Marker != test.marker || args.Source != test.source {
				t.Fatalf("decodeJobArgs() = %#v", args)
			}
			if args.Kind() != jobKind {
				t.Fatalf("Kind() = %q, want %q", args.Kind(), jobKind)
			}
		})
	}
}

func TestGoContractEncodingIsExact(t *testing.T) {
	t.Parallel()

	encoded, err := json.Marshal(jobArgs{
		ContractVersion: contractVersion,
		Marker:          "go-fixture",
		Source:          "go",
	})
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	if string(encoded) != `{"contract_version":1,"marker":"go-fixture","source":"go"}` {
		t.Fatalf("json.Marshal() = %s", encoded)
	}
}

func TestDecodeJobArgsRejectsDrift(t *testing.T) {
	t.Parallel()

	for name, payload := range map[string]string{
		"unknown field":   `{"contract_version":1,"marker":"m","source":"go","extra":true}`,
		"unknown version": `{"contract_version":2,"marker":"m","source":"go"}`,
		"empty marker":    `{"contract_version":1,"marker":" ","source":"go"}`,
		"unknown source":  `{"contract_version":1,"marker":"m","source":"other"}`,
		"multiple values": `{"contract_version":1,"marker":"m","source":"go"} {}`,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := decodeJobArgs([]byte(payload)); err == nil {
				t.Fatal("decodeJobArgs() error = nil, want contract rejection")
			}
		})
	}
}

func TestResolveOperationAcceptsModeAlias(t *testing.T) {
	t.Parallel()

	if got, err := resolveOperation("", "work"); err != nil || got != "work" {
		t.Fatalf("resolveOperation() = %q, %v", got, err)
	}
	if _, err := resolveOperation("insert", "work"); err == nil {
		t.Fatal("resolveOperation() accepted conflicting flags")
	}
}

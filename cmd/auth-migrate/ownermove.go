package main

import (
	"context"
	"errors"
	"io"

	"github.com/full-chaos/dev-health-ops/internal/platform/secrets"
	"github.com/full-chaos/dev-health-ops/internal/storage/postgres/authschema"
)

const ownerMoveVerb = "owner-move"

func ownerMoveUsage() string { return "" }

func parseOwnerMoveArgs(args []string, lookup secrets.LookupEnv, stderr io.Writer) (authschema.OwnerMoveOptions, error) {
	return authschema.OwnerMoveOptions{}, errors.New("not implemented")
}

func executeOwnerMove(parent context.Context, args []string, lookup secrets.LookupEnv, stdout, stderr io.Writer) int {
	return 1
}

func schemaOwnerRole(lookup secrets.LookupEnv) (string, error) { return "unset", nil }

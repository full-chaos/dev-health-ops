package main

import (
	"io"

	"github.com/google/uuid"
)

// parsePlatformRoleBootstrapArgs reads the bootstrap command line.
func parsePlatformRoleBootstrapArgs(args []string, stderr io.Writer) (uuid.UUID, error) {
	_, _ = args, stderr
	return uuid.UUID{}, nil
}

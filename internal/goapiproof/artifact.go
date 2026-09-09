package goapiproof

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
)

// ArtifactStore writes response bodies to content-addressed files and
// returns a durable reference to each.
//
// go_api_proof_run.baseline_response_ref / candidate_response_ref are
// documented as "durable artifact references (e.g. object-storage key),
// never inlined response bodies -- this table must stay cheap to
// scan/index". A local directory is the deployment-independent form of
// that contract: the ref names the file, the file name IS the content
// digest, so a receipt can never point at a body that was edited after
// the fact without the ref changing too.
type ArtifactStore struct {
	Dir string
	// Scheme prefixes the returned ref. Defaults to "file". A deployment
	// that moves these to object storage changes this and Put's body,
	// nothing else.
	Scheme string
}

// NewArtifactStore creates dir if needed.
func NewArtifactStore(dir string) (*ArtifactStore, error) {
	if dir == "" {
		return nil, fmt.Errorf("goapiproof: an artifact directory is required -- a receipt with no stored response is not reviewable evidence")
	}
	if err := os.MkdirAll(dir, 0o750); err != nil {
		return nil, fmt.Errorf("goapiproof: create artifact dir: %w", err)
	}
	return &ArtifactStore{Dir: dir, Scheme: "file"}, nil
}

// Put stores body and returns its reference. Storing the same bytes twice
// is a no-op that returns the same ref: the name is the digest.
func (s *ArtifactStore) Put(body []byte) (string, error) {
	sum := sha256.Sum256(body)
	name := hex.EncodeToString(sum[:]) + ".json"
	path := filepath.Join(s.Dir, name)

	if _, err := os.Stat(path); err != nil {
		if !os.IsNotExist(err) {
			return "", fmt.Errorf("goapiproof: stat artifact: %w", err)
		}
		// Write via a temp file in the same directory and rename, so a
		// crash mid-write can never leave a file whose NAME claims a
		// digest its CONTENT does not have -- the one failure mode a
		// content-addressed store must not have.
		temp, err := os.CreateTemp(s.Dir, ".partial-*")
		if err != nil {
			return "", fmt.Errorf("goapiproof: create artifact temp: %w", err)
		}
		tempName := temp.Name()
		if _, err := temp.Write(body); err != nil {
			_ = temp.Close()
			_ = os.Remove(tempName)
			return "", fmt.Errorf("goapiproof: write artifact: %w", err)
		}
		if err := temp.Close(); err != nil {
			_ = os.Remove(tempName)
			return "", fmt.Errorf("goapiproof: close artifact: %w", err)
		}
		if err := os.Rename(tempName, path); err != nil {
			_ = os.Remove(tempName)
			return "", fmt.Errorf("goapiproof: place artifact: %w", err)
		}
	}

	scheme := s.Scheme
	if scheme == "" {
		scheme = "file"
	}
	return scheme + "://" + path, nil
}

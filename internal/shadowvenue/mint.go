package shadowvenue

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

var mintName = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9_-]*$`)

// MintResult is one minted token's public face: the principal name, the claims read back from it and its expiry.
type MintResult struct {
	Name, Sub, Org string
	Exp            time.Time
}

// MintLine is the one line the mint step prints for a result; it holds no token.
func MintLine(r MintResult) string {
	return strings.Join([]string{r.Name, r.Sub, r.Org, r.Exp.UTC().Format(time.RFC3339)}, "\t")
}

// Mint signs each named principal in with its org (the sign-in names the org, so a member of it gets an org claim) and
// writes one owner-only token file <name>.token in outDir. All sign-ins happen before any file is written, and a file
// is never overwritten; the token reaches nothing but its file. The claims of each token are read back and must be the
// principal's sub and org.
func Mint(ctx context.Context, d *Driver, venue Venue, names []string, outDir string) ([]MintResult, error) {
	if len(names) == 0 {
		return nil, errors.New("mint: no principal name")
	}
	seen := map[string]bool{}
	for _, name := range names {
		switch {
		case !mintName.MatchString(name):
			return nil, fmt.Errorf("mint: %q is not a principal name", name)
		case seen[name]:
			return nil, fmt.Errorf("mint: %s named twice", name)
		}
		seen[name] = true
		if p, ok := venue.Principal(name); !ok || p.Org == none {
			return nil, fmt.Errorf("mint: %s is no principal with an org in the principals file", name)
		}
	}
	info, err := os.Lstat(outDir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("mint: -out-dir %s: want an existing owner-only (0700) directory", outDir)
	}
	tokens := map[string]string{}
	var results []MintResult
	for _, name := range names {
		p, _ := venue.Principal(name)
		token, err := d.Login(ctx, p, venue.Orgs[p.Org])
		if err != nil {
			return nil, err
		}
		claims, err := DecodeClaims(token)
		if err != nil {
			return nil, fmt.Errorf("mint: the token of %s: %w", name, err)
		}
		if claims.Sub != WirePrincipal(p.UUID) || claims.Org != venue.orgClaim(p.Org) {
			return nil, fmt.Errorf("mint: the token of %s carries sub %s and org %s, not the principal's %s and %s", name, claims.Sub, claims.Org, WirePrincipal(p.UUID), venue.orgClaim(p.Org))
		}
		tokens[name] = token
		results = append(results, MintResult{Name: name, Sub: claims.Sub, Org: claims.Org, Exp: claims.Exp})
	}
	for _, name := range names {
		path := filepath.Join(outDir, name+".token")
		file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return nil, fmt.Errorf("mint: token file %s: %w", path, errors.Unwrap(err))
		}
		_, werr := file.WriteString(tokens[name] + "\n")
		if cerr := file.Close(); werr == nil {
			werr = cerr
		}
		if werr != nil {
			return nil, fmt.Errorf("mint: token file %s: write failed", path)
		}
	}
	return results, nil
}

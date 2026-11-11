package livee2e

import (
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"errors"
	"math/big"
	"os"
	"time"

	"github.com/full-chaos/dev-health-ops/internal/api/licensing"
)

func mintLicense(publicFile, licenseFile string) error {
	for _, path := range []string{publicFile, licenseFile} {
		if _, err := os.Lstat(path); !os.IsNotExist(err) {
			return errors.New("licence outputs must not exist and must be accessible")
		}
	}
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return errors.New("cannot generate licence key")
	}
	license, err := licensing.SignLicense(base64.StdEncoding.EncodeToString(private.Seed()), licensing.LicenseRequest{
		OrgID: "live-e2e", Tier: "enterprise", IssuedAt: time.Now().Unix(), LicenseID: "live-e2e", DurationDays: big.NewInt(2),
	})
	if err != nil {
		return errors.New("cannot sign licence")
	}
	var created []string
	complete := false
	defer func() {
		if !complete {
			for _, path := range created {
				_ = os.Remove(path)
			}
		}
	}()
	for _, output := range []struct{ path, value string }{
		{publicFile, base64.StdEncoding.EncodeToString(public)}, {licenseFile, license},
	} {
		f, err := os.OpenFile(output.path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
		if err != nil {
			return errors.New("cannot create licence output")
		}
		created = append(created, output.path)
		_, writeErr := f.WriteString(output.value)
		closeErr := f.Close()
		if writeErr != nil || closeErr != nil {
			return errors.New("cannot write licence output")
		}
	}
	complete = true
	return nil
}

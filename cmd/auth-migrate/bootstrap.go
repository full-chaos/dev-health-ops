package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/google/uuid"

	"github.com/full-chaos/dev-health-ops/internal/auth/session"
	"github.com/full-chaos/dev-health-ops/internal/auth/workload"
)

// bootstrapStore is what the bootstrap verbs use of *workload.Store.
type bootstrapStore interface {
	CreateBootstrap(ctx context.Context, name, value string, audiences []string, expiresIn time.Duration, a workload.Actor) (workload.BootstrapResult, error)
	RotateBootstrap(ctx context.Context, name, value string, overlap, expiresIn time.Duration, a workload.Actor) (workload.BootstrapResult, error)
	RevokeBootstrap(ctx context.Context, credentialID uuid.UUID, reason string, a workload.Actor) error
	DisableBootstrap(ctx context.Context, name string, a workload.Actor) (workload.BootstrapResult, error)
	LiveBootstrap(ctx context.Context, name string) ([]workload.BootstrapCredential, error)
	BootstrapStatus(ctx context.Context, credentialID uuid.UUID) (string, error)
}

var errOutExists = errors.New("the -out path exists")

// syncDir makes a new entry in dir durable: open, fsync and close, each error
// returned.
var syncDir = func(dir string) error {
	d, err := os.Open(dir) // #nosec G304 -- the operator-chosen output path's directory
	if err != nil {
		return err
	}
	serr := d.Sync()
	if cerr := d.Close(); serr == nil {
		serr = cerr
	}
	return serr
}

// writeSecretFile creates path (it must not exist; a symlink there is not
// followed) with mode 0600 whatever the umask, writes value with no newline,
// syncs and closes the file, then syncs its directory. Any failure removes
// path and is returned, so the caller registers nothing: a value is
// registered only once it and its directory entry are on disk.
func writeSecretFile(path, value string) (err error) {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600) // #nosec G304 -- operator-chosen output path, created exclusively
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return errOutExists
		}
		return errors.New("the -out file could not be created")
	}
	defer func() {
		if err != nil {
			_ = os.Remove(path)
		}
	}()
	if err := fillSecretFile(f, value); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return errors.New("the -out file could not be closed")
	}
	if err := syncDir(filepath.Dir(path)); err != nil {
		return errors.New("the -out directory could not be synced")
	}
	return nil
}

// replaceSecretFile writes value to a new 0600 file in path's directory, syncs it and renames it over path, then syncs
// the directory: a reader sees the old file or the new one, never a partial one, and a failure leaves path as it was.
// Only bootstrap-create -if-absent uses it, for a workload with no live value, where the file it replaces is dead.
func replaceSecretFile(path, value string) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".bootstrap-*") // #nosec G304 -- the operator-chosen output path's directory
	if err != nil {
		return errors.New("the -out directory is not writable")
	}
	name := tmp.Name()
	if err := fillSecretFile(tmp, value); err != nil {
		_ = tmp.Close()
		_ = os.Remove(name)
		return err
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return errors.New("the -out file could not be closed")
	}
	if err := os.Rename(name, path); err != nil {
		_ = os.Remove(name)
		return errors.New("the -out file could not be replaced")
	}
	if err := syncDir(dir); err != nil {
		_ = os.Remove(path)
		return errors.New("the -out directory could not be synced")
	}
	return nil
}

func fillSecretFile(f *os.File, value string) error {
	if err := f.Chmod(0o600); err != nil {
		return errors.New("the -out file mode could not be set")
	}
	if _, err := io.WriteString(f, value); err != nil {
		return errors.New("the -out file could not be written")
	}
	if err := f.Sync(); err != nil {
		return errors.New("the -out file could not be synced")
	}
	return nil
}

func when(t time.Time) string { return t.UTC().Format(time.RFC3339) }

// runBootstrap runs one bootstrap verb. The value is minted here, written to
// the -out file BEFORE the registry commit, and the file is removed if the
// commit fails; nothing this prints holds the value or its digest.
func runBootstrap(ctx context.Context, store bootstrapStore, parsed workloadsArgs, stdout, stderr io.Writer) int {
	fail := func(what string, err error) int {
		fmt.Fprintf(stderr, "workloads error: %s failed (%s)\n", what, failureClass(err))
		return 1
	}
	if !parsed.apply {
		return bootstrapDryRun(ctx, store, parsed, stdout, stderr)
	}
	switch parsed.verb {
	case "bootstrap-create", "bootstrap-rotate":
		if parsed.ifAbsent {
			live, err := store.LiveBootstrap(ctx, parsed.name)
			if err != nil {
				return fail(parsed.verb, err)
			}
			if len(live) > 0 {
				fmt.Fprintf(stdout, "workloads %s skipped: workload=%s live=%d (-if-absent; no change written, no file written)\n", parsed.verb, parsed.name, len(live))
				return 0
			}
		}
		value, err := workload.MintBootstrap()
		if err != nil {
			fmt.Fprintln(stderr, "workloads error: the system random source failed")
			return 1
		}
		write := writeSecretFile
		if parsed.ifAbsent {
			write = replaceSecretFile
		}
		if err := write(parsed.out, value); err != nil {
			fmt.Fprintf(stderr, "workloads error: %s: %v (nothing was registered)\n", parsed.verb, err)
			return 1
		}
		var res workload.BootstrapResult
		if parsed.verb == "bootstrap-create" {
			res, err = store.CreateBootstrap(ctx, parsed.name, value, parsed.audiences, parsed.expiresIn, workload.Actor{})
		} else {
			res, err = store.RotateBootstrap(ctx, parsed.name, value, parsed.overlap, parsed.expiresIn, workload.Actor{})
		}
		if err != nil {
			if rerr := os.Remove(parsed.out); rerr != nil {
				fmt.Fprintf(stderr, "workloads error: remove %s by hand: it holds a value that was not registered\n", parsed.out)
			}
			return fail(parsed.verb, err)
		}
		fmt.Fprintf(stdout, "workloads %s applied: workload=%s credential=%s expires_at=%s out=%s\n",
			parsed.verb, parsed.name, res.CredentialID, when(res.ExpiresAt), parsed.out)
		if parsed.verb == "bootstrap-rotate" {
			fmt.Fprintf(stdout, "previous credential=%s now expires_at=%s\n", res.ParentID, when(res.ParentExpiresAt))
		}
	case "bootstrap-revoke":
		if err := store.RevokeBootstrap(ctx, parsed.credentialID, parsed.reason, workload.Actor{}); err != nil {
			return fail(parsed.verb, err)
		}
		fmt.Fprintf(stdout, "workloads bootstrap-revoke applied: credential=%s reason=%s (revocation is terminal)\n", parsed.credentialID, parsed.reason)
	case "bootstrap-disable":
		res, err := store.DisableBootstrap(ctx, parsed.name, workload.Actor{})
		if err != nil {
			return fail(parsed.verb, err)
		}
		fmt.Fprintf(stdout, "workloads bootstrap-disable applied: workload=%s revoked=%d (workload tokens issued before this live at most 300 s)\n",
			parsed.name, res.Revoked)
	}
	return 0
}

// bootstrapDryRun reads and reports; it writes nothing, not even the -out file.
func bootstrapDryRun(ctx context.Context, store bootstrapStore, parsed workloadsArgs, stdout, stderr io.Writer) int {
	if parsed.verb == "bootstrap-revoke" {
		state, err := store.BootstrapStatus(ctx, parsed.credentialID)
		if err != nil {
			fmt.Fprintf(stderr, "workloads error: %s dry run failed (%s)\n", parsed.verb, failureClass(err))
			return 1
		}
		fmt.Fprintf(stdout, "workloads bootstrap-revoke dry run: credential=%s status=%s (no change written)\n", parsed.credentialID, state)
		return 0
	}
	if parsed.out != "" && !parsed.ifAbsent {
		if _, err := os.Lstat(parsed.out); err == nil {
			fmt.Fprintf(stderr, "workloads error: %s dry run: %v\n", parsed.verb, errOutExists)
			return 1
		}
	}
	live, err := store.LiveBootstrap(ctx, parsed.name)
	if err != nil {
		fmt.Fprintf(stderr, "workloads error: %s dry run failed (%s)\n", parsed.verb, failureClass(err))
		return 1
	}
	fmt.Fprintf(stdout, "workloads %s dry run: workload=%s live=%d (no change written, no file written)\n", parsed.verb, parsed.name, len(live))
	for _, c := range live {
		expiry := "none"
		if c.ExpiresAt != nil {
			expiry = when(*c.ExpiresAt)
		}
		fmt.Fprintf(stdout, "live credential=%s expires_at=%s\n", c.ID, expiry)
	}
	return 0
}

// subjectStore is what the subject verb uses of *workload.Store.
type subjectStore interface {
	Lookup(ctx context.Context, name string) (*workload.Info, error)
}

// runSubject writes the wire principal id of a registered workload to -out. The id names a principal and holds no
// secret, so the file is world-readable; it replaces -out atomically, so a rerun with the same registry changes nothing.
func runSubject(ctx context.Context, store subjectStore, parsed workloadsArgs, stdout, stderr io.Writer) int {
	info, err := store.Lookup(ctx, parsed.name)
	if err != nil {
		fmt.Fprintf(stderr, "workloads error: subject failed (%s)\n", failureClass(err))
		return 1
	}
	subject := session.WirePrincipalID(info.PrincipalID)
	if !parsed.apply {
		fmt.Fprintf(stdout, "workloads subject dry run: workload=%s subject=%s (no file written)\n", parsed.name, subject)
		return 0
	}
	if err := replaceFile(parsed.out, subject, 0o644); err != nil {
		fmt.Fprintf(stderr, "workloads error: subject: %v\n", err)
		return 1
	}
	fmt.Fprintf(stdout, "workloads subject applied: workload=%s subject=%s out=%s\n", parsed.name, subject, parsed.out)
	return 0
}

// replaceFile writes value to a new file beside path, syncs it and renames it over path; a failure leaves path as it was.
func replaceFile(path, value string, mode os.FileMode) error {
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".subject-*") // #nosec G304 -- the operator-chosen output path's directory
	if err != nil {
		return errors.New("the -out directory is not writable")
	}
	name := tmp.Name()
	fail := func(msg string) error {
		_ = tmp.Close()
		_ = os.Remove(name)
		return errors.New(msg)
	}
	if _, err := io.WriteString(tmp, value); err != nil {
		return fail("the -out file could not be written")
	}
	if err := tmp.Chmod(mode); err != nil {
		return fail("the -out file mode could not be set")
	}
	if err := tmp.Sync(); err != nil {
		return fail("the -out file could not be synced")
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(name)
		return errors.New("the -out file could not be closed")
	}
	if err := os.Rename(name, path); err != nil {
		_ = os.Remove(name)
		return errors.New("the -out file could not be replaced")
	}
	if err := syncDir(dir); err != nil {
		return errors.New("the -out directory could not be synced")
	}
	return nil
}

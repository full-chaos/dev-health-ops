package syncbucket

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// TestUnitWritersLockTheBucketFirst pins acp lock-1 rule 2 on providersync's unit writers: in Complete,
// CompleteLinearWorkItemFamily and failTx the bucket lock (repository.lockSlotBucket) is the first statement after the
// transaction begins, before any row write.
func TestUnitWritersLockTheBucketFirst(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("..", "providersync", "repository_postgres.go"))
	if err != nil {
		t.Fatal(err)
	}
	source := string(raw)
	for _, name := range []string{"Complete", "CompleteLinearWorkItemFamily", "failTx"} {
		start := strings.Index(source, "func (repository *PostgresRepository) "+name+"(")
		if start < 0 {
			t.Fatalf("%s not found", name)
		}
		body := source[start:]
		body = body[:strings.Index(body, "\n}\n")]
		begin := strings.Index(body, "repository.Pool.Begin(ctx)")
		if begin < 0 {
			t.Fatalf("%s: no transaction begin", name)
		}
		after := body[begin:]
		lock := strings.Index(after, "repository.lockSlotBucket(")
		first := len(after)
		for _, statement := range []string{"tx.Exec(", "tx.QueryRow(", "tx.Query(", "bumpSyncRunRollup(", "syncrunrollup."} {
			if at := strings.Index(after, statement); at >= 0 && at < first {
				first = at
			}
		}
		if lock < 0 || lock > first {
			t.Errorf("%s: the bucket lock is not the transaction's first statement (lock at %d, first statement at %d)", name, lock, first)
		}
	}
}

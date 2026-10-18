package authschema

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5"
)

// ErrRuntimeGrantsNarrowed reports that a run would remove a privilege the
// runtime role held directly before it. Acknowledge a deliberate narrowing
// with Options.AcknowledgeNarrowing (auth-migrate -ack-grant-narrowing).
var ErrRuntimeGrantsNarrowed = fmt.Errorf(
	"%w: the runtime role would lose grants it held before this run", ErrMigrationFailed,
)

// GrantRecord is one direct privilege held by the runtime role.
type GrantRecord struct {
	// Kind is "relation" (table, view or sequence), "column" or "schema".
	Kind      string
	Object    string
	Column    string
	Privilege string
	// Grantable is WITH GRANT OPTION. Losing the option is a narrowing even
	// when the privilege itself survives.
	Grantable bool
}

// String is for humans only. It is NOT injective (a quoted identifier may
// contain a dot or a space), so identity and the digest never use it.
func (g GrantRecord) String() string {
	option := ""
	if g.Grantable {
		option = " WITH GRANT OPTION"
	}
	if g.Column != "" {
		return fmt.Sprintf("%s %s.%s %s%s", g.Kind, g.Object, g.Column, g.Privilege, option)
	}
	return fmt.Sprintf("%s %s %s%s", g.Kind, g.Object, g.Privilege, option)
}

func (g GrantRecord) less(o GrantRecord) bool {
	if g.Kind != o.Kind {
		return g.Kind < o.Kind
	}
	if g.Object != o.Object {
		return g.Object < o.Object
	}
	if g.Column != o.Column {
		return g.Column < o.Column
	}
	if g.Privilege != o.Privilege {
		return g.Privilege < o.Privilege
	}
	return !g.Grantable && o.Grantable
}

// NarrowingError is the typed form of ErrRuntimeGrantsNarrowed. Digest is the
// value to pass back as Options.AcknowledgeNarrowing.
type NarrowingError struct {
	Schema string
	Role   string
	Lost   []GrantRecord
	Digest string
}

func (e *NarrowingError) Error() string {
	lines := make([]string, 0, len(e.Lost))
	for _, lost := range e.Lost {
		lines = append(lines, "  "+lost.String())
	}
	return fmt.Sprintf(
		"%v (schema %q, role %q); nothing was changed. If this narrowing is deliberate, "+
			"re-run with -ack-grant-narrowing=%s:\n%s",
		ErrRuntimeGrantsNarrowed, e.Schema, e.Role, e.Digest, strings.Join(lines, "\n"),
	)
}

func (e *NarrowingError) Unwrap() error { return ErrRuntimeGrantsNarrowed }

type rowQuerier interface {
	Query(ctx context.Context, sql string, args ...any) (pgx.Rows, error)
}

// snapshotDirectGrants reads the privileges the runtime role holds DIRECTLY in
// the schema, from the ACL columns, independent of the manifest.
//
// Direct only, by decision. aclexplode names the grantee that appears in the
// ACL, so a privilege the role merely inherits through a membership never
// shows up here; that is the same set REVOKE ... FROM role removes, so the
// before/after comparison is like for like. Inherited and effective authority
// is VerifyRuntimePosture's job (has_*_privilege), and comparing effective
// privileges here would blame the runtime role for changes to some other
// role's grants.
func snapshotDirectGrants(ctx context.Context, q rowQuerier, schema, role string) ([]GrantRecord, error) {
	rows, err := q.Query(ctx, `
		SELECT 'relation'::text, c.relname::text, ''::text, a.privilege_type::text, a.is_grantable
		FROM pg_class c
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL aclexplode(c.relacl) a
		WHERE n.nspname = $1
		  AND a.grantee = (SELECT oid FROM pg_roles WHERE rolname = $2)
		  AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
		UNION ALL
		SELECT 'column', c.relname::text, att.attname::text, a.privilege_type::text, a.is_grantable
		FROM pg_attribute att
		JOIN pg_class c ON c.oid = att.attrelid
		JOIN pg_namespace n ON n.oid = c.relnamespace
		CROSS JOIN LATERAL aclexplode(att.attacl) a
		WHERE n.nspname = $1
		  AND NOT att.attisdropped
		  AND a.grantee = (SELECT oid FROM pg_roles WHERE rolname = $2)
		UNION ALL
		SELECT 'schema', n.nspname::text, '', a.privilege_type::text, a.is_grantable
		FROM pg_namespace n
		CROSS JOIN LATERAL aclexplode(n.nspacl) a
		WHERE n.nspname = $1
		  AND a.grantee = (SELECT oid FROM pg_roles WHERE rolname = $2)`,
		schema, role,
	)
	if err != nil {
		return nil, fmt.Errorf("%w: reading the runtime role's direct grants", ErrMigrationFailed)
	}
	defer rows.Close()

	var grants []GrantRecord
	for rows.Next() {
		var g GrantRecord
		if err := rows.Scan(&g.Kind, &g.Object, &g.Column, &g.Privilege, &g.Grantable); err != nil {
			return nil, fmt.Errorf("%w: reading a direct grant", ErrMigrationFailed)
		}
		grants = append(grants, g)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("%w: reading the runtime role's direct grants", ErrMigrationFailed)
	}
	return grants, nil
}

// narrowedGrants returns what was held before and is absent after, sorted. A
// privilege held WITH GRANT OPTION is lost when only the plain privilege
// remains; the reverse is a widening, not a loss.
func narrowedGrants(before, after []GrantRecord) []GrantRecord {
	// held maps a privilege (ignoring the option) to whether the option is held.
	held := make(map[GrantRecord]bool, len(after))
	for _, g := range after {
		option := g.Grantable
		g.Grantable = false
		held[g] = held[g] || option
	}
	var lost []GrantRecord
	seen := make(map[GrantRecord]struct{}, len(before))
	for _, g := range before {
		if _, dup := seen[g]; dup {
			continue
		}
		seen[g] = struct{}{}
		plain := g
		plain.Grantable = false
		if option, ok := held[plain]; ok && (option || !g.Grantable) {
			continue
		}
		// A table-level privilege covers every column, so a column grant that
		// the manifest now holds at table level has not been lost.
		if g.Kind == "column" {
			table := GrantRecord{Kind: "relation", Object: g.Object, Privilege: g.Privilege}
			if option, ok := held[table]; ok && (option || !g.Grantable) {
				continue
			}
		}
		lost = append(lost, g)
	}
	sort.Slice(lost, func(i, j int) bool { return lost[i].less(lost[j]) })
	return lost
}

// narrowingDigest binds an acknowledgement to one exact set of lost grants.
func narrowingDigest(lost []GrantRecord) string {
	// JSON of the struct fields is injective: strings are quoted and escaped,
	// so no identifier can shift a boundary between fields or records.
	encoded, err := json.Marshal(lost)
	if err != nil {
		panic(fmt.Sprintf("authschema: encoding grant records: %v", err))
	}
	sum := sha256.Sum256(encoded)
	return hex.EncodeToString(sum[:])[:16]
}

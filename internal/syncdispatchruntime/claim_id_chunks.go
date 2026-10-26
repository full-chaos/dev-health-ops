package syncdispatchruntime

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
)

// claimIDChunkSize is the most ids one uuid[] bind of a claim statement carries. A var only so a test can make it small.
var claimIDChunkSize = 50000

// chunkIDs splits ids into binds of at most size; an empty list is one empty chunk so the statement keeps its shape.
func chunkIDs(ids []string, size int) [][]string {
	if len(ids) == 0 {
		return [][]string{{}}
	}
	var chunks [][]string
	for len(ids) > size {
		chunks = append(chunks, ids[:size])
		ids = ids[size:]
	}
	return append(chunks, ids)
}

// notAnyClauses renders `AND NOT (id = ANY($n::uuid[]))` for every chunk of every list, binding from parameter first. A
// unit is excluded when it is in ANY chunk, which is what ANDing the clauses means.
func notAnyClauses(first int, lists ...[]string) (string, []any) {
	var clauses strings.Builder
	var args []any
	for _, list := range lists {
		for _, chunk := range chunkIDs(list, claimIDChunkSize) {
			fmt.Fprintf(&clauses, "\n  AND NOT (id = ANY($%d::uuid[]))", first+len(args))
			args = append(args, chunk)
		}
	}
	return clauses.String(), args
}

// execClaimStatement runs a claim UPDATE once per chunk of the authorized ids. Each execution binds one chunk as
// `id = ANY(...)` and excludes every id of excluded lists through notAnyClauses; the chunks are disjoint and a claimed
// row leaves the status the statement selects, so the union of the executions is the single-statement result.
// build gets the text of the id predicates, whose parameters follow head.
func execClaimStatement(
	ctx context.Context, tx pgx.Tx, build func(idPredicates string) string,
	head []any, authorized []string, excluded ...[]string,
) ([]string, error) {
	exclusion, exclusionArgs := notAnyClauses(len(head)+1, excluded...)
	authorizedPosition := len(head) + len(exclusionArgs) + 1
	sql := build(fmt.Sprintf("\n  AND id = ANY($%d::uuid[])%s", authorizedPosition, exclusion))
	var claimed []string
	for _, chunk := range chunkIDs(authorized, claimIDChunkSize) {
		args := append(append(append([]any{}, head...), exclusionArgs...), chunk)
		ids, err := execReturningIDs(ctx, tx, sql, args...)
		if err != nil {
			return nil, err
		}
		claimed = append(claimed, ids...)
	}
	return claimed, nil
}

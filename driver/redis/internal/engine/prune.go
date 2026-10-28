package engine

import (
	"context"
	_ "embed"
	"fmt"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

// MaxPruneKeys is the maximum number of candidate values read by one cleanup call.
const MaxPruneKeys = 64

// PruneCursor is an opaque, caller-owned scan position. Start with its zero value.
// Keep returned values between calls; cursors are scope-bound and not serializable.
// Restart from zero after topology changes or process restarts. Concurrent changes
// make discovery best effort, so applications must schedule repeated full cycles.
type PruneCursor struct {
	// scope prevents using a cursor with a different namespace/domain.
	scope string
	// position is the SCAN position on the domain's current primary.
	position string
	// pending buffers overflow because SCAN COUNT is not a hard bound.
	pending []string
	// done distinguishes a completed cycle from the initial zero cursor.
	done bool
}

// Done reports completion of the scan cycle and any buffered candidates.
func (c PruneCursor) Done() bool { return c.done }

// PruneResult describes one cleanup step. Ignore it on error.
type PruneResult struct {
	// Cursor is the next position; repeat until Cursor.Done().
	Cursor PruneCursor
	// Examined is the number of candidate values checked, at most the requested limit.
	Examined int
	// Deleted is the number of logically expired values physically removed.
	Deleted int64
}

// scanScript routes discovery to the domain's primary using a same-slot anchor.
// It does not access values or derive keys for a mutation inside the script.
var scanScript = redis.NewScript(`return redis.call('SCAN', ARGV[1], 'MATCH', KEYS[1] .. '*', 'COUNT', ARGV[2])`)

// pruneSource rechecks candidate expiry before its only deletion command.
//
//go:embed prune.lua
var pruneSource string

// pruneScript shares the admission protocol's clock and expiry readers.
var pruneScript = redis.NewScript(protocolSource + pruneSource)

// Prune performs at most one SCAN page and checks at most limit candidate values,
// with 1 <= limit <= MaxPruneKeys. SCAN COUNT is only a discovery-work hint;
// overflow is buffered in the cursor. This is not a hard Redis latency bound.
// Active debt and live receipts are never reset. Malformed candidates abort the
// batch without deleting any keys. The caller owns scheduling; no worker is started.
func (l *Limiter) Prune(ctx context.Context, cursor PruneCursor, limit int) (PruneResult, error) {
	if err := l.ready(ctx); err != nil {
		return PruneResult{}, failure("prune", NotCharged, err)
	}
	if limit < 1 || limit > MaxPruneKeys || (cursor.scope != "" && cursor.scope != l.prefix) {
		return PruneResult{}, failure("prune", NotCharged, fmt.Errorf("%w: cleanup limit or cursor scope", ErrInvalid))
	}
	if cursor.done {
		return PruneResult{Cursor: cursor}, nil
	}
	next := cursor
	next.scope = l.prefix
	if len(next.pending) == 0 {
		position := next.position
		if position == "" {
			position = "0"
		}
		cmd := scanScript.Run(ctx, l.client, []string{l.prefix}, position, limit)
		if err := cmd.Err(); err != nil {
			return PruneResult{}, scriptFailure("prune", err)
		}
		values, err := cmd.Slice()
		if err != nil || len(values) != 2 {
			return PruneResult{}, failure("prune", Unknown, ErrState)
		}
		position, ok := values[0].(string)
		if _, err := strconv.ParseUint(position, 10, 64); !ok || err != nil {
			return PruneResult{}, failure("prune", Unknown, ErrState)
		}
		keys, ok := values[1].([]interface{})
		if !ok {
			return PruneResult{}, failure("prune", Unknown, ErrState)
		}
		next.position = position
		next.pending = nil
		for _, value := range keys {
			key, ok := value.(string)
			if !ok {
				return PruneResult{}, failure("prune", Unknown, ErrState)
			}
			if l.ownedKey(key) {
				next.pending = append(next.pending, key)
			}
		}
	}
	n := len(next.pending)
	if n > limit {
		n = limit
	}
	candidates := next.pending[:n]
	next.pending = next.pending[n:]
	next.done = next.position == "0" && len(next.pending) == 0
	result := PruneResult{Cursor: next, Examined: n}
	if n == 0 {
		return result, nil
	}
	cmd := pruneScript.Run(ctx, l.client, candidates)
	if err := cmd.Err(); err != nil {
		return PruneResult{}, scriptFailure("prune", err)
	}
	deleted, err := cmd.Int64()
	if err != nil || deleted < 0 || deleted > int64(n) {
		return PruneResult{}, failure("prune", Unknown, ErrState)
	}
	result.Deleted = deleted
	return result, nil
}

// ownedKey recognizes only SDK-generated quota and admission keys in this scope.
func (l *Limiter) ownedKey(key string) bool {
	suffix, ok := strings.CutPrefix(key, l.prefix)
	if !ok {
		return false
	}
	if strings.HasPrefix(suffix, "attempt:") {
		suffix = strings.TrimPrefix(suffix, "attempt:")
	} else {
		suffix = strings.TrimPrefix(suffix, "policy:")
	}
	if len(suffix) != 64 {
		return false
	}
	for _, c := range suffix {
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}

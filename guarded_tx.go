package redis

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
)

// Guarded transaction lifecycle errors.
var (
	// ErrTxGuardNotBegan is returned when Commit or Abandon is called before
	// Begin.
	ErrTxGuardNotBegan = errors.New("redis: transaction has not begun")
	// ErrTxGuardAlreadyBegan is returned when Begin is called twice on the same
	// TxGuard, including after a previous transaction on that guard finished.
	ErrTxGuardAlreadyBegan = errors.New("redis: transaction has already begun")
	// ErrTxGuardFinalized is returned when a guard is reused after Commit or
	// Abandon. A TxGuard is single use; create a new one for the next
	// transaction so that watched keys and buffered commands never leak across
	// transactions.
	ErrTxGuardFinalized = errors.New("redis: transaction has already finished")
)

// TxConflictError reports that a guarded transaction failed to commit because
// at least one watched key changed between WATCH and EXEC.
//
// ChangedKeys lists every watched key whose serialized value (including
// deletion) at commit time differs from the value captured when it was
// watched. Remaining is the number of commands buffered in the transaction's
// pipeline that never executed. None of them took effect.
type TxConflictError struct {
	ChangedKeys []string
	Remaining   int
}

func (e *TxConflictError) Error() string {
	return fmt.Sprintf(
		"redis: transaction conflict: watched keys changed: [%s]; pipeline remaining: %d",
		strings.Join(e.ChangedKeys, ", "), e.Remaining,
	)
}

// Is reports TxFailedErr for compatibility with code that checks the classic
// optimistic-lock failure.
func (e *TxConflictError) Is(target error) bool {
	return target == TxFailedErr
}

// keySnapshot is a serialized value of a key. exists is false when the key is
// missing, so deletion counts as a change rather than comparing equal to an
// empty value.
type keySnapshot struct {
	exists bool
	value  []byte
}

func snapshotEqual(a, b keySnapshot) bool {
	return a.exists == b.exists && bytes.Equal(a.value, b.value)
}

// TxGuard is a single-use, check-and-set transaction: WATCHed keys, an
// in-transaction command pipeline and Commit/Abandon all share one
// connection.
//
// Typical use:
//
//	g := client.NewTxGuard()
//	defer g.Close()
//	if err := g.Watch(ctx, "counter"); err != nil { ... }
//	pipe, err := g.Begin(ctx)
//	pipe.Incr(ctx, "counter")
//	cmds, err := g.Commit(ctx)
//
// On conflict Commit returns *TxConflictError naming the changed keys and the
// unexecuted command count, releases the WATCH and discards the pipeline.
// TxGuard is not safe for concurrent use.
type TxGuard struct {
	client *Client
	tx     *Tx

	keys []string // watched keys, in first-Watch order, de-duplicated
	// watchSnaps are the values captured immediately after each WATCH.
	watchSnaps map[string]keySnapshot
	// beginView holds the values of the watched keys at Begin time; reads of
	// watched keys buffered inside the transaction are answered from it.
	beginView map[string]string
	beginHas  map[string]bool

	began    bool
	finished bool

	pipe     *Pipeline
	buffered []Cmder
}

// NewTxGuard creates an empty guarded transaction. No connection is taken
// until Watch or Begin is called.
func (c *Client) NewTxGuard() *TxGuard {
	return &TxGuard{
		client:     c,
		watchSnaps: make(map[string]keySnapshot),
	}
}

func (g *TxGuard) ensureTx() {
	if g.tx == nil {
		g.tx = g.client.newTx()
	}
}

// Watch marks keys for conditional execution. Watch may be called more than
// once before Begin; each call extends the watched set.
func (g *TxGuard) Watch(ctx context.Context, keys ...string) error {
	if g.finished {
		return ErrTxGuardFinalized
	}
	if g.began {
		return ErrTxGuardAlreadyBegan
	}
	if len(keys) == 0 {
		return nil
	}
	g.ensureTx()
	if err := g.tx.Watch(ctx, keys...).Err(); err != nil {
		return err
	}
	snaps, err := g.dumpKeys(ctx, keys)
	if err != nil {
		// The WATCH itself succeeded; release it immediately so the failure
		// does not leave the connection armed.
		_ = g.tx.Unwatch(ctx).Err()
		return err
	}
	for _, key := range keys {
		if _, ok := g.watchSnaps[key]; !ok {
			g.keys = append(g.keys, key)
		}
		g.watchSnaps[key] = snaps[key]
	}
	return nil
}

// Pipeline returns an ordinary, immediately executed pipeline bound to the
// transaction's connection. Commands sent through it run right away and are
// never swept into a later Begin/Commit.
func (g *TxGuard) Pipeline() Pipeliner {
	g.ensureTx()
	return g.tx.Pipeline()
}

// Begin opens the transaction and returns the pipeline used to buffer its
// commands. Nothing is sent to the server until Commit, apart from the reads
// needed to establish the transaction's read view. Calling Begin twice, or on
// a finished guard, is an error; the two command sets are never merged.
func (g *TxGuard) Begin(ctx context.Context) (Pipeliner, error) {
	switch {
	case g.finished:
		return nil, ErrTxGuardFinalized
	case g.began:
		return nil, ErrTxGuardAlreadyBegan
	}
	g.ensureTx()
	g.began = true

	if len(g.keys) > 0 {
		reads := make([]*StringCmd, len(g.keys))
		pipe := g.tx.Pipeline()
		for i, key := range g.keys {
			reads[i] = pipe.Get(ctx, key)
		}
		if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, Nil) {
			g.invalidate()
			return nil, err
		}
		g.beginView = make(map[string]string, len(g.keys))
		g.beginHas = make(map[string]bool, len(g.keys))
		for i, key := range g.keys {
			err := reads[i].Err()
			switch {
			case err == nil:
				g.beginView[key] = reads[i].Val()
				g.beginHas[key] = true
			case errors.Is(err, Nil):
				g.beginHas[key] = false
			default:
				g.invalidate()
				return nil, err
			}
		}
	}

	g.pipe = &Pipeline{
		exec: func(_ context.Context, cmds []Cmder) error {
			g.buffered = append(g.buffered, cmds...)
			return nil
		},
	}
	g.pipe.init()
	return g.pipe, nil
}

// Commit validates the watched keys and, if none changed, atomically executes
// the buffered commands through MULTI/EXEC. Results are returned in the same
// order the commands were buffered.
//
// If a watched key changed (including being deleted) no buffered command
// executes and Commit returns *TxConflictError; the WATCH is released and the
// guard is finished. Without any watched keys this behaves like a plain
// MULTI/EXEC block.
func (g *TxGuard) Commit(ctx context.Context) ([]Cmder, error) {
	switch {
	case g.finished:
		return nil, ErrTxGuardFinalized
	case !g.began:
		return nil, ErrTxGuardNotBegan
	}

	// Flush anything still held in the pipeline buffer.
	if g.pipe.Len() > 0 {
		if _, err := g.pipe.Exec(ctx); err != nil {
			g.invalidate()
			return nil, err
		}
	}
	buffered := g.buffered

	// Serve reads of watched keys from the Begin view before anything else, so
	// a concurrent write is never visible through them even when the commit
	// later turns out to conflict. Everything else is committed atomically.
	execCmds := make([]Cmder, 0, len(buffered))
	touched := make(map[string]bool)
	for _, cmd := range buffered {
		if key, ok := watchedReadKey(cmd, g.keys); ok {
			// A read after a write to the same key in the same transaction
			// must observe that write; let EXEC serialize it instead.
			if !touched[key] {
				g.answerFromView(cmd, key)
				continue
			}
		}
		if key, ok := cmdKey(cmd); ok {
			touched[key] = true
		}
		execCmds = append(execCmds, cmd)
	}

	conflict, err := g.detectConflicts(ctx)
	if err != nil {
		g.invalidate()
		return nil, err
	}
	if len(conflict) > 0 {
		return nil, g.failConflict(ctx, conflict)
	}

	if len(execCmds) > 0 {
		wrapped := wrapMultiExec(ctx, execCmds)
		execErr := g.tx.processTxPipelineHook(ctx, wrapped)
		if execErr != nil {
			if errors.Is(execErr, TxFailedErr) {
				// A last-moment change raced the pre-check; identify the keys.
				raceConflict, _ := g.detectConflicts(ctx)
				return nil, g.failConflict(ctx, raceConflict)
			}
			// EXECABORT or another server/connection error: nothing applied.
			g.invalidate()
			return nil, execErr
		}
	}

	g.finish()
	return buffered, nil
}

// Abandon gives up a begun transaction: the WATCH is released, the buffered
// commands are discarded, and the guard is finished. Commands buffered before
// an Abandon never execute and never carry into a later guard.
func (g *TxGuard) Abandon(ctx context.Context) error {
	switch {
	case g.finished:
		return ErrTxGuardFinalized
	case !g.began:
		return ErrTxGuardNotBegan
	}
	return g.abandon(ctx)
}

// AbandonWatch releases a transaction that only watched keys and never began.
func (g *TxGuard) AbandonWatch(ctx context.Context) error {
	if g.finished {
		return ErrTxGuardFinalized
	}
	if g.began {
		return ErrTxGuardAlreadyBegan
	}
	return g.abandon(ctx)
}

func (g *TxGuard) abandon(ctx context.Context) error {
	if g.tx != nil && g.tx.watchArmed {
		if err := g.tx.Unwatch(ctx).Err(); err != nil {
			g.invalidate()
			return err
		}
	}
	g.finish()
	return nil
}

// Close releases the guard's connection. It is safe to call after Commit or
// Abandon. It never executes buffered commands.
func (g *TxGuard) Close() error {
	if g.tx == nil {
		g.finished = true
		return nil
	}
	return g.tx.Close(context.Background())
}

// finish marks the guard used and releases the connection without sending
// UNWATCH: callers have already released the watch (UNWATCH or a completed
// EXEC) or deliberately invalidate the guard.
func (g *TxGuard) finish() {
	g.finished = true
	g.began = false
	g.keys = nil
	g.watchSnaps = map[string]keySnapshot{}
	g.beginView = nil
	g.beginHas = nil
	g.buffered = nil
	g.pipe = nil
	if g.tx != nil {
		_ = g.tx.Close(context.Background())
		g.tx = nil
	}
}

// invalidate is used on connection/server failures or after a conflict: the
// watch and the pipeline are voided and the connection is discarded, so a
// reconnect can never auto-submit the buffer or inherit the WATCH.
func (g *TxGuard) invalidate() {
	if g.tx != nil {
		// The connection may be dead; skip the otherwise redundant round trip.
		g.tx.watchArmed = false
	}
	g.finish()
}

// failConflict reports a conflict, releases the still-armed WATCH and voids
// the pipeline.
func (g *TxGuard) failConflict(ctx context.Context, changed []string) error {
	if g.tx != nil && g.tx.watchArmed {
		_ = g.tx.Unwatch(ctx).Err()
	}
	remaining := len(g.buffered)
	g.finish()
	return &TxConflictError{ChangedKeys: changed, Remaining: remaining}
}

// detectConflicts DUMPs every watched key on the transaction connection and
// compares the values with the snapshots taken at Watch time. Deleted keys and
// newly created keys both show up as changes.
func (g *TxGuard) detectConflicts(ctx context.Context) ([]string, error) {
	if len(g.keys) == 0 {
		return nil, nil
	}
	current, err := g.dumpKeys(ctx, g.keys)
	if err != nil {
		return nil, err
	}
	var changed []string
	for _, key := range g.keys {
		if !snapshotEqual(g.watchSnaps[key], current[key]) {
			changed = append(changed, key)
		}
	}
	return changed, nil
}

// dumpKeys returns serialized snapshots of the given keys using one DUMP per
// key on the transaction's connection. A missing key yields exists=false.
func (g *TxGuard) dumpKeys(ctx context.Context, keys []string) (map[string]keySnapshot, error) {
	out := make(map[string]keySnapshot, len(keys))
	pipe := g.tx.Pipeline()
	cmds := make([]*StringCmd, len(keys))
	for i, key := range keys {
		cmds[i] = NewStringCmd(ctx, "dump", key)
		_ = pipe.Process(ctx, cmds[i])
	}
	if _, err := pipe.Exec(ctx); err != nil && !errors.Is(err, Nil) {
		return nil, err
	}
	for i, key := range keys {
		out[key] = snapshotFromCmd(cmds[i])
	}
	return out, nil
}

func snapshotFromCmd(cmd *StringCmd) keySnapshot {
	if errors.Is(cmd.Err(), Nil) {
		return keySnapshot{}
	}
	value, _ := cmd.Bytes()
	return keySnapshot{exists: true, value: value}
}

// watchedReadKey reports key when cmd is a GET of one of the watched keys;
// those reads are answered from the Begin-time view.
func watchedReadKey(cmd Cmder, keys []string) (string, bool) {
	args := cmd.Args()
	name, _ := args[0].(string)
	if len(args) != 2 || !strings.EqualFold(name, "get") {
		return "", false
	}
	key, ok := args[1].(string)
	if !ok {
		return "", false
	}
	for _, watched := range keys {
		if watched == key {
			return key, true
		}
	}
	return "", false
}

// answerFromView fills a GET command with the value the watched key had at
// Begin time, or redis.Nil when the key was missing then.
func (g *TxGuard) answerFromView(cmd Cmder, key string) {
	get, ok := cmd.(*StringCmd)
	if !ok {
		return
	}
	if !g.beginHas[key] {
		get.SetErr(Nil)
		return
	}
	get.SetVal(g.beginView[key])
}

// cmdKey returns the key a single-key command operates on. Only the write
// commands matter here: they invalidate the Begin-view shortcut for later
// reads of the same key in the same transaction.
func cmdKey(cmd Cmder) (string, bool) {
	args := cmd.Args()
	if len(args) < 2 {
		return "", false
	}
	name, _ := args[0].(string)
	switch strings.ToLower(name) {
	case "set", "get", "del", "incr", "incrby", "decr", "decrby", "append",
		"getset", "setnx", "setex", "psetex", "hset", "hsetnx", "hincrby",
		"lpush", "rpush", "sadd", "srem", "zadd", "zrem", "expire", "pexpire",
		"persist", "rename":
		key, ok := args[1].(string)
		return key, ok
	default:
		return "", false
	}
}

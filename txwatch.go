package redis

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

// TxConflictError is returned by WatchTx.Commit when one or more watched keys
// were modified (or deleted) by another client between Watch and Commit. It
// reports exactly which watched keys changed and how many pipelined commands
// were still queued (and therefore discarded) when the conflict was detected.
type TxConflictError struct {
	// Keys lists the watched keys that changed since Watch.
	Keys []string
	// Pending is the number of commands that remained in the transaction
	// pipeline and were discarded without taking effect.
	Pending int
}

func (e *TxConflictError) Error() string {
	return fmt.Sprintf("redis: transaction conflict: watched keys changed: %s (%d pipelined command(s) discarded)",
		strings.Join(e.Keys, ", "), e.Pending)
}

// IsTxConflictError reports whether err is (or wraps) a *TxConflictError and,
// if so, returns it.
func IsTxConflictError(err error) (*TxConflictError, bool) {
	var cerr *TxConflictError
	if errors.As(err, &cerr) {
		return cerr, true
	}
	return nil, false
}

// ErrWatchTxDone is returned when Commit or Discard is called on a WatchTx
// that has already been committed or discarded: a transaction can be
// finalized at most once, so Commit and Discard can never both succeed.
var ErrWatchTxDone = errors.New("redis: transaction already committed or discarded")

// ErrWatchTxNotStarted is returned by Commit when Begin was not called.
var ErrWatchTxNotStarted = errors.New("redis: transaction not started: call Begin first")

// ErrWatchTxStarted is returned by Begin when a transaction is already in
// progress: nested transactions are rejected, never merged.
var ErrWatchTxStarted = errors.New("redis: transaction already started: nested transactions are not supported")

type watchTxState int

const (
	watchTxIdle watchTxState = iota
	watchTxBegun
	watchTxDone
)

// keySnapshot captures a key's value at a point in time so later changes
// (including deletion) can be detected by comparison.
type keySnapshot struct {
	val    string
	exists bool
}

// WatchTx is a client-side transaction guard. It watches a set of keys on a
// dedicated (sticky) connection, buffers pipelined commands on that same
// connection, and commits them atomically only if none of the watched keys
// were modified by others since Watch. On conflict, Commit fails with a
// *TxConflictError naming the changed keys and the number of queued commands
// that were discarded; none of the queued commands take effect.
//
// Typical usage:
//
//	wtx := client.NewWatchTx()
//	defer wtx.Close(ctx)
//	if err := wtx.Watch(ctx, "key"); err != nil { ... }
//	if err := wtx.Begin(ctx); err != nil { ... }
//	old, _ := wtx.Get(ctx, "key").Result() // read the Begin-time view
//	wtx.Pipe().Set(ctx, "key", newVal, 0)  // queued, not executed yet
//	if _, err := wtx.Commit(ctx); err != nil { ... }
//
// Like Tx, WatchTx is NOT safe for concurrent use by multiple goroutines.
type WatchTx struct {
	tx   *Tx
	pipe Pipeliner

	state watchTxState

	// watched is the value snapshot taken at Watch time; it is the baseline
	// used to detect conflicting modifications at Commit.
	watched map[string]keySnapshot
	// view is the snapshot taken at Begin time; reads of watched keys inside
	// the transaction are served from it so a transaction never observes
	// writes made by others after it started.
	view map[string]keySnapshot
}

// NewWatchTx creates a transaction guard bound to a single connection of
// this client. The caller must Close it to release the connection.
func (c *Client) NewWatchTx() *WatchTx {
	tx := c.newTx()
	return &WatchTx{
		tx:   tx,
		pipe: tx.TxPipeline(),
	}
}

// Watch marks keys for conflict detection and records their current values as
// the baseline. Calling Watch again after a Commit or Discard starts a fresh
// transaction generation: keys from the previous generation are forgotten.
func (t *WatchTx) Watch(ctx context.Context, keys ...string) error {
	if t.state == watchTxBegun {
		return ErrWatchTxStarted
	}
	// A new Watch after a finalized (or not yet started) transaction resets
	// all state so nothing leaks into the next transaction.
	t.reset()
	t.state = watchTxIdle
	if len(keys) == 0 {
		return nil
	}
	if err := t.tx.Watch(ctx, keys...).Err(); err != nil {
		return err
	}
	snap, err := t.snapshot(ctx, keys)
	if err != nil {
		return err
	}
	t.watched = snap
	return nil
}

// Begin starts the transaction. Commands queued afterwards are committed
// atomically by Commit. Commands queued before Begin (outside the
// transaction) are discarded here so they can never be swept into the
// transaction. Nested Begin calls are rejected with ErrWatchTxStarted.
func (t *WatchTx) Begin(ctx context.Context) error {
	if t.state == watchTxBegun {
		return ErrWatchTxStarted
	}
	if t.state == watchTxDone {
		return ErrWatchTxDone
	}
	// Drop anything queued outside the transaction.
	t.pipe.Discard()
	// Capture the Begin-time view used for in-transaction reads.
	if len(t.watched) > 0 {
		keys := make([]string, 0, len(t.watched))
		for key := range t.watched {
			keys = append(keys, key)
		}
		snap, err := t.snapshot(ctx, keys)
		if err != nil {
			return err
		}
		t.view = snap
	}
	t.state = watchTxBegun
	return nil
}

// Pipe returns the transaction pipeline. Commands queued on it are buffered
// on the same connection as the watch and take effect only when Commit
// succeeds. Its Exec must not be called directly; use Commit.
func (t *WatchTx) Pipe() Pipeliner {
	return t.pipe
}

// Pending returns the number of commands currently queued in the transaction
// pipeline.
func (t *WatchTx) Pending() int {
	return t.pipe.Len()
}

// Get reads key. Inside a transaction (after Begin, before Commit/Discard) a
// watched key is served from the Begin-time view, so concurrent writes by
// others are not observed. Otherwise the read goes to the server.
func (t *WatchTx) Get(ctx context.Context, key string) *StringCmd {
	if t.state == watchTxBegun {
		if snap, ok := t.view[key]; ok {
			cmd := NewStringCmd(ctx, "get", key)
			if snap.exists {
				cmd.SetVal(snap.val)
			} else {
				cmd.SetErr(Nil)
			}
			return cmd
		}
	}
	return t.tx.Get(ctx, key)
}

// Commit atomically applies the queued commands if none of the watched keys
// changed since Watch. On conflict it fails with a *TxConflictError (naming
// the changed keys and the number of discarded commands), none of the queued
// commands take effect, and the watch is released. After Commit — successful
// or not — the transaction is over and the watch is released.
func (t *WatchTx) Commit(ctx context.Context) ([]Cmder, error) {
	switch t.state {
	case watchTxDone:
		return nil, ErrWatchTxDone
	case watchTxIdle:
		return nil, ErrWatchTxNotStarted
	}

	changed, err := t.changedKeys(ctx)
	if err != nil {
		// The connection is broken: the server-side watch died with it, so
		// invalidate the watch and the pipeline client-side as well.
		t.reset()
		t.state = watchTxDone
		return nil, err
	}
	if len(changed) > 0 {
		pending := t.pipe.Len()
		t.abort(ctx)
		return nil, &TxConflictError{Keys: changed, Pending: pending}
	}

	cmds, err := t.pipe.Exec(ctx)
	if err != nil {
		pending := len(cmds)
		if errors.Is(err, TxFailedErr) {
			// A watched key changed between our check and EXEC; the server
			// aborted the transaction, so no queued command took effect.
			// Re-read to name the conflicting keys.
			keys, derr := t.changedKeys(ctx)
			t.reset()
			t.state = watchTxDone
			if derr != nil {
				return nil, err
			}
			return nil, &TxConflictError{Keys: keys, Pending: pending}
		}
		t.reset()
		t.state = watchTxDone
		return nil, err
	}
	t.reset()
	t.state = watchTxDone
	// An empty pipeline never reaches EXEC, so release the server-side
	// watch explicitly rather than holding it until Close.
	if t.tx.watchArmed {
		_ = t.tx.Unwatch(ctx).Err()
	}
	return cmds, nil
}

// Discard aborts the transaction: the watch is released and the pipeline is
// cleared, so no queued command can leak into a later transaction. It is
// valid to Discard after Watch without Begin. Changes made by others after
// Discard do not affect the next transaction.
func (t *WatchTx) Discard(ctx context.Context) error {
	if t.state == watchTxDone {
		return ErrWatchTxDone
	}
	t.abort(ctx)
	return nil
}

// Close releases the connection. Any watch still held is released and any
// queued commands are discarded; nothing is committed implicitly.
func (t *WatchTx) Close(ctx context.Context) error {
	t.reset()
	t.state = watchTxDone
	return t.tx.Close(ctx)
}

// abort releases the watch, clears the pipeline and marks the transaction
// done without executing any queued command.
func (t *WatchTx) abort(ctx context.Context) {
	t.reset()
	if t.tx.watchArmed {
		_ = t.tx.Unwatch(ctx).Err()
	}
	t.state = watchTxDone
}

// reset clears the client-side transaction state: snapshots and every queued
// pipelined command.
func (t *WatchTx) reset() {
	t.pipe.Discard()
	t.watched = nil
	t.view = nil
}

// snapshot reads the current values of keys on the transaction connection.
func (t *WatchTx) snapshot(ctx context.Context, keys []string) (map[string]keySnapshot, error) {
	snap := make(map[string]keySnapshot, len(keys))
	for _, key := range keys {
		val, err := t.tx.Get(ctx, key).Result()
		if err != nil {
			if errors.Is(err, Nil) {
				snap[key] = keySnapshot{}
				continue
			}
			return nil, err
		}
		snap[key] = keySnapshot{val: val, exists: true}
	}
	return snap, nil
}

// changedKeys returns the watched keys whose current value differs from the
// Watch-time baseline. A deleted key counts as changed.
func (t *WatchTx) changedKeys(ctx context.Context) ([]string, error) {
	if len(t.watched) == 0 {
		return nil, nil
	}
	keys := make([]string, 0, len(t.watched))
	for key := range t.watched {
		keys = append(keys, key)
	}
	now, err := t.snapshot(ctx, keys)
	if err != nil {
		return nil, err
	}
	var changed []string
	for _, key := range keys {
		if now[key] != t.watched[key] {
			changed = append(changed, key)
		}
	}
	return changed, nil
}

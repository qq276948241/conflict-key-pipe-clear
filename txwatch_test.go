package redis_test

import (
	"strings"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("WatchTx", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	It("commits queued commands in order when nothing changed", func() {
		wtx := client.NewWatchTx()
		defer wtx.Close(ctx)

		Expect(client.Set(ctx, "k1", "0", 0).Err()).NotTo(HaveOccurred())
		Expect(wtx.Watch(ctx, "k1")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		set1 := wtx.Pipe().Set(ctx, "k1", "1", 0)
		set2 := wtx.Pipe().Set(ctx, "k2", "2", 0)
		get := wtx.Pipe().Get(ctx, "k1")
		Expect(wtx.Pending()).To(Equal(3))

		cmds, err := wtx.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(3))
		Expect(cmds[0]).To(Equal(set1))
		Expect(cmds[1]).To(Equal(set2))
		Expect(cmds[2]).To(Equal(get))
		Expect(get.Val()).To(Equal("1"))

		// Reads after a successful commit see the final written values.
		Expect(client.Get(ctx, "k1").Val()).To(Equal("1"))
		Expect(client.Get(ctx, "k2").Val()).To(Equal("2"))
	})

	It("fails with the changed key names and pending count, applying nothing", func() {
		wtx := client.NewWatchTx()
		defer wtx.Close(ctx)

		Expect(client.Set(ctx, "a", "1", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "b", "1", 0).Err()).NotTo(HaveOccurred())
		Expect(wtx.Watch(ctx, "a", "b")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		wtx.Pipe().Set(ctx, "a", "tx", 0)
		wtx.Pipe().Set(ctx, "b", "tx", 0)

		// Only "b" is modified by someone else.
		Expect(client.Set(ctx, "b", "other", 0).Err()).NotTo(HaveOccurred())

		_, err := wtx.Commit(ctx)
		Expect(err).To(HaveOccurred())
		cerr, ok := redis.IsTxConflictError(err)
		Expect(ok).To(BeTrue())
		Expect(cerr.Keys).To(Equal([]string{"b"}))
		Expect(cerr.Pending).To(Equal(2))
		// Key names and pending count appear in the same failure message.
		Expect(err.Error()).To(ContainSubstring("b"))
		Expect(err.Error()).To(ContainSubstring("2"))

		// Nothing from the pipeline took effect.
		Expect(client.Get(ctx, "a").Val()).To(Equal("1"))
		Expect(client.Get(ctx, "b").Val()).To(Equal("other"))

		// The watch is released after a failed commit.
		Expect(wtx.Pending()).To(Equal(0))
	})

	It("does not report a conflict when nothing changed", func() {
		wtx := client.NewWatchTx()
		defer wtx.Close(ctx)

		Expect(client.Set(ctx, "k", "v", 0).Err()).NotTo(HaveOccurred())
		Expect(wtx.Watch(ctx, "k")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		wtx.Pipe().Set(ctx, "k", "v2", 0)
		_, err := wtx.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
	})

	It("ignores changes to unwatched keys", func() {
		wtx := client.NewWatchTx()
		defer wtx.Close(ctx)

		Expect(client.Set(ctx, "watched", "1", 0).Err()).NotTo(HaveOccurred())
		Expect(wtx.Watch(ctx, "watched")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		wtx.Pipe().Set(ctx, "watched", "2", 0)
		Expect(client.Set(ctx, "unwatched", "x", 0).Err()).NotTo(HaveOccurred())
		_, err := wtx.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "watched").Val()).To(Equal("2"))
	})

	It("treats deletion of a watched key as a change", func() {
		wtx := client.NewWatchTx()
		defer wtx.Close(ctx)

		Expect(client.Set(ctx, "gone", "1", 0).Err()).NotTo(HaveOccurred())
		Expect(wtx.Watch(ctx, "gone")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		wtx.Pipe().Set(ctx, "gone", "2", 0)
		Expect(client.Del(ctx, "gone").Err()).NotTo(HaveOccurred())

		_, err := wtx.Commit(ctx)
		cerr, ok := redis.IsTxConflictError(err)
		Expect(ok).To(BeTrue())
		Expect(cerr.Keys).To(Equal([]string{"gone"}))
	})

	It("clears watch and pipeline on Discard, even before Begin", func() {
		wtx := client.NewWatchTx()
		defer wtx.Close(ctx)

		Expect(client.Set(ctx, "k", "1", 0).Err()).NotTo(HaveOccurred())
		Expect(wtx.Watch(ctx, "k")).NotTo(HaveOccurred())
		wtx.Pipe().Set(ctx, "k", "queued", 0)
		Expect(wtx.Discard(ctx)).NotTo(HaveOccurred())
		Expect(wtx.Pending()).To(Equal(0))

		// Commit and Discard cannot both succeed.
		_, err := wtx.Commit(ctx)
		Expect(err).To(MatchError(redis.ErrWatchTxDone))
		Expect(wtx.Discard(ctx)).To(MatchError(redis.ErrWatchTxDone))

		// Nothing queued leaked into the next transaction, and changes made
		// after Discard do not poison it.
		Expect(client.Set(ctx, "k", "after-discard", 0).Err()).NotTo(HaveOccurred())
		Expect(wtx.Watch(ctx, "k")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		Expect(wtx.Pending()).To(Equal(0))
		wtx.Pipe().Set(ctx, "k", "next", 0)
		_, err = wtx.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "k").Val()).To(Equal("next"))
	})

	It("does not sweep pre-transaction commands into Begin", func() {
		wtx := client.NewWatchTx()
		defer wtx.Close(ctx)

		Expect(wtx.Watch(ctx)).NotTo(HaveOccurred())
		wtx.Pipe().Set(ctx, "outside", "no", 0)
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		Expect(wtx.Pending()).To(Equal(0))
		wtx.Pipe().Set(ctx, "inside", "yes", 0)
		_, err := wtx.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Exists(ctx, "outside").Val()).To(Equal(int64(0)))
		Expect(client.Get(ctx, "inside").Val()).To(Equal("yes"))
	})

	It("rejects nested transactions", func() {
		wtx := client.NewWatchTx()
		defer wtx.Close(ctx)

		Expect(wtx.Watch(ctx, "k")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		wtx.Pipe().Set(ctx, "k", "1", 0)
		Expect(wtx.Begin(ctx)).To(MatchError(redis.ErrWatchTxStarted))
		// The first transaction is still intact and commits alone.
		_, err := wtx.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "k").Val()).To(Equal("1"))
	})

	It("serves in-transaction reads from the Begin-time view", func() {
		wtx := client.NewWatchTx()
		defer wtx.Close(ctx)

		Expect(client.Set(ctx, "k", "before", 0).Err()).NotTo(HaveOccurred())
		Expect(wtx.Watch(ctx, "k")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "k", "concurrent", 0).Err()).NotTo(HaveOccurred())

		// The transaction still sees the value from when it started.
		Expect(wtx.Get(ctx, "k").Val()).To(Equal("before"))

		_, err := wtx.Commit(ctx)
		cerr, ok := redis.IsTxConflictError(err)
		Expect(ok).To(BeTrue())
		Expect(cerr.Keys).To(Equal([]string{"k"}))
	})

	It("does not leak the first transaction's keys or failure into the second", func() {
		wtx := client.NewWatchTx()
		defer wtx.Close(ctx)

		Expect(client.Set(ctx, "first", "1", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "second", "1", 0).Err()).NotTo(HaveOccurred())

		Expect(wtx.Watch(ctx, "first")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		wtx.Pipe().Set(ctx, "first", "tx", 0)
		Expect(client.Set(ctx, "first", "other", 0).Err()).NotTo(HaveOccurred())
		_, err := wtx.Commit(ctx)
		Expect(err).To(HaveOccurred())

		// Second transaction watches only its own keys; the first
		// transaction's keys and conflict are gone.
		Expect(wtx.Watch(ctx, "second")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		wtx.Pipe().Set(ctx, "second", "2", 0)
		_, err = wtx.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "second").Val()).To(Equal("2"))
	})

	It("invalidates watch and pipeline when the connection drops mid-transaction", func() {
		wtx := client.NewWatchTx()

		Expect(client.Set(ctx, "k", "1", 0).Err()).NotTo(HaveOccurred())
		Expect(wtx.Watch(ctx, "k")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		wtx.Pipe().Set(ctx, "k", "tx", 0)

		// Kill the connection underneath the transaction.
		Expect(wtx.Close(ctx)).NotTo(HaveOccurred())
		Expect(wtx.Pending()).To(Equal(0))

		// The next connection must not auto-commit anything.
		Expect(client.Get(ctx, "k").Val()).To(Equal("1"))
	})

	It("still supports plain TxPipelined without watching", func() {
		cmds, err := client.TxPipelined(ctx, func(pipe redis.Pipeliner) error {
			pipe.Set(ctx, "x", "1", 0)
			pipe.Incr(ctx, "x")
			return nil
		})
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(2))
		Expect(client.Get(ctx, "x").Val()).To(Equal("2"))
	})

	It("reports conflict via the error message text", func() {
		wtx := client.NewWatchTx()
		defer wtx.Close(ctx)

		Expect(client.Set(ctx, "named", "1", 0).Err()).NotTo(HaveOccurred())
		Expect(wtx.Watch(ctx, "named")).NotTo(HaveOccurred())
		Expect(wtx.Begin(ctx)).NotTo(HaveOccurred())
		wtx.Pipe().Set(ctx, "named", "2", 0)
		Expect(client.Set(ctx, "named", "other", 0).Err()).NotTo(HaveOccurred())

		_, err := wtx.Commit(ctx)
		Expect(err).To(HaveOccurred())
		Expect(strings.Contains(err.Error(), "named")).To(BeTrue())
	})
})

package redis_test

import (
	"errors"
	"strings"

	. "github.com/bsm/ginkgo/v2"
	. "github.com/bsm/gomega"

	"github.com/redis/go-redis/v9"
)

var _ = Describe("TxGuard", func() {
	var client *redis.Client

	BeforeEach(func() {
		client = redis.NewClient(redisOptions())
		Expect(client.FlushDB(ctx).Err()).NotTo(HaveOccurred())
	})

	AfterEach(func() {
		Expect(client.Close()).NotTo(HaveOccurred())
	})

	conflictErr := func(err error) *redis.TxConflictError {
		var ce *redis.TxConflictError
		Expect(errors.As(err, &ce)).To(BeTrue(), "expected *TxConflictError, got %v", err)
		return ce
	}

	It("commits all buffered commands in order when no key changed", func() {
		g := client.NewTxGuard()
		defer g.Close()
		Expect(g.Watch(ctx, "k1", "k2")).NotTo(HaveOccurred())

		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		pipe.Set(ctx, "k1", "a", 0)
		pipe.Set(ctx, "k2", "b", 0)
		pipe.Get(ctx, "k1")
		pipe.Get(ctx, "k2")

		cmds, err := g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(4))
		Expect(cmds[2].(*redis.StringCmd).Val()).To(Equal("a"))
		Expect(cmds[3].(*redis.StringCmd).Val()).To(Equal("b"))

		Expect(client.Get(ctx, "k1").Val()).To(Equal("a"))
		Expect(client.Get(ctx, "k2").Val()).To(Equal("b"))
	})

	It("works without WATCH like a plain MULTI/EXEC", func() {
		g := client.NewTxGuard()
		defer g.Close()
		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		pipe.Set(ctx, "plain", "v", 0)
		pipe.Incr(ctx, "counter")
		cmds, err := g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(2))
		Expect(cmds[1].(*redis.IntCmd).Val()).To(Equal(int64(1)))
		Expect(client.Get(ctx, "plain").Val()).To(Equal("v"))
	})

	It("does not report a conflict when watched keys are untouched", func() {
		Expect(client.Set(ctx, "k1", "0", 0).Err()).NotTo(HaveOccurred())
		g := client.NewTxGuard()
		defer g.Close()
		Expect(g.Watch(ctx, "k1")).NotTo(HaveOccurred())
		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		pipe.Incr(ctx, "other")
		cmds, err := g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(1))
	})

	It("fails naming the single changed key and applies nothing", func() {
		other := redis.NewClient(redisOptions())
		defer other.Close()
		Expect(client.Set(ctx, "a", "1", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "b", "2", 0).Err()).NotTo(HaveOccurred())

		g := client.NewTxGuard()
		defer g.Close()
		Expect(g.Watch(ctx, "a", "b")).NotTo(HaveOccurred())
		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		pipe.Set(ctx, "a", "x", 0)
		pipe.Set(ctx, "b", "y", 0)
		Expect(other.Set(ctx, "b", "changed", 0).Err()).NotTo(HaveOccurred())

		_, err = g.Commit(ctx)
		ce := conflictErr(err)
		Expect(ce.ChangedKeys).To(Equal([]string{"b"}))
		Expect(ce.Remaining).To(Equal(2))
		Expect(ce.Error()).To(ContainSubstring("b"))
		Expect(ce.Error()).To(ContainSubstring("pipeline remaining: 2"))

		Expect(client.Get(ctx, "a").Val()).To(Equal("1"))
		Expect(client.Get(ctx, "b").Val()).To(Equal("changed"))
	})

	It("treats deletion of a watched key as a change", func() {
		other := redis.NewClient(redisOptions())
		defer other.Close()
		Expect(client.Set(ctx, "k", "v", 0).Err()).NotTo(HaveOccurred())
		g := client.NewTxGuard()
		defer g.Close()
		Expect(g.Watch(ctx, "k")).NotTo(HaveOccurred())
		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		pipe.Set(ctx, "k", "new", 0)
		Expect(other.Del(ctx, "k").Err()).NotTo(HaveOccurred())
		_, err = g.Commit(ctx)
		ce := conflictErr(err)
		Expect(ce.ChangedKeys).To(Equal([]string{"k"}))
		Expect(client.Exists(ctx, "k").Val()).To(Equal(int64(0)))
	})

	It("treats creation of a previously missing watched key as a change", func() {
		other := redis.NewClient(redisOptions())
		defer other.Close()
		g := client.NewTxGuard()
		defer g.Close()
		Expect(g.Watch(ctx, "k")).NotTo(HaveOccurred())
		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		pipe.Set(ctx, "k", "v", 0)
		Expect(other.Set(ctx, "k", "born", 0).Err()).NotTo(HaveOccurred())
		_, err = g.Commit(ctx)
		Expect(conflictErr(err).ChangedKeys).To(Equal([]string{"k"}))
	})

	It("commits when an unwatched key changes", func() {
		other := redis.NewClient(redisOptions())
		defer other.Close()
		g := client.NewTxGuard()
		defer g.Close()
		Expect(g.Watch(ctx, "watched")).NotTo(HaveOccurred())
		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		pipe.Set(ctx, "watched", "yes", 0)
		Expect(other.Set(ctx, "unrelated", "go", 0).Err()).NotTo(HaveOccurred())
		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "watched").Val()).To(Equal("yes"))
	})

	It("releases the watch after a failed commit", func() {
		other := redis.NewClient(redisOptions())
		defer other.Close()
		Expect(client.Set(ctx, "k", "0", 0).Err()).NotTo(HaveOccurred())
		g := client.NewTxGuard()
		Expect(g.Watch(ctx, "k")).NotTo(HaveOccurred())
		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		pipe.Set(ctx, "k", "1", 0)
		Expect(other.Set(ctx, "k", "2", 0).Err()).NotTo(HaveOccurred())
		_, err = g.Commit(ctx)
		Expect(err).To(HaveOccurred())
		Expect(g.Close()).NotTo(HaveOccurred())

		// Mutating k on a reused pooled connection must not hit a leftover WATCH.
		Expect(client.Set(ctx, "k", "3", 0).Err()).NotTo(HaveOccurred())
		g2 := client.NewTxGuard()
		defer g2.Close()
		Expect(g2.Watch(ctx, "other")).NotTo(HaveOccurred())
		p2, err := g2.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		p2.Set(ctx, "other", "fine", 0)
		_, err = g2.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
	})

	It("serves buffered reads from the Begin view", func() {
		other := redis.NewClient(redisOptions())
		defer other.Close()
		Expect(client.Set(ctx, "k", "old", 0).Err()).NotTo(HaveOccurred())
		g := client.NewTxGuard()
		defer g.Close()
		Expect(g.Watch(ctx, "k")).NotTo(HaveOccurred())
		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		get := pipe.Get(ctx, "k")
		Expect(other.Set(ctx, "k", "new", 0).Err()).NotTo(HaveOccurred())
		_, err = g.Commit(ctx)
		Expect(err).To(HaveOccurred()) // key changed -> abort
		// But the read still reflects the Begin view, not the concurrent write.
		Expect(get.Val()).To(Equal("old"))
	})

	It("reads the final committed values afterwards, not stale ones", func() {
		g := client.NewTxGuard()
		defer g.Close()
		Expect(g.Watch(ctx, "k")).NotTo(HaveOccurred())
		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		pipe.Set(ctx, "k", "one", 0)
		pipe.Set(ctx, "k", "two", 0)
		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "k").Val()).To(Equal("two"))
	})

	It("abandons a begun transaction and keeps nothing for later", func() {
		g := client.NewTxGuard()
		defer g.Close()
		Expect(g.Watch(ctx, "k")).NotTo(HaveOccurred())
		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		pipe.Set(ctx, "k", "abandoned", 0)
		Expect(g.Abandon(ctx)).NotTo(HaveOccurred())
		Expect(client.Exists(ctx, "k").Val()).To(Equal(int64(0)))

		// A later change and a fresh guard must not be poisoned by the abort.
		other := redis.NewClient(redisOptions())
		defer other.Close()
		g2 := client.NewTxGuard()
		defer g2.Close()
		Expect(client.Set(ctx, "k", "0", 0).Err()).NotTo(HaveOccurred())
		Expect(g2.Watch(ctx, "k")).NotTo(HaveOccurred())
		p2, err := g2.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		p2.Incr(ctx, "k")
		_, err = g2.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "k").Val()).To(Equal("1"))
	})

	It("abandons watches before Begin", func() {
		Expect(client.Set(ctx, "k", "0", 0).Err()).NotTo(HaveOccurred())
		g := client.NewTxGuard()
		Expect(g.Watch(ctx, "k")).NotTo(HaveOccurred())
		Expect(g.AbandonWatch(ctx)).NotTo(HaveOccurred())
		Expect(g.Close()).NotTo(HaveOccurred())

		// A leftover WATCH would make a later unrelated tx fail.
		g2 := client.NewTxGuard()
		defer g2.Close()
		Expect(client.Incr(ctx, "k").Err()).NotTo(HaveOccurred())
		p, err := g2.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		p.Set(ctx, "fresh", "ok", 0)
		_, err = g2.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "fresh").Val()).To(Equal("ok"))
	})

	It("changes after abandon do not spoil the next transaction", func() {
		other := redis.NewClient(redisOptions())
		defer other.Close()
		g := client.NewTxGuard()
		Expect(g.Watch(ctx, "k")).NotTo(HaveOccurred())
		Expect(g.AbandonWatch(ctx)).NotTo(HaveOccurred())
		Expect(g.Close()).NotTo(HaveOccurred())
		Expect(other.Set(ctx, "k", "changed", 0).Err()).NotTo(HaveOccurred())

		g2 := client.NewTxGuard()
		defer g2.Close()
		Expect(g2.Watch(ctx, "k")).NotTo(HaveOccurred())
		p, err := g2.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		p.Set(ctx, "k", "next", 0)
		_, err = g2.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
	})

	It("refuses commit and abandon without begin", func() {
		g := client.NewTxGuard()
		defer g.Close()
		Expect(g.Watch(ctx, "k")).NotTo(HaveOccurred())
		Expect(g.Commit(ctx)).Error().To(MatchError(redis.ErrTxGuardNotBegan))
		Expect(g.Abandon(ctx)).Error().To(MatchError(redis.ErrTxGuardNotBegan))
	})

	It("refuses a nested begin", func() {
		g := client.NewTxGuard()
		defer g.Close()
		pipe, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		pipe.Set(ctx, "a", "1", 0)
		_, err = g.Begin(ctx)
		Expect(err).To(MatchError(redis.ErrTxGuardAlreadyBegan))
		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred(), "only the first batch is committed")
		Expect(client.Get(ctx, "a").Val()).To(Equal("1"))
	})

	It("does not merge two sequential transactions in one guard", func() {
		g := client.NewTxGuard()
		defer g.Close()
		p, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		p.Set(ctx, "first", "1", 0)
		_, err = g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(g.Watch(ctx, "x")).Error().To(MatchError(redis.ErrTxGuardFinalized))
		_, err = g.Begin(ctx)
		Expect(err).To(MatchError(redis.ErrTxGuardFinalized))
		Expect(g.Abandon(ctx)).Error().To(MatchError(redis.ErrTxGuardFinalized))
	})

	It("does not sweep pre-transaction pipeline commands into the tx", func() {
		g := client.NewTxGuard()
		defer g.Close()
		pre := g.Pipeline()
		pre.Set(ctx, "outside", "now", 0)
		_, err := pre.Exec(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "outside").Val()).To(Equal("now"))

		txp, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		txp.Set(ctx, "inside", "later", 0)
		cmds, err := g.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(cmds).To(HaveLen(1), "the pre-tx command must not be merged")
		Expect(client.Get(ctx, "inside").Val()).To(Equal("later"))
	})

	It("a first failed transaction does not arm the second one", func() {
		other := redis.NewClient(redisOptions())
		defer other.Close()
		Expect(client.Set(ctx, "k", "0", 0).Err()).NotTo(HaveOccurred())

		g1 := client.NewTxGuard()
		Expect(g1.Watch(ctx, "k")).NotTo(HaveOccurred())
		p1, err := g1.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		p1.Set(ctx, "k", "a", 0)
		Expect(other.Set(ctx, "k", "9", 0).Err()).NotTo(HaveOccurred())
		_, err = g1.Commit(ctx)
		Expect(err).To(HaveOccurred())
		Expect(g1.Close()).NotTo(HaveOccurred())

		// The first batch's keys must not watch the second transaction.
		g2 := client.NewTxGuard()
		defer g2.Close()
		p2, err := g2.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		p2.Set(ctx, "x", "y", 0)
		_, err = g2.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(client.Get(ctx, "x").Val()).To(Equal("y"))
	})

	It("voids watch and pipeline when the connection drops mid-transaction", func() {
		opt := redisOptions()
		opt.PoolSize = 1
		c := redis.NewClient(opt)
		defer c.Close()
		Expect(c.Set(ctx, "k", "0", 0).Err()).NotTo(HaveOccurred())

		g := c.NewTxGuard()
		Expect(g.Watch(ctx, "k")).NotTo(HaveOccurred())
		p, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		p.Set(ctx, "k", "1", 0)

		// Kill the guard's server-side connection from a separate client while
		// the tx is open (PoolSize 1 means c itself cannot issue the KILL).
		killer := redis.NewClient(redisOptions())
		defer killer.Close()
		Expect(killer.Do(ctx, "client", "kill", "type", "normal").Err()).NotTo(HaveOccurred())

		_, err = g.Commit(ctx)
		Expect(err).To(HaveOccurred())
		Expect(g.Close()).NotTo(HaveOccurred())

		// Nothing buffered was applied, and the reconnect must not auto-commit.
		Expect(c.Get(ctx, "k").Val()).To(Equal("0"))
		g2 := c.NewTxGuard()
		defer g2.Close()
		p2, err := g2.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		p2.Set(ctx, "after", "ok", 0)
		_, err = g2.Commit(ctx)
		Expect(err).NotTo(HaveOccurred())
		Expect(c.Get(ctx, "after").Val()).To(Equal("ok"))
	})

	It("never auto-commits when closed without commit or abandon", func() {
		g := client.NewTxGuard()
		Expect(g.Watch(ctx, "k")).NotTo(HaveOccurred())
		p, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		p.Set(ctx, "k", "buffered", 0)
		Expect(g.Close()).NotTo(HaveOccurred())
		Expect(client.Exists(ctx, "k").Val()).To(Equal(int64(0)))
	})

	It("reports both changed key names and remaining count together", func() {
		other := redis.NewClient(redisOptions())
		defer other.Close()
		Expect(client.Set(ctx, "a", "1", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "b", "2", 0).Err()).NotTo(HaveOccurred())
		Expect(client.Set(ctx, "c", "3", 0).Err()).NotTo(HaveOccurred())
		g := client.NewTxGuard()
		defer g.Close()
		Expect(g.Watch(ctx, "a", "b", "c")).NotTo(HaveOccurred())
		p, err := g.Begin(ctx)
		Expect(err).NotTo(HaveOccurred())
		p.Set(ctx, "a", "x", 0)
		p.Set(ctx, "c", "z", 0)
		Expect(other.Del(ctx, "a").Err()).NotTo(HaveOccurred())
		Expect(other.Set(ctx, "c", "zz", 0).Err()).NotTo(HaveOccurred())
		_, err = g.Commit(ctx)
		ce := conflictErr(err)
		msg := ce.Error()
		Expect(ce.ChangedKeys).To(ConsistOf("a", "c"))
		Expect(ce.Remaining).To(Equal(2))
		Expect(strings.Contains(msg, "a") && strings.Contains(msg, "c")).To(BeTrue())
		Expect(msg).To(ContainSubstring("pipeline remaining: 2"))
	})
})

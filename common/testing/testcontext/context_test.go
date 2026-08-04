package testcontext

import (
	"context"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
	"go.temporal.io/server/common/debug"
	"google.golang.org/grpc/metadata"
)

func TestWithTimeout(t *testing.T) {
	t.Parallel()

	t.Run("default", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t)
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(DefaultTimeout()), deadline)
			require.Equal(t, 90*time.Second*debug.TimeoutMultiplier, DefaultTimeout())
		})
	})

	t.Run("custom", func(t *testing.T) {
		t.Parallel()

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t, WithTimeout(time.Second))
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(time.Second), deadline)
		})
	})
}

func TestNameMetadata(t *testing.T) {
	t.Parallel()

	ctx := For(t)
	md, ok := metadata.FromOutgoingContext(ctx)
	require.True(t, ok)
	require.Equal(t, []string{t.Name()}, md.Get(testNameMetadataKey))
}

func TestContextDecorators(t *testing.T) {
	t.Parallel()

	t.Run("applied once across calls", func(t *testing.T) {
		t.Parallel()

		type key struct{}

		var calls atomic.Int32
		decorator := func(ctx context.Context) context.Context {
			calls.Add(1)
			return context.WithValue(ctx, key{}, "decorated")
		}

		AttachDecorator(t, key{}, decorator)
		ctx := For(t)
		require.Equal(t, "decorated", ctx.Value(key{}))

		AttachDecorator(t, key{}, decorator)
		ctx = For(t)
		require.Equal(t, "decorated", ctx.Value(key{}))
		require.Equal(t, int32(1), calls.Load(), "decorator should only be applied once")
	})

	t.Run("applied once for same key", func(t *testing.T) {
		t.Parallel()

		type key struct{}

		var calls atomic.Int32
		decorator := func(ctx context.Context) context.Context {
			calls.Add(1)
			return context.WithValue(ctx, key{}, "decorated")
		}

		AttachDecorator(t, key{}, decorator)
		AttachDecorator(t, key{}, decorator)
		ctx := For(t)

		require.Equal(t, "decorated", ctx.Value(key{}))
		require.Equal(t, int32(1), calls.Load(), "decorator should only be applied once")
	})

	t.Run("multiple decorators", func(t *testing.T) {
		t.Parallel()

		type key1 struct{}
		type key2 struct{}

		AttachDecorator(t, key1{}, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key1{}, "one")
		})
		AttachDecorator(t, key2{}, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key2{}, "two")
		})
		ctx := For(t)

		require.Equal(t, "one", ctx.Value(key1{}))
		require.Equal(t, "two", ctx.Value(key2{}))
	})

	t.Run("later call decorates cached context", func(t *testing.T) {
		t.Parallel()

		type key struct{}

		ctx := For(t)
		require.Nil(t, ctx.Value(key{}))

		AttachDecorator(t, key{}, func(ctx context.Context) context.Context {
			return context.WithValue(ctx, key{}, "decorated")
		})
		ctx = For(t)
		require.Equal(t, "decorated", ctx.Value(key{}))
	})
}

func TestCleanupCancelsContext(t *testing.T) {
	t.Parallel()

	var ctx context.Context
	t.Run("subtest", func(t *testing.T) {
		ctx = For(t)
		require.NoError(t, ctx.Err())
	})
	require.ErrorIs(t, ctx.Err(), context.Canceled)
}

// A test whose deadline expires is reported as timed out, unless StopTimeout was
// called first. Asserting on timedOut() rather than the cleanup's Errorf keeps
// this test from failing itself.
func TestStopTimeout(t *testing.T) {
	t.Run("expired deadline is reported", func(t *testing.T) {
		ctx := For(t, WithTimeout(time.Millisecond))
		<-ctx.Done()

		st := stateFor(t, t)
		require.True(t, st.timedOut(), "an expired deadline must be reported as a timeout")

		// Stop it so this test does not fail itself when the cleanup runs.
		StopTimeout(t)
		require.False(t, st.timedOut())
	})

	t.Run("a live context is not a timeout", func(t *testing.T) {
		For(t, WithTimeout(time.Minute))
		require.False(t, stateFor(t, t).timedOut())
	})

	t.Run("no context is a no-op", func(t *testing.T) {
		StopTimeout(t) // must not panic
	})
}

// A parent test is not held to its own deadline while it waits for parallel
// subtests: Go defers the parent's cleanup until they finish, so the parent's
// deadline would otherwise expire measuring its children's runtime. This is the
// shape that failed suites in CI with a spurious "test exceeded timeout".
func TestParentOfParallelSubtestsIsNotReportedAsTimedOut(t *testing.T) {
	t.Run("parent", func(t *testing.T) {
		ctx := For(t, WithTimeout(10*time.Millisecond))
		parentState := stateFor(t, t)

		// Stand in for parallelsuite.Suite.Run's handoff to subtests.
		StopTimeout(t)

		// The child runs after the parent's body returns but before the parent's
		// cleanup, so the parent's state is still live to assert against.
		t.Run("slow parallel child", func(t *testing.T) {
			t.Parallel()
			<-ctx.Done() // outlive the parent's deadline

			require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded,
				"the parent's deadline must really expire for this test to mean anything")
			require.False(t, parentState.timedOut(),
				"a parent waiting on parallel subtests must not be reported as timed out")
		})
	})
}

// stateFor returns the context state registered for key, failing if absent.
func stateFor(t *testing.T, key testing.TB) *contextState {
	t.Helper()
	testContexts.Lock()
	defer testContexts.Unlock()
	st, ok := testContexts.byTest[key]
	require.True(t, ok, "no test context registered")
	return st
}

func TestEnvTimeout(t *testing.T) {
	t.Run("from env", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t)
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(10*time.Second), deadline)
		})
	})

	t.Run("custom overrides env", func(t *testing.T) {
		t.Setenv("TEMPORAL_TEST_TIMEOUT", "10s")

		synctest.Test(t, func(t *testing.T) {
			start := time.Now()
			ctx := For(t, WithTimeout(time.Second))
			deadline, ok := ctx.Deadline()
			require.True(t, ok)
			require.Equal(t, start.Add(time.Second), deadline)
		})
	})
}

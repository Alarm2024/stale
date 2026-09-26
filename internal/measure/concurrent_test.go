package measure

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

// The fake endpoint answers only once the other call is in flight too. Two
// calls made together meet at once and Run finishes in the blink of its
// 20 ms window. Two calls made one after the other can never meet: the first
// waits alone until it gives up, and that is what the test looks for.
func TestCallsRunConcurrently(t *testing.T) {
	// How long a lone call waits for its partner before giving up. Long
	// enough that a scheduler hiccup cannot fake a sequential run, short
	// enough that a failing test does not hang.
	const patience = 2 * time.Second

	var (
		inFlight    atomic.Int32
		met         = make(chan struct{})
		waitedAlone atomic.Bool
	)
	get := func(_ context.Context, _ string) (uint64, error) {
		// Run waits for both answers before it asks again, so the count
		// passes through 2 exactly once: on the first pair.
		if inFlight.Add(1) == 2 {
			close(met)
		}
		select {
		case <-met:
			return 1000, nil
		case <-time.After(patience):
			waitedAlone.Store(true)
			return 0, context.DeadlineExceeded
		}
	}

	cfg := Config{
		TargetURL: "http://target.invalid",
		RefURL:    "http://ref.invalid",
		MaxLag:    5,
		For:       20 * time.Millisecond,
		Sleep:     func(context.Context, time.Duration) error { return nil },
		GetSlot:   get,
	}

	done := make(chan error, 1)
	go func() {
		_, err := Run(context.Background(), cfg)
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(patience + time.Second):
		t.Fatal("Run did not return")
	}
	if waitedAlone.Load() {
		t.Fatal("a getSlot call waited alone until it gave up: target and reference are asked one after the other, not together")
	}
}

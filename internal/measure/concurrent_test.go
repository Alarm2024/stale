package measure

import (
	"context"
	"sync"
	"testing"
	"time"
)

// Each fake endpoint blocks until the other call has also started. With
// sequential calls this deadlocks; with concurrent calls it completes.
func TestCallsRunConcurrently(t *testing.T) {
	var mu sync.Mutex
	started := map[string]bool{}
	bothStarted := make(chan struct{})
	var once sync.Once

	get := func(_ context.Context, ep string) (uint64, error) {
		mu.Lock()
		started[ep] = true
		if len(started) == 2 {
			once.Do(func() { close(bothStarted) })
		}
		mu.Unlock()
		select {
		case <-bothStarted:
			return 1000, nil
		case <-time.After(5 * time.Second):
			return 0, context.DeadlineExceeded
		}
	}

	cfg := Config{
		TargetURL: "http://target.invalid", RefURL: "http://ref.invalid",
		MaxLag: 5, For: 20 * time.Millisecond,
		Sleep:   func(context.Context, time.Duration) error { return nil },
		GetSlot: get,
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
	case <-time.After(6 * time.Second):
		t.Fatal("Run did not finish: the two getSlot calls are not concurrent")
	}
}

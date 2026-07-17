package download

import (
	"context"
	"testing"
	"time"
)

// acquireAsync starts an Acquire in the background and returns a channel that
// closes once the slot is granted.
func acquireAsync(s *fairSem, job string) chan struct{} {
	done := make(chan struct{})
	go func() {
		_ = s.Acquire(context.Background(), job)
		close(done)
	}()
	return done
}

func waitWaiters(t *testing.T, s *fairSem, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		s.mu.Lock()
		got := len(s.waiters)
		s.mu.Unlock()
		if got == n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("waiter count never reached %d", n)
}

// TestFairSemPrefersStarvedJob: with the pool saturated by job A, a freed slot
// must go to job B (0 slots held) even though A's waiter arrived first.
func TestFairSemPrefersStarvedJob(t *testing.T) {
	s := newFairSem(2)
	if err := s.Acquire(context.Background(), "A"); err != nil {
		t.Fatal(err)
	}
	if err := s.Acquire(context.Background(), "A"); err != nil {
		t.Fatal(err)
	}

	a3 := acquireAsync(s, "A")
	waitWaiters(t, s, 1)
	b1 := acquireAsync(s, "B")
	waitWaiters(t, s, 2)

	s.Release("A") // A still holds 1 → B (holding 0) must win despite arriving later
	select {
	case <-b1:
	case <-time.After(2 * time.Second):
		t.Fatal("B never granted a slot")
	}
	select {
	case <-a3:
		t.Fatal("A's third acquire was granted before B")
	case <-time.After(50 * time.Millisecond):
	}

	s.Release("A")
	select {
	case <-a3:
	case <-time.After(2 * time.Second):
		t.Fatal("A's waiter never granted after second release")
	}
}

// TestFairSemAcquireCancel: a cancelled waiter must not leak a slot.
func TestFairSemAcquireCancel(t *testing.T) {
	s := newFairSem(1)
	if err := s.Acquire(context.Background(), "A"); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- s.Acquire(ctx, "B") }()
	waitWaiters(t, s, 1)
	cancel()
	if err := <-errCh; err == nil {
		t.Fatal("cancelled Acquire returned nil")
	}
	s.Release("A")
	// The slot must be reusable after the cancelled waiter left.
	done := acquireAsync(s, "C")
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("slot leaked after cancelled waiter")
	}
}

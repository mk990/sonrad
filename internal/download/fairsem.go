package download

import (
	"context"
	"sync"
)

// fairSem is a counting semaphore that, when slots are contended, grants the
// next free slot to the job holding the fewest slots (FIFO between equals).
// This stops one big season pack from monopolizing every MaxConcurrent slot
// while other jobs wait.
type fairSem struct {
	mu      sync.Mutex
	free    int
	held    map[string]int // job ID -> slots currently held
	waiters []*semWaiter   // in arrival order
}

type semWaiter struct {
	job string
	ch  chan struct{}
}

func newFairSem(n int) *fairSem {
	return &fairSem{free: n, held: map[string]int{}}
}

// Acquire blocks until a slot is granted to job or ctx ends.
func (s *fairSem) Acquire(ctx context.Context, job string) error {
	s.mu.Lock()
	if s.free > 0 {
		s.free--
		s.held[job]++
		s.mu.Unlock()
		return nil
	}
	w := &semWaiter{job: job, ch: make(chan struct{})}
	s.waiters = append(s.waiters, w)
	s.mu.Unlock()

	select {
	case <-w.ch:
		return nil
	case <-ctx.Done():
		s.mu.Lock()
		granted := true
		for i, x := range s.waiters {
			if x == w {
				s.waiters = append(s.waiters[:i], s.waiters[i+1:]...)
				granted = false
				break
			}
		}
		s.mu.Unlock()
		if granted {
			// Release raced with cancellation and already granted us the slot.
			s.Release(job)
		}
		return ctx.Err()
	}
}

// Release returns job's slot, handing it to the waiting job with the fewest
// slots held (ties go to the earliest waiter).
func (s *fairSem) Release(job string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.held[job] > 0 {
		s.held[job]--
		if s.held[job] == 0 {
			delete(s.held, job)
		}
	}
	if len(s.waiters) == 0 {
		s.free++
		return
	}
	best := 0
	for i, w := range s.waiters[1:] {
		if s.held[w.job] < s.held[s.waiters[best].job] {
			best = i + 1
		}
	}
	w := s.waiters[best]
	s.waiters = append(s.waiters[:best], s.waiters[best+1:]...)
	s.held[w.job]++
	close(w.ch)
}

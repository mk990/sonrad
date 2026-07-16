// Package download runs the download queue: it accepts jobs, fetches their
// files from the CDN with concurrency and rate limits, tracks progress for
// the SABnzbd-compatible API, and persists queue/history state across
// restarts.
package download

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"

	"github.com/mk990/sonrad/internal/naming"
	"github.com/mk990/sonrad/internal/upstream"
)

const maxHistory = 500

// Options configures a Manager.
type Options struct {
	MaxConcurrent int   // max concurrent file downloads across all jobs
	RateLimit     int64 // aggregate bytes/sec cap (0 = unlimited)
	Retries       int   // attempts per file before marking it failed
	StateFile     string
}

type Manager struct {
	mu      sync.RWMutex
	queue   []*Job
	history []*Job

	up       *upstream.Client
	opts     Options
	sem      chan struct{} // caps concurrent file transfers across all jobs
	throttle *throttle     // shared aggregate rate limiter (nil = unlimited)
	ctx      context.Context

	dirty  atomic.Bool
	saveMu sync.Mutex     // serializes state-file writes
	wg     sync.WaitGroup // counts in-flight runJob goroutines
}

func NewManager(ctx context.Context, up *upstream.Client, opts Options) *Manager {
	if opts.MaxConcurrent < 1 {
		opts.MaxConcurrent = 1
	}
	if opts.Retries < 1 {
		opts.Retries = 1
	}
	m := &Manager{
		up:   up,
		opts: opts,
		sem:  make(chan struct{}, opts.MaxConcurrent),
		ctx:  ctx,
	}
	if opts.RateLimit > 0 {
		m.throttle = newThrottle(opts.RateLimit)
	}
	return m
}

// Add queues a job and starts its download goroutine.
func (m *Manager) Add(j *Job) {
	j.mu.Lock()
	if j.ctx == nil {
		j.ctx, j.cancel = context.WithCancel(m.ctx)
	}
	j.mu.Unlock()
	m.mu.Lock()
	m.queue = append(m.queue, j)
	m.mu.Unlock()
	m.markDirty()
	m.wg.Add(1)
	go func() {
		defer m.wg.Done()
		m.runJob(j)
	}()
}

// Delete removes a job from the queue or history. It also cancels any
// in-flight download and marks the job deleted so the runJob goroutine can't
// resurrect it into history after we've removed it. When delFiles is true,
// the job's storage folder is removed too.
func (m *Manager) Delete(id string, delFiles bool) bool {
	m.mu.Lock()
	target := removeByID(&m.queue, id)
	if target == nil {
		target = removeByID(&m.history, id)
	}
	if target == nil {
		m.mu.Unlock()
		return false
	}
	target.mu.Lock()
	target.deleted = true
	cancel := target.cancel
	storage := target.StoragePath
	target.mu.Unlock()
	m.markDirty()
	m.mu.Unlock()

	if cancel != nil {
		cancel() // stop the in-flight download so it can't re-add itself
	}
	if delFiles && storage != "" {
		if err := os.RemoveAll(storage); err != nil {
			log.Printf("delete %s: remove %s: %v", id, storage, err)
		}
	}
	return true
}

func removeByID(jobs *[]*Job, id string) *Job {
	for i, j := range *jobs {
		if j.ID == id {
			*jobs = append((*jobs)[:i], (*jobs)[i+1:]...)
			return j
		}
	}
	return nil
}

// Snapshot returns consistent copies of the queue and history for rendering.
func (m *Manager) Snapshot() (queue, history []View) {
	m.mu.RLock()
	q := append([]*Job(nil), m.queue...)
	h := append([]*Job(nil), m.history...)
	m.mu.RUnlock()
	queue = make([]View, 0, len(q))
	for _, j := range q {
		queue = append(queue, j.View())
	}
	history = make([]View, 0, len(h))
	for _, j := range h {
		history = append(history, j.View())
	}
	return queue, history
}

// Wait blocks until all in-flight job goroutines have returned.
func (m *Manager) Wait() { m.wg.Wait() }

func (m *Manager) finalize(j *Job, ok bool, errMsg string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	removeByID(&m.queue, j.ID)
	j.mu.Lock()
	if j.deleted {
		// Deleted mid-flight — don't resurrect it into history (that's what made
		// removed downloads reappear in Sonarr on the next history poll).
		j.mu.Unlock()
		m.markDirty()
		return
	}
	j.Completed = time.Now()
	if ok {
		j.Status = "Completed"
	} else {
		j.Status = "Failed"
		j.FailMessage = errMsg
	}
	j.mu.Unlock()
	m.history = append([]*Job{j}, m.history...)
	if len(m.history) > maxHistory {
		m.history = m.history[:maxHistory]
	}
	m.markDirty()
}

func (m *Manager) runJob(j *Job) {
	j.mu.Lock()
	j.Status = "Queued"
	storage := j.StoragePath
	files := append([]*File(nil), j.Files...)
	jctx := j.ctx
	j.mu.Unlock()
	if jctx == nil {
		jctx = m.ctx
	}

	// The storage directory is created by downloadFile when a file actually
	// starts, not here — a job waiting for a download slot must not touch the
	// disk yet, or every queued job looks like it started downloading at once.

	// Download the job's files concurrently, but gate every file on the shared
	// m.sem so we never exceed MaxConcurrent transfers in total — across all
	// jobs *and* within this one (e.g. a season pack). The slot is acquired
	// here before launching so the loop blocks once the pool is full, then
	// released by the worker goroutine when its file finishes.
	var (
		fileWG   sync.WaitGroup
		failMu   sync.Mutex
		failMsg  string
		failed   bool
		canceled bool
	)
	for _, f := range files {
		select {
		case <-jctx.Done():
			canceled = true
		case m.sem <- struct{}{}:
		}
		if canceled {
			break
		}
		j.mu.Lock()
		j.Status = "Downloading"
		f.Status = "downloading"
		j.mu.Unlock()
		fileWG.Add(1)
		go func(f *File) {
			defer fileWG.Done()
			defer func() { <-m.sem }()
			dest := filepath.Join(storage, naming.Sanitize(f.Filename))
			err := m.downloadWithRetry(jctx, f.URL, dest,
				func(n int64) { j.recordProgress(f, n) },
				func(total int64) { j.setFileSize(f, total) })
			if err != nil {
				j.setFileStatus(f, "failed", err.Error())
				failMu.Lock()
				failed = true
				failMsg = err.Error()
				failMu.Unlock()
				log.Printf("job %s: file %q failed: %v", j.ID, f.Filename, err)
				return
			}
			j.setFileStatus(f, "done", "")
			// Reconcile size if upstream didn't advertise it
			if info, e := os.Stat(dest); e == nil {
				j.mu.Lock()
				j.Bytes += info.Size() - f.Bytes
				f.Bytes = info.Size()
				if f.BytesDone != info.Size() {
					j.BytesDone += info.Size() - f.BytesDone
					f.BytesDone = info.Size()
				}
				j.mu.Unlock()
			}
		}(f)
	}
	fileWG.Wait()
	if canceled {
		m.finalize(j, false, "canceled")
		return
	}
	m.finalize(j, !failed, failMsg)
}

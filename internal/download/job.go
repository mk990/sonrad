package download

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"sync"
	"time"
)

// Job is one queued/completed download (a movie, an episode, or a season
// pack). All fields are guarded by mu; concurrent readers outside this
// package must go through View().
type Job struct {
	mu sync.Mutex

	ID          string
	Name        string
	Category    string
	Status      string
	Bytes       int64
	BytesDone   int64
	Added       time.Time
	Completed   time.Time
	StoragePath string
	FailMessage string
	Files       []*File

	// transient — never persisted
	speedBPS        float64
	lastSampleAt    time.Time
	lastSampleBytes int64
	cancel          context.CancelFunc // cancels this job's download goroutine
	ctx             context.Context    // per-job context (child of the manager ctx)
	deleted         bool               // set by Delete so finalize won't resurrect it into history
}

// File is one media file within a Job.
type File struct {
	URL       string
	Filename  string
	Bytes     int64
	BytesDone int64
	Status    string // pending|downloading|done|failed
	Error     string
}

// FileSpec describes one file of a new job.
type FileSpec struct {
	URL      string
	Filename string
	Size     int64 // estimate; corrected from Content-Length once known
}

// NewJob builds a queued job from file specs.
func NewJob(name, category, storagePath string, files []FileSpec) *Job {
	j := &Job{
		ID:          newID(),
		Name:        name,
		Category:    category,
		Status:      "Queued",
		Added:       time.Now(),
		StoragePath: storagePath,
	}
	for _, fs := range files {
		j.Files = append(j.Files, &File{
			URL:      fs.URL,
			Filename: fs.Filename,
			Bytes:    fs.Size,
			Status:   "pending",
		})
		j.Bytes += fs.Size
	}
	return j
}

// View is a consistent, lock-free copy of a job's public state for rendering.
type View struct {
	ID          string
	Name        string
	Category    string
	Status      string
	Bytes       int64
	BytesDone   int64
	Added       time.Time
	Completed   time.Time
	StoragePath string
	FailMessage string
	SpeedBPS    float64
	Files       []FileView
}

// FileView is the per-file slice of a View.
type FileView struct {
	Filename  string
	Bytes     int64
	BytesDone int64
	Status    string
	Error     string
}

// View snapshots the job under its lock.
func (j *Job) View() View {
	j.mu.Lock()
	defer j.mu.Unlock()
	v := View{
		ID:          j.ID,
		Name:        j.Name,
		Category:    j.Category,
		Status:      j.Status,
		Bytes:       j.Bytes,
		BytesDone:   j.BytesDone,
		Added:       j.Added,
		Completed:   j.Completed,
		StoragePath: j.StoragePath,
		FailMessage: j.FailMessage,
		SpeedBPS:    j.speedBPS,
	}
	for _, f := range j.Files {
		v.Files = append(v.Files, FileView{
			Filename:  f.Filename,
			Bytes:     f.Bytes,
			BytesDone: f.BytesDone,
			Status:    f.Status,
			Error:     f.Error,
		})
	}
	return v
}

// recordProgress is called whenever `n` more bytes have been pulled for `f`.
// Updates the job and file counters and refreshes an EWMA byte-rate that
// drives the SAB queue's speed / timeleft fields.
func (j *Job) recordProgress(f *File, n int64) {
	j.mu.Lock()
	defer j.mu.Unlock()
	if f != nil {
		f.BytesDone += n
	}
	j.BytesDone += n

	now := time.Now()
	if j.lastSampleAt.IsZero() {
		j.lastSampleAt = now
		j.lastSampleBytes = j.BytesDone
		return
	}
	elapsed := now.Sub(j.lastSampleAt).Seconds()
	if elapsed < 0.5 {
		return
	}
	instant := float64(j.BytesDone-j.lastSampleBytes) / elapsed
	const alpha = 0.3
	j.speedBPS = alpha*instant + (1-alpha)*j.speedBPS
	j.lastSampleAt = now
	j.lastSampleBytes = j.BytesDone
}

// setFileSize records the authoritative byte size for `f` (learned from the
// CDN's Content-Length) and keeps the job total in sync, replacing whatever
// estimate the indexer seeded.
func (j *Job) setFileSize(f *File, total int64) {
	if f == nil || total <= 0 {
		return
	}
	j.mu.Lock()
	defer j.mu.Unlock()
	j.Bytes += total - f.Bytes
	f.Bytes = total
}

func (j *Job) setFileStatus(f *File, status, errMsg string) {
	j.mu.Lock()
	defer j.mu.Unlock()
	f.Status = status
	f.Error = errMsg
}

func newID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return "SABnzbd_nzo_" + hex.EncodeToString(b)
}

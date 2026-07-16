package download

import (
	"context"
	"encoding/json"
	"log"
	"os"
	"time"
)

// jobState / fileState are the on-disk JSON layout. Field names match the
// pre-refactor format so existing state files keep loading. We deliberately
// keep it tiny and flat — humans should be able to read and edit it.
type jobState struct {
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
	Files       []fileState
}

type fileState struct {
	URL       string
	Filename  string
	Bytes     int64
	BytesDone int64
	Status    string
	Error     string
}

type savedState struct {
	Queue   []jobState `json:"queue"`
	History []jobState `json:"history"`
}

func (m *Manager) markDirty() { m.dirty.Store(true) }

func jobToState(j *Job) jobState {
	j.mu.Lock()
	defer j.mu.Unlock()
	st := jobState{
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
	}
	for _, f := range j.Files {
		st.Files = append(st.Files, fileState(*f))
	}
	return st
}

func jobFromState(st jobState) *Job {
	j := &Job{
		ID:          st.ID,
		Name:        st.Name,
		Category:    st.Category,
		Status:      st.Status,
		Bytes:       st.Bytes,
		BytesDone:   st.BytesDone,
		Added:       st.Added,
		Completed:   st.Completed,
		StoragePath: st.StoragePath,
		FailMessage: st.FailMessage,
	}
	for _, f := range st.Files {
		file := File(f)
		j.Files = append(j.Files, &file)
	}
	return j
}

// SaveNow writes the current queue/history to the state file (atomically via
// a temp file + rename). saveMu serializes concurrent savers — the periodic
// SaveLoop flush and the final flush at shutdown can otherwise race on the
// temp file.
func (m *Manager) SaveNow() {
	if m.opts.StateFile == "" {
		return
	}
	m.saveMu.Lock()
	defer m.saveMu.Unlock()
	m.mu.RLock()
	q := append([]*Job(nil), m.queue...)
	h := append([]*Job(nil), m.history...)
	m.mu.RUnlock()
	st := savedState{Queue: []jobState{}, History: []jobState{}}
	for _, j := range q {
		st.Queue = append(st.Queue, jobToState(j))
	}
	for _, j := range h {
		st.History = append(st.History, jobToState(j))
	}
	data, err := json.MarshalIndent(st, "", "  ")
	if err != nil {
		log.Printf("state save: marshal: %v", err)
		return
	}
	tmp := m.opts.StateFile + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		log.Printf("state save: write %s: %v", tmp, err)
		return
	}
	if err := os.Rename(tmp, m.opts.StateFile); err != nil {
		log.Printf("state save: rename: %v", err)
	}
}

// SaveLoop persists state every few seconds while dirty; a final flush runs
// when ctx is cancelled.
func (m *Manager) SaveLoop(ctx context.Context) {
	t := time.NewTicker(5 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			m.SaveNow()
			return
		case <-t.C:
			if m.dirty.Swap(false) {
				m.SaveNow()
			}
		}
	}
}

// LoadState reads any persisted state, restores history, and re-queues any
// jobs that were in flight (resume picks up via HTTP Range on next attempt).
func (m *Manager) LoadState() {
	if m.opts.StateFile == "" {
		return
	}
	data, err := os.ReadFile(m.opts.StateFile)
	if err != nil {
		if !os.IsNotExist(err) {
			log.Printf("state load: %v", err)
		}
		return
	}
	var st savedState
	if err := json.Unmarshal(data, &st); err != nil {
		log.Printf("state load: corrupt %s: %v (ignoring)", m.opts.StateFile, err)
		return
	}
	m.mu.Lock()
	for _, js := range st.History {
		m.history = append(m.history, jobFromState(js))
	}
	if len(m.history) > maxHistory {
		m.history = m.history[:maxHistory]
	}
	m.mu.Unlock()
	for _, js := range st.Queue {
		j := jobFromState(js)
		// statuses are re-driven by the worker; speed restarts from 0
		j.Status = "Queued"
		for _, f := range j.Files {
			if f.Status == "downloading" {
				f.Status = "pending"
			}
		}
		m.Add(j)
	}
	log.Printf("state load: %d queued (resuming), %d history", len(st.Queue), len(st.History))
}

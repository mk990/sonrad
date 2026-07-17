package server

import (
	"fmt"
	"net/http"
)

// handleMetrics exposes Prometheus text-format metrics. Stdlib only — the
// format is simple enough to emit by hand.
func (s *Server) handleMetrics(w http.ResponseWriter, r *http.Request) {
	st := s.mgr.Stats()
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	metric := func(name, typ, help string, v float64) {
		fmt.Fprintf(w, "# HELP %s %s\n# TYPE %s %s\n%s %g\n", name, help, name, typ, name, v)
	}
	paused := 0.0
	if st.Paused {
		paused = 1
	}
	var upstreamLast float64
	if t := s.up.LastSuccess(); !t.IsZero() {
		upstreamLast = float64(t.Unix())
	}
	free, total := diskspaceGB(s.cfg.DownloadDir)
	metric("sonrad_queue_jobs", "gauge", "Jobs currently queued or downloading.", float64(st.QueueJobs))
	metric("sonrad_history_jobs", "gauge", "Jobs in history.", float64(st.HistoryJobs))
	metric("sonrad_paused", "gauge", "1 when the download queue is paused.", paused)
	metric("sonrad_download_speed_bytes_per_second", "gauge", "Aggregate current download speed.", st.SpeedBPS)
	metric("sonrad_downloaded_bytes_total", "counter", "Bytes fetched from the CDN since start.", float64(st.BytesFetched))
	metric("sonrad_jobs_completed_total", "counter", "Jobs finished successfully since start.", float64(st.JobsCompleted))
	metric("sonrad_jobs_failed_total", "counter", "Jobs finished with an error since start.", float64(st.JobsFailed))
	metric("sonrad_disk_free_gigabytes", "gauge", "Free space on the download filesystem.", free)
	metric("sonrad_disk_total_gigabytes", "gauge", "Total space on the download filesystem.", total)
	metric("sonrad_upstream_last_success_timestamp_seconds", "gauge", "Unix time of the last successful upstream response (0 = never).", upstreamLast)
}

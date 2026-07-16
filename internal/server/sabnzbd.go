package server

import (
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const sabVersion = "4.1.0"

// handleSABnzbd dispatches the SABnzbd-compatible API (mode=…).
func (s *Server) handleSABnzbd(w http.ResponseWriter, r *http.Request) {
	if !s.authOK(r) {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false, "error": "API Key Incorrect"})
		return
	}
	mode := r.URL.Query().Get("mode")
	if mode == "" {
		mode = r.FormValue("mode")
	}
	switch mode {
	case "version":
		writeJSON(w, 200, map[string]any{"version": sabVersion})
	case "auth":
		writeJSON(w, 200, map[string]any{"auth": "apikey"})
	case "get_config":
		s.sabGetConfig(w)
	case "set_config", "set_config_default":
		writeJSON(w, 200, map[string]any{"status": true})
	case "fullstatus", "status":
		s.sabFullStatus(w)
	case "queue":
		s.sabQueue(w)
	case "history":
		s.sabHistory(w)
	case "addurl", "addid":
		s.sabAddURL(w, r)
	case "addfile", "addlocalfile":
		s.sabAddFile(w, r)
	case "delete":
		s.sabDelete(w, r)
	case "get_cats":
		writeJSON(w, 200, map[string]any{"categories": []string{"*", "movies", "tv"}})
	case "get_scripts":
		writeJSON(w, 200, map[string]any{"scripts": []string{"None"}})
	case "qstatus":
		s.sabQStatus(w)
	case "warnings":
		writeJSON(w, 200, map[string]any{"warnings": []any{}})
	case "server_stats":
		writeJSON(w, 200, map[string]any{"total": 0, "month": 0, "week": 0, "day": 0})
	default: // shutdown, restart, pause, resume, anything unknown
		writeJSON(w, 200, map[string]any{"status": true})
	}
}

func (s *Server) sabGetConfig(w http.ResponseWriter) {
	writeJSON(w, 200, map[string]any{
		"config": map[string]any{
			"misc": map[string]any{
				"complete_dir":          s.cfg.DownloadDir,
				"download_dir":          s.cfg.DownloadDir,
				"complete_dir_writable": true,
				"history_retention":     "",
				"queue_complete":        "",
				"pre_check":             false,
				"enable_meta":           true,
				"sample_match":          false,
			},
			"categories": []map[string]any{
				{"name": "*", "dir": "", "pp": 3, "script": "None", "priority": -100},
				{"name": "movies", "dir": "movies", "pp": 3, "script": "None", "priority": 0},
				{"name": "tv", "dir": "tv", "pp": 3, "script": "None", "priority": 0},
			},
			"sorters":    []any{},
			"servers":    []any{},
			"scheduling": []any{},
		},
	})
}

func (s *Server) sabFullStatus(w http.ResponseWriter) {
	writeJSON(w, 200, map[string]any{
		"status": map[string]any{
			"paused":          false,
			"pause_int":       "0",
			"diskspace1":      "1000.0",
			"diskspace2":      "1000.0",
			"diskspacetotal1": "1000.0",
			"diskspacetotal2": "1000.0",
			"speedlimit":      "0",
			"speedlimit_abs":  "0",
			"have_warnings":   "0",
			"version":         sabVersion,
			"uptime":          "0",
		},
	})
}

func (s *Server) sabQueue(w http.ResponseWriter) {
	queue, _ := s.mgr.Snapshot()
	slots := make([]map[string]any, 0, len(queue))
	var totalBytes, totalDone int64
	var totalSpeed float64
	for i, j := range queue {
		left := max(j.Bytes-j.BytesDone, 0)
		jobETA := "0:00:00"
		if j.SpeedBPS > 0 {
			jobETA = formatHMS(int64(float64(left) / j.SpeedBPS))
		}
		slots = append(slots, map[string]any{
			"status":        j.Status,
			"index":         i,
			"nzo_id":        j.ID,
			"filename":      j.Name,
			"name":          j.Name,
			"cat":           j.Category,
			"mb":            bytesToMB(j.Bytes),
			"mbleft":        bytesToMB(left),
			"mbmissing":     "0.0",
			"size":          bytesString(j.Bytes),
			"sizeleft":      bytesString(left),
			"percentage":    percentage(j.BytesDone, j.Bytes),
			"timeleft":      jobETA,
			"kbpersec":      fmt.Sprintf("%.1f", j.SpeedBPS/1024),
			"mbpersec":      fmt.Sprintf("%.3f", j.SpeedBPS/(1024*1024)),
			"priority":      "Normal",
			"script":        "None",
			"labels":        []string{},
			"missing":       0,
			"direct_unpack": "",
			"avg_age":       "0d",
		})
		totalBytes += j.Bytes
		totalDone += j.BytesDone
		totalSpeed += j.SpeedBPS
	}
	left := max(totalBytes-totalDone, 0)
	timeLeft := "0:00:00"
	if totalSpeed > 0 {
		timeLeft = formatHMS(int64(float64(left) / totalSpeed))
	}
	writeJSON(w, 200, map[string]any{
		"queue": map[string]any{
			"status":          "Downloading",
			"paused":          false,
			"noofslots":       len(slots),
			"noofslots_total": len(slots),
			"limit":           100,
			"start":           0,
			"mb":              bytesToMB(totalBytes),
			"mbleft":          bytesToMB(left),
			"speed":           fmt.Sprintf("%.1f K", totalSpeed/1024),
			"kbpersec":        fmt.Sprintf("%.1f", totalSpeed/1024),
			"timeleft":        timeLeft,
			"slots":           slots,
			"diskspace1":      "1000.0",
			"diskspace2":      "1000.0",
			"diskspacetotal1": "1000.0",
			"diskspacetotal2": "1000.0",
			"version":         sabVersion,
		},
	})
}

func (s *Server) sabHistory(w http.ResponseWriter) {
	_, history := s.mgr.Snapshot()
	slots := make([]map[string]any, 0, len(history))
	for _, j := range history {
		slots = append(slots, map[string]any{
			"nzo_id":        j.ID,
			"name":          j.Name,
			"title":         j.Name,
			"nzb_name":      j.Name + ".nzb",
			"category":      j.Category,
			"status":        j.Status,
			"bytes":         j.Bytes,
			"size":          bytesString(j.Bytes),
			"completed":     j.Completed.Unix(),
			"completeness":  100,
			"fail_message":  j.FailMessage,
			"storage":       j.StoragePath,
			"path":          j.StoragePath,
			"download_time": 0,
			"postproc_time": 0,
			"action_line":   "",
			"pp":            "",
			"script":        "None",
			"report":        "",
			"downloaded":    j.BytesDone,
			"stage_log":     []any{},
		})
	}
	writeJSON(w, 200, map[string]any{
		"history": map[string]any{
			"noofslots":           len(slots),
			"total_size":          "0",
			"month_size":          "0",
			"week_size":           "0",
			"day_size":            "0",
			"slots":               slots,
			"last_history_update": time.Now().Unix(),
			"version":             sabVersion,
		},
	})
}

func (s *Server) sabAddURL(w http.ResponseWriter, r *http.Request) {
	rawURL := firstNonEmpty(r.URL.Query().Get("name"), r.FormValue("name"))
	if rawURL == "" {
		writeJSON(w, 200, map[string]any{"status": false, "error": "no url"})
		return
	}
	body, err := s.up.GetBytes(rawURL)
	if err != nil {
		writeJSON(w, 200, map[string]any{"status": false, "error": err.Error()})
		return
	}
	s.enqueueNZB(w, r, body)
}

func (s *Server) sabAddFile(w http.ResponseWriter, r *http.Request) {
	_ = r.ParseMultipartForm(64 * 1024 * 1024)
	var data []byte
	if r.MultipartForm != nil {
		for _, fhs := range r.MultipartForm.File {
			for _, fh := range fhs {
				f, err := fh.Open()
				if err != nil {
					continue
				}
				data, _ = io.ReadAll(f)
				f.Close()
				if len(data) > 0 {
					break
				}
			}
			if len(data) > 0 {
				break
			}
		}
	}
	if len(data) == 0 {
		data, _ = io.ReadAll(r.Body)
	}
	s.enqueueNZB(w, r, data)
}

// enqueueNZB extracts the sonrad token from an NZB payload and queues the job.
func (s *Server) enqueueNZB(w http.ResponseWriter, r *http.Request, nzb []byte) {
	tok, err := extractToken(nzb)
	if err != nil {
		writeJSON(w, 200, map[string]any{"status": false, "error": "not a sonrad nzb: " + err.Error()})
		return
	}
	if cat := firstNonEmpty(r.URL.Query().Get("cat"), r.FormValue("cat")); cat != "" {
		tok.Category = cat
	}
	j, err := s.startJob(tok)
	if err != nil {
		writeJSON(w, 200, map[string]any{"status": false, "error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]any{"status": true, "nzo_ids": []string{j.ID}})
}

func (s *Server) sabDelete(w http.ResponseWriter, r *http.Request) {
	val := firstNonEmpty(r.URL.Query().Get("value"), r.FormValue("value"))
	delFiles := firstNonEmpty(r.URL.Query().Get("del_files"), r.FormValue("del_files")) == "1"
	for _, id := range strings.Split(val, ",") {
		if id = strings.TrimSpace(id); id != "" {
			s.mgr.Delete(id, delFiles)
		}
	}
	writeJSON(w, 200, map[string]any{"status": true})
}

func (s *Server) sabQStatus(w http.ResponseWriter) {
	queue, _ := s.mgr.Snapshot()
	writeJSON(w, 200, map[string]any{
		"paused":    false,
		"kbpersec":  0.0,
		"mb":        0.0,
		"mbleft":    0.0,
		"noofslots": len(queue),
		"timeleft":  "0:00:00",
	})
}

// formatHMS renders seconds as H:MM:SS for SAB queue/history fields.
func formatHMS(secs int64) string {
	if secs < 0 {
		secs = 0
	}
	if secs > 99*3600 { // SAB caps display at "99:59:59" — bigger values look broken in arr UIs
		secs = 99 * 3600
	}
	return fmt.Sprintf("%d:%02d:%02d", secs/3600, (secs/60)%60, secs%60)
}

func bytesToMB(b int64) string { return fmt.Sprintf("%.2f", float64(b)/(1024*1024)) }

func bytesString(b int64) string {
	if b >= 1024*1024*1024 {
		return fmt.Sprintf("%.2f GB", float64(b)/(1024*1024*1024))
	}
	return fmt.Sprintf("%.2f MB", float64(b)/(1024*1024))
}

func percentage(done, total int64) string {
	if total <= 0 {
		return "0"
	}
	return strconv.Itoa(int(min(done*100/total, 100)))
}

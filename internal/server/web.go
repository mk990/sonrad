package server

import (
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/mk990/sonrad/internal/download"
)

// handleIndex serves the HTML status page at /. Without the API key the page
// is read-only and the key is masked; with ?apikey=KEY it shows the full key
// and unlocks the action buttons (pause/resume, delete, retry).
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	authed := s.authOK(r)
	queue, history := s.mgr.Snapshot()
	stats := s.mgr.Stats()

	key := maskKey(s.cfg.APIKey)
	if authed {
		key = s.cfg.APIKey
		if key == "" {
			key = "(none)"
		}
	}

	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<!doctype html><meta charset=utf-8><meta http-equiv="refresh" content="5"><title>sonrad</title>
<style>body{font-family:-apple-system,system-ui,sans-serif;margin:24px;color:#222}
table{border-collapse:collapse;margin-bottom:24px;width:100%%}
td,th{border:1px solid #e4e4e4;padding:6px 10px;font-size:13px;vertical-align:top}
th{background:#fafafa;text-align:left}
h1{font-size:20px;margin-bottom:4px}h2{font-size:14px;margin-top:24px}
code{background:#f4f4f4;padding:1px 5px;border-radius:3px;font-size:12px}
small{color:#888}
.ok{color:#16a34a}.fail{color:#dc2626}.run{color:#2563eb}.pause{color:#d97706}
.bar{background:#eee;border-radius:3px;height:8px;width:140px;overflow:hidden}
.bar>div{background:#2563eb;height:100%%}
.files{margin:4px 0 0;padding-left:16px;color:#666;font-size:12px}
form.inline{display:inline;margin:0 2px 0 0}
button{font-size:12px;padding:2px 8px;cursor:pointer}</style>
<h1>sonrad %s%s</h1>
<p><small>Newznab: <code>%s/api</code> &nbsp;·&nbsp; SABnzbd: <code>%s/sabnzbd</code> &nbsp;·&nbsp; API key: <code>%s</code> &nbsp;·&nbsp; speed: %s/s</small></p>`,
		s.version, pausedBadge(stats.Paused), s.publicBase(r), s.publicBase(r), htmlEscMin(key), bytesString(int64(stats.SpeedBPS)))

	if authed {
		op := "pause"
		label := "Pause queue"
		if stats.Paused {
			op = "resume"
			label = "Resume queue"
		}
		fmt.Fprintf(w, `<p>%s</p>`, actionForm(s.cfg.APIKey, op, "", label, ""))
	} else if s.cfg.APIKey != "" {
		fmt.Fprint(w, `<p><small>read-only — open <code>/?apikey=YOURKEY</code> to unlock actions</small></p>`)
	}

	fmt.Fprintf(w, `<h2>Queue (%d)</h2><table><tr><th>Name</th><th>Cat</th><th>Status</th><th>Progress</th><th>Speed</th><th>ETA</th>%s</tr>`,
		len(queue), th(authed, "Actions"))
	for _, j := range queue {
		eta := "—"
		if left := j.Bytes - j.BytesDone; j.SpeedBPS > 0 && left > 0 {
			eta = formatHMS(int64(float64(left) / j.SpeedBPS))
		}
		actions := ""
		if authed {
			actions = "<td>" +
				actionForm(s.cfg.APIKey, "delete", j.ID, "Delete", "") +
				actionForm(s.cfg.APIKey, "delete", j.ID, "Delete+files", "1") +
				"</td>"
		}
		fmt.Fprintf(w, `<tr><td>%s%s</td><td>%s</td><td class="run">%s</td><td><div class="bar"><div style="width:%s%%"></div></div><small>%s / %s (%s%%)</small></td><td>%s/s</td><td>%s</td>%s</tr>`,
			htmlEscMin(j.Name), fileList(j), j.Category, j.Status,
			percentage(j.BytesDone, j.Bytes), bytesString(j.BytesDone), bytesString(j.Bytes), percentage(j.BytesDone, j.Bytes),
			bytesString(int64(j.SpeedBPS)), eta, actions)
	}

	fmt.Fprintf(w, `</table><h2>History (%d)</h2><table><tr><th>Name</th><th>Cat</th><th>Status</th><th>Size</th><th>Completed</th><th>Storage</th>%s</tr>`,
		len(history), th(authed, "Actions"))
	for _, j := range history {
		cls := "ok"
		if j.Status != "Completed" {
			cls = "fail"
		}
		failMsg := ""
		if j.FailMessage != "" {
			failMsg = `<br><small>` + htmlEscMin(j.FailMessage) + `</small>`
		}
		actions := ""
		if authed {
			retry := ""
			if j.Status != "Completed" {
				retry = actionForm(s.cfg.APIKey, "retry", j.ID, "Retry", "")
			}
			actions = "<td>" + retry +
				actionForm(s.cfg.APIKey, "delete", j.ID, "Delete", "") +
				"</td>"
		}
		fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td class="%s">%s%s</td><td>%s</td><td>%s</td><td><code>%s</code></td>%s</tr>`,
			htmlEscMin(j.Name), j.Category, cls, j.Status, failMsg,
			bytesString(j.Bytes), j.Completed.Format("2006-01-02 15:04"), htmlEscMin(j.StoragePath), actions)
	}
	w.Write([]byte(`</table>`))
}

// handleUIAction executes a status-page button press (POST only, key required).
func (s *Server) handleUIAction(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "POST only", http.StatusMethodNotAllowed)
		return
	}
	if !s.authOK(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	id := r.FormValue("id")
	switch r.FormValue("op") {
	case "pause":
		s.mgr.Pause()
	case "resume":
		s.mgr.Resume()
	case "delete":
		s.mgr.Delete(id, r.FormValue("del_files") == "1")
	case "retry":
		s.mgr.Retry(id)
	default:
		http.Error(w, "unknown op", http.StatusBadRequest)
		return
	}
	dest := "/"
	if k := r.FormValue("apikey"); k != "" {
		dest = "/?apikey=" + url.QueryEscape(k)
	}
	http.Redirect(w, r, dest, http.StatusSeeOther)
}

// handleHealthz is a tiny liveness/readiness probe for Docker HEALTHCHECK,
// kubernetes probes, and uptime monitors.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	st := s.mgr.Stats()
	upstreamLast := ""
	if t := s.up.LastSuccess(); !t.IsZero() {
		upstreamLast = t.Format(time.RFC3339)
	}
	writeJSON(w, 200, map[string]any{
		"status":                "ok",
		"version":               s.version,
		"queue_length":          st.QueueJobs,
		"history_length":        st.HistoryJobs,
		"paused":                st.Paused,
		"speed_bps":             st.SpeedBPS,
		"upstream_last_success": upstreamLast,
	})
}

// fileList renders a job's per-file status sublist (only for multi-file jobs
// like season packs — a single file would just repeat the job row).
func fileList(j download.View) string {
	if len(j.Files) < 2 {
		return ""
	}
	var b strings.Builder
	b.WriteString(`<ul class="files">`)
	for _, f := range j.Files {
		note := f.Status
		if f.Error != "" {
			note += ": " + f.Error
		}
		fmt.Fprintf(&b, `<li>%s — %s (%s%%)</li>`, htmlEscMin(f.Filename), htmlEscMin(note), percentage(f.BytesDone, f.Bytes))
	}
	b.WriteString(`</ul>`)
	return b.String()
}

func actionForm(apikey, op, id, label, delFiles string) string {
	var b strings.Builder
	b.WriteString(`<form class="inline" method="post" action="/ui/action">`)
	hidden := func(name, val string) {
		if val != "" {
			fmt.Fprintf(&b, `<input type="hidden" name=%q value=%q>`, name, htmlEscMin(val))
		}
	}
	hidden("apikey", apikey)
	hidden("op", op)
	hidden("id", id)
	hidden("del_files", delFiles)
	fmt.Fprintf(&b, `<button>%s</button></form>`, htmlEscMin(label))
	return b.String()
}

func pausedBadge(paused bool) string {
	if paused {
		return ` <span class="pause">⏸ paused</span>`
	}
	return ""
}

func th(show bool, label string) string {
	if !show {
		return ""
	}
	return "<th>" + label + "</th>"
}

// maskKey hides most of the API key so the unauthenticated status page can't
// leak it to anyone who can reach the port.
func maskKey(k string) string {
	if k == "" {
		return "(none)"
	}
	if len(k) <= 6 {
		return "••••"
	}
	return k[:4] + "••••"
}

func htmlEscMin(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	s = strings.ReplaceAll(s, `"`, "&quot;")
	return s
}

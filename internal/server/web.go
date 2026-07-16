package server

import (
	"fmt"
	"net/http"
	"strings"
)

// handleIndex serves the minimal HTML status page at /.
func (s *Server) handleIndex(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	queue, history := s.mgr.Snapshot()
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	fmt.Fprintf(w, `<!doctype html><meta charset=utf-8><title>sonrad</title>
<style>body{font-family:-apple-system,system-ui,sans-serif;margin:24px;color:#222}
table{border-collapse:collapse;margin-bottom:24px;width:100%%}
td,th{border:1px solid #e4e4e4;padding:6px 10px;font-size:13px;vertical-align:top}
th{background:#fafafa;text-align:left}
h1{font-size:20px;margin-bottom:4px}h2{font-size:14px;margin-top:24px}
code{background:#f4f4f4;padding:1px 5px;border-radius:3px;font-size:12px}
small{color:#888}
.ok{color:#16a34a}.fail{color:#dc2626}.run{color:#2563eb}</style>
<h1>sonrad %s</h1>
<p><small>Newznab: <code>%s/api</code> &nbsp;·&nbsp; SABnzbd: <code>%s/sabnzbd</code> &nbsp;·&nbsp; API key: <code>%s</code></small></p>
<h2>Queue (%d)</h2><table><tr><th>Name</th><th>Cat</th><th>Status</th><th>Progress</th></tr>`,
		s.version, s.publicBase(r), s.publicBase(r), s.cfg.APIKey, len(queue))
	for _, j := range queue {
		fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td class="run">%s</td><td>%s / %s (%s%%)</td></tr>`,
			htmlEscMin(j.Name), j.Category, j.Status,
			bytesString(j.BytesDone), bytesString(j.Bytes), percentage(j.BytesDone, j.Bytes))
	}
	fmt.Fprintf(w, `</table><h2>History (%d)</h2><table><tr><th>Name</th><th>Cat</th><th>Status</th><th>Size</th><th>Completed</th><th>Storage</th></tr>`, len(history))
	for _, j := range history {
		cls := "ok"
		if j.Status != "Completed" {
			cls = "fail"
		}
		fmt.Fprintf(w, `<tr><td>%s</td><td>%s</td><td class="%s">%s</td><td>%s</td><td>%s</td><td><code>%s</code></td></tr>`,
			htmlEscMin(j.Name), j.Category, cls, j.Status,
			bytesString(j.Bytes), j.Completed.Format("2006-01-02 15:04"), htmlEscMin(j.StoragePath))
	}
	w.Write([]byte(`</table>`))
}

// handleHealthz is a tiny liveness/readiness probe for Docker HEALTHCHECK,
// kubernetes probes, and uptime monitors.
func (s *Server) handleHealthz(w http.ResponseWriter, r *http.Request) {
	queue, history := s.mgr.Snapshot()
	writeJSON(w, 200, map[string]any{
		"status":         "ok",
		"version":        s.version,
		"queue_length":   len(queue),
		"history_length": len(history),
	})
}

func htmlEscMin(s string) string {
	s = strings.ReplaceAll(s, "&", "&amp;")
	s = strings.ReplaceAll(s, "<", "&lt;")
	s = strings.ReplaceAll(s, ">", "&gt;")
	return s
}

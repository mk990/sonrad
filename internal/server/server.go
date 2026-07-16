// Package server implements sonrad's HTTP surface: a Newznab indexer, a
// SABnzbd-compatible download-client API, a fake-NZB endpoint carrying the
// token between the two, plus a status page and health probe.
package server

import (
	"encoding/json"
	"net/http"

	"github.com/mk990/sonrad/internal/config"
	"github.com/mk990/sonrad/internal/download"
	"github.com/mk990/sonrad/internal/film2"
	"github.com/mk990/sonrad/internal/upstream"
)

type Server struct {
	cfg     *config.Config
	version string
	mgr     *download.Manager
	site    *film2.Client
	up      *upstream.Client
}

func New(cfg *config.Config, version string, mgr *download.Manager, site *film2.Client, up *upstream.Client) *Server {
	return &Server{cfg: cfg, version: version, mgr: mgr, site: site, up: up}
}

// Handler returns the full route table.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/api", s.handleAPI)
	mux.HandleFunc("/api/", s.handleAPI)
	mux.HandleFunc("/getnzb", s.handleGetNZB)
	mux.HandleFunc("/sabnzbd/api", s.handleSABnzbd)
	mux.HandleFunc("/sabnzbd/api/", s.handleSABnzbd)
	mux.HandleFunc("/healthz", s.handleHealthz)
	mux.HandleFunc("/", s.handleIndex)
	return mux
}

func (s *Server) authOK(r *http.Request) bool {
	if s.cfg.APIKey == "" {
		return true
	}
	k := r.URL.Query().Get("apikey")
	if k == "" {
		k = r.URL.Query().Get("api_key")
	}
	if k == "" {
		k = r.Header.Get("X-Api-Key")
	}
	if k == "" {
		// Some SABnzbd clients send the key in the (multipart) form body rather
		// than the URL, notably on addfile POSTs. Parse as a fallback; harmless
		// for query-only GET requests.
		k = r.FormValue("apikey")
		if k == "" {
			k = r.FormValue("api_key")
		}
	}
	return k == s.cfg.APIKey
}

// publicBase returns the scheme+host to use in callback links handed to
// Sonarr/Radarr.
func (s *Server) publicBase(r *http.Request) string {
	scheme := "http"
	if r.TLS != nil {
		scheme = "https"
	}
	if s.cfg.PublicHost != "" {
		return scheme + "://" + s.cfg.PublicHost
	}
	return scheme + "://" + r.Host
}

func respondXML(w http.ResponseWriter, body string) {
	w.Header().Set("Content-Type", "application/xml; charset=utf-8")
	w.Write([]byte(body))
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	enc := json.NewEncoder(w)
	enc.SetEscapeHTML(false)
	_ = enc.Encode(v)
}

func firstNonEmpty(vs ...string) string {
	for _, v := range vs {
		if v != "" {
			return v
		}
	}
	return ""
}

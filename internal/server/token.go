package server

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"encoding/xml"
	"errors"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"

	"github.com/mk990/sonrad/internal/download"
	"github.com/mk990/sonrad/internal/naming"
)

// Token is carried through the indexer → download-client round trip inside a
// fake NZB: the indexer encodes it into each result's download link, and the
// SABnzbd addfile handler decodes it back into a download job.
type Token struct {
	Title    string   `json:"t"`
	Category string   `json:"c"`
	URLs     []string `json:"u"`           // absolute CDN file URL(s) to download
	Sizes    []int64  `json:"s,omitempty"` // per-URL size estimate (bytes); seeds the queue's total so progress isn't 0/NaN before the first byte
	Names    []string `json:"n,omitempty"` // per-URL on-disk filename (Sonarr/Radarr-parseable release name + real ext); without this a season pack's files keep their raw CDN names and Sonarr can't map them to episodes
}

func encodeToken(t Token) string {
	b, _ := json.Marshal(t)
	return base64.RawURLEncoding.EncodeToString(b)
}

func decodeToken(s string) (Token, error) {
	b, err := base64.RawURLEncoding.DecodeString(strings.TrimSpace(s))
	if err != nil {
		return Token{}, err
	}
	var t Token
	err = json.Unmarshal(b, &t)
	return t, err
}

// handleGetNZB returns a fake-NZB file carrying our token.
func (s *Server) handleGetNZB(w http.ResponseWriter, r *http.Request) {
	if !s.authOK(r) {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	tokStr := r.URL.Query().Get("token")
	if tokStr == "" {
		http.Error(w, "missing token", http.StatusBadRequest)
		return
	}
	tok, err := decodeToken(tokStr)
	if err != nil {
		http.Error(w, "bad token", http.StatusBadRequest)
		return
	}
	w.Header().Set("Content-Type", "application/x-nzb")
	w.Header().Set("Content-Disposition", `attachment; filename="`+naming.Sanitize(tok.Title)+`.nzb"`)
	w.Write([]byte(buildFakeNZB(tok)))
}

func buildFakeNZB(t Token) string {
	var b strings.Builder
	b.WriteString(`<?xml version="1.0" encoding="UTF-8"?>` + "\n")
	b.WriteString(`<!DOCTYPE nzb PUBLIC "-//newzBin//DTD NZB 1.1//EN" "http://www.newzbin.com/DTD/nzb/nzb-1.1.dtd">` + "\n")
	b.WriteString(`<nzb xmlns="http://www.newzbin.com/DTD/2003/nzb">` + "\n")
	b.WriteString(`  <head>` + "\n")
	b.WriteString(`    <meta type="title">` + xmlEsc(t.Title) + `</meta>` + "\n")
	b.WriteString(`    <meta type="category">` + xmlEsc(t.Category) + `</meta>` + "\n")
	b.WriteString(`    <meta type="sonrad-token">` + xmlEsc(encodeToken(t)) + `</meta>` + "\n")
	b.WriteString(`  </head>` + "\n")
	b.WriteString(`  <file poster="sonrad@local" date="0" subject="` + xmlEsc(t.Title) + `">` + "\n")
	b.WriteString(`    <groups><group>alt.binaries.sonrad</group></groups>` + "\n")
	b.WriteString(`    <segments><segment bytes="1" number="1">sonrad@local</segment></segments>` + "\n")
	b.WriteString(`  </file>` + "\n")
	b.WriteString(`</nzb>` + "\n")
	return b.String()
}

var reNZBToken = regexp.MustCompile(`(?is)<meta\s+type="sonrad-token">([^<]+)</meta>`)

// extractToken pulls the sonrad token out of an uploaded NZB (or a raw token
// pasted as the whole body).
func extractToken(b []byte) (Token, error) {
	if m := reNZBToken.FindSubmatch(b); len(m) > 1 {
		return decodeToken(strings.TrimSpace(string(m[1])))
	}
	if t, err := decodeToken(string(bytes.TrimSpace(b))); err == nil && len(t.URLs) > 0 {
		return t, nil
	}
	return Token{}, errors.New("no sonrad token found")
}

// startJob turns a decoded token into a queued download job.
func (s *Server) startJob(t Token) (download.View, error) {
	if len(t.URLs) == 0 {
		return download.View{}, errors.New("token has no urls")
	}
	cat := t.Category
	if cat == "" {
		cat = "*"
	}
	sub := ""
	switch cat {
	case "movies":
		sub = "movies"
	case "tv":
		sub = "tv"
	}
	storage := filepath.Join(s.cfg.DownloadDir, sub, naming.Sanitize(t.Title))
	var files []download.FileSpec
	for i, u := range t.URLs {
		var sz int64
		if i < len(t.Sizes) {
			sz = t.Sizes[i]
		}
		name := naming.URLBaseName(u)
		if i < len(t.Names) && t.Names[i] != "" {
			name = t.Names[i]
		}
		files = append(files, download.FileSpec{URL: u, Filename: name, Size: sz})
	}
	j := download.NewJob(t.Title, cat, storage, files)
	s.mgr.Add(j)
	return j.View(), nil
}

func xmlEsc(s string) string {
	var buf bytes.Buffer
	xml.EscapeText(&buf, []byte(s))
	return buf.String()
}

// Package naming holds small filename helpers shared by the scraper, the
// indexer and the download manager.
package naming

import (
	"net/url"
	"path/filepath"
	"regexp"
	"strings"
)

var reUnsafe = regexp.MustCompile(`[\\/:*?"<>|]+`)

// Sanitize makes a string safe to use as a file or directory name.
func Sanitize(s string) string {
	s = reUnsafe.ReplaceAllString(s, "_")
	s = strings.TrimSpace(s)
	if s == "" {
		s = "untitled"
	}
	return s
}

// URLBaseName returns the decoded basename of a download URL, dropping any
// query string or fragment.
func URLBaseName(u string) string {
	name := u
	if i := strings.IndexAny(name, "?#"); i >= 0 {
		name = name[:i]
	}
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	if dec, err := url.PathUnescape(name); err == nil && dec != "" {
		name = dec
	}
	return name
}

// ReleaseFileName builds the on-disk filename for a downloaded file: the
// Sonarr/Radarr-parseable release name (e.g. Show.S02E01.720p.…FILM2MZ) plus
// the real extension carried by the upstream URL. This is what lets Sonarr map
// each file in a season-pack folder back to its episode.
func ReleaseFileName(release, u string) string {
	ext := filepath.Ext(URLBaseName(u))
	if ext == "" || len(ext) > 5 {
		ext = ".mkv"
	}
	return release + ext
}

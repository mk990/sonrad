// Package config holds the runtime configuration shared by sonrad's
// components. It is populated from command-line flags in cmd/sonrad.
package config

import "time"

type Config struct {
	Addr        string // HTTP listen address
	DownloadDir string // absolute path finished files end up in
	APIKey      string // key Sonarr/Radarr must present (empty = no auth)

	MaxConcurrent   int   // max concurrent file downloads
	RateLimit       int64 // aggregate bytes/sec cap (0 = unlimited)
	DownloadRetries int   // attempts per file before marking it failed

	UserAgent string // HTTP User-Agent for upstream requests
	Cookies   string // raw Cookie header for upstream requests
	BaseURL   string // main site base URL

	CacheTTL          time.Duration // indexer scrape cache TTL
	SearchConcurrency int           // parallel upstream fetches per indexer search
	NoDubbed          bool          // exclude Dubbed audio variants from indexer results

	PublicHost         string // host[:port] used in indexer callback links
	StateFile          string // JSON state file for queue/history persistence
	InsecureSkipVerify bool   // skip TLS verification on upstream requests
	ShutdownTimeout    time.Duration
	Debug              bool
}

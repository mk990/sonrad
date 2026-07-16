// sonrad — Sonarr/Radarr bridge for film2mz.top
//
// Acts as BOTH a Newznab/Torznab indexer and a SABnzbd-compatible download
// client. Pure Go stdlib — no external dependencies.
//
//	go run ./cmd/sonrad -addr :8910 -download-dir /downloads -api-key MYKEY
//
// Sonarr / Radarr config:
//
//	Indexer  → Newznab
//	  URL:     http://HOST:8910
//	  API key: MYKEY
//	Download client → SABnzbd
//	  Host:     HOST
//	  Port:     8910
//	  URL base: /sabnzbd
//	  API key:  MYKEY
//	  Category: movies (Radarr) / tv (Sonarr)
//
// Search flow:
//
//	Sonarr → /api?t=tvsearch&imdbid=tt..&season=1&ep=2  → RSS with results
//	Sonarr → /getnzb?token=…                            → fake-NZB carrying token
//	Sonarr → /sabnzbd/api?mode=addfile                  → enqueues job
//	Worker → fetches files into <download-dir>/<cat>/<name>/
//	Sonarr polls /sabnzbd/api?mode=history → sees Completed → imports.
package main

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/mk990/sonrad/internal/config"
	"github.com/mk990/sonrad/internal/download"
	"github.com/mk990/sonrad/internal/film2"
	"github.com/mk990/sonrad/internal/release"
	"github.com/mk990/sonrad/internal/server"
	"github.com/mk990/sonrad/internal/upstream"
)

var version = "dev" // overridden at build time via -ldflags "-X main.version=..."

var (
	flagAddr            = flag.String("addr", ":8910", "HTTP listen address")
	flagDownloadDir     = flag.String("download-dir", "./downloads", "directory finished files end up in")
	flagAPIKey          = flag.String("api-key", "", "API key Sonarr/Radarr must present (auto-generated if empty)")
	flagMaxConc         = flag.Int("max-concurrent", 3, "max concurrent file downloads")
	flagRateLimit       = flag.Int64("rate-limit", 0, "aggregate bytes/sec cap (0 = unlimited)")
	flagUA              = flag.String("user-agent", "Mozilla/5.0 (X11; Linux x86_64) sonrad/"+version, "HTTP User-Agent for upstream")
	flagCookies         = flag.String("cookies", "", "raw Cookie header for upstream requests")
	flagBase            = flag.String("base-url", "https://www.film2mz.top", "main site base URL (env: SONRAD_BASE_URL)")
	flagCacheTTL        = flag.Duration("cache-ttl", 10*time.Minute, "indexer scrape cache TTL")
	flagPubHost         = flag.String("public-host", "", "host[:port] used in indexer callback links (default: from request Host header)")
	flagDebug           = flag.Bool("debug", false, "verbose logging")
	flagTest            = flag.String("test", "", "search this title on the site, print results, exit")
	flagStateFile       = flag.String("state-file", "", "path to JSON state file for queue/history persistence (default: <download-dir>/sonrad.state.json)")
	flagInsecure        = flag.Bool("insecure-skip-verify", false, "skip TLS verification on upstream requests (for mirrors with bad certs)")
	flagShutdownTimeout = flag.Duration("shutdown-timeout", 30*time.Second, "how long to wait for in-flight requests during shutdown")
	flagRetries         = flag.Int("download-retries", 3, "attempts per file before marking it failed (1 = no retry)")
	flagSearchConc      = flag.Int("search-concurrency", 4, "parallel upstream fetches per indexer search")
	flagNoDubbed        = flag.Bool("no-dubbed", false, "exclude Dubbed audio variants from indexer results")
)

func main() {
	flag.Parse()
	cfg := configFromFlags()

	up := upstream.New(cfg.UserAgent, cfg.Cookies, cfg.InsecureSkipVerify)
	if cfg.InsecureSkipVerify {
		log.Printf("WARNING: TLS verification disabled for upstream requests")
	}
	site := film2.New(up, cfg.BaseURL, cfg.CacheTTL)

	if *flagTest != "" {
		testScrape(site, *flagTest)
		return
	}

	if cfg.APIKey == "" {
		cfg.APIKey = randomKey()
		log.Printf("no -api-key supplied; generated ephemeral key: %s", cfg.APIKey)
	}
	if err := os.MkdirAll(cfg.DownloadDir, 0o755); err != nil {
		log.Fatalf("download dir: %v", err)
	}
	if abs, err := filepath.Abs(cfg.DownloadDir); err == nil {
		cfg.DownloadDir = abs
	}
	if cfg.StateFile == "" {
		cfg.StateFile = filepath.Join(cfg.DownloadDir, "sonrad.state.json")
	}

	// Root context cancelled on SIGINT/SIGTERM — workers honor it.
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	mgr := download.NewManager(ctx, up, download.Options{
		MaxConcurrent: cfg.MaxConcurrent,
		RateLimit:     cfg.RateLimit,
		Retries:       cfg.DownloadRetries,
		StateFile:     cfg.StateFile,
	})
	mgr.LoadState()
	go mgr.SaveLoop(ctx)

	srv := &http.Server{
		Addr:              cfg.Addr,
		Handler:           server.New(cfg, version, mgr, site, up).Handler(),
		ReadHeaderTimeout: 30 * time.Second,
	}

	log.Printf("sonrad %s listening on %s", version, cfg.Addr)
	log.Printf("download dir: %s", cfg.DownloadDir)
	log.Printf("state file:   %s", cfg.StateFile)
	log.Printf("api key:      %s", cfg.APIKey)
	log.Printf("concurrency:  %d files, %d search, rate-limit %d B/s, retries %d",
		cfg.MaxConcurrent, cfg.SearchConcurrency, cfg.RateLimit, cfg.DownloadRetries)
	log.Printf("Newznab indexer URL: http://<host>%s/api  (apikey: %s)", cfg.Addr, cfg.APIKey)
	log.Printf("SABnzbd base URL   : http://<host>%s/sabnzbd  (apikey: %s)", cfg.Addr, cfg.APIKey)

	// Run the server in a goroutine so we can react to ctx cancellation.
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
		}
	}()

	select {
	case <-ctx.Done():
		log.Printf("shutdown signal received, draining (timeout %s)…", cfg.ShutdownTimeout)
	case err := <-errCh:
		log.Printf("server error: %v", err)
	}

	// 1. stop accepting new HTTP requests, drain in-flight ones
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("http shutdown: %v", err)
	}

	// 2. wait for download workers (ctx is already cancelled, they're aborting)
	done := make(chan struct{})
	go func() {
		mgr.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-shutdownCtx.Done():
		log.Printf("workers didn't drain in time, forcing exit")
	}

	// 3. final state flush so a restart picks up where we left off
	mgr.SaveNow()
	log.Printf("bye")
}

func configFromFlags() *config.Config {
	return &config.Config{
		Addr:               *flagAddr,
		DownloadDir:        *flagDownloadDir,
		APIKey:             *flagAPIKey,
		MaxConcurrent:      *flagMaxConc,
		RateLimit:          *flagRateLimit,
		DownloadRetries:    *flagRetries,
		UserAgent:          *flagUA,
		Cookies:            *flagCookies,
		BaseURL:            *flagBase,
		CacheTTL:           *flagCacheTTL,
		SearchConcurrency:  *flagSearchConc,
		NoDubbed:           *flagNoDubbed,
		PublicHost:         *flagPubHost,
		StateFile:          *flagStateFile,
		InsecureSkipVerify: *flagInsecure,
		ShutdownTimeout:    *flagShutdownTimeout,
		Debug:              *flagDebug,
	}
}

func randomKey() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// testScrape runs a free-text search against film2mz and, for each hit,
// scrapes its page and prints the download links + parsed release metadata.
func testScrape(site *film2.Client, query string) {
	hits, err := site.Search(release.CleanQuery(query))
	if err != nil {
		log.Fatalf("search: %v", err)
	}
	fmt.Printf("Query: %q → %d hit(s)\n\n", query, len(hits))
	for _, h := range hits {
		kind := "movie"
		if h.IsTV {
			kind = "tv"
		}
		fmt.Printf("%s [%s] (%s, %d)\n  %s\n", h.Title, kind, h.IMDB, h.Year, h.URL)
		files, err := site.PageFiles(h.URL)
		if err != nil {
			fmt.Printf("    error: %v\n", err)
			continue
		}
		for _, f := range files {
			inf := release.Parse(f.Name)
			fmt.Printf("    - %s (S%02dE%02d, q=%s codec=%s src=%s audio=%s)\n",
				f.Name, f.Season, f.Episode, inf.Quality, inf.Codec, inf.Source, inf.Audio)
			fmt.Printf("        %s\n", f.URL)
		}
		fmt.Println()
	}
}

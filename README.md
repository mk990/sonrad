# sonrad

A Go bridge that lets **Sonarr** and **Radarr** download from
`film2mz.top` direct links.

It speaks two protocols at once:

- **Newznab indexer** — Sonarr/Radarr search film2mz by title, see every
  available quality / codec / audio variant as a separate result, and let their
  own quality profiles pick.
- **SABnzbd download client** — when Sonarr/Radarr send a release back, sonrad
  downloads the file(s) into the right folder, and reports
  status back so the import works automatically.

No external dependencies (Go stdlib only).

```
cmd/sonrad/         entry point: flags, wiring, lifecycle
internal/config/    shared runtime configuration
internal/upstream/  HTTP client for the site + CDN (UA/cookies/TLS)
internal/film2/     film2mz scraper: search + page-link extraction
internal/release/   release-name parsing, formatting, categories, sizes
internal/download/  job queue, downloader (resume/retry/throttle), state
internal/server/    Newznab indexer, SABnzbd API, NZB token, status page
```

---

## Quick start

### From source

```sh
go build -o sonrad ./cmd/sonrad

./sonrad \
  -addr :8910 \
  -download-dir /downloads \
  -api-key MYKEY
```

### Docker

```sh
docker build -t sonrad .

docker run -d --name sonrad \
  -p 8910:8910 \
  -e SONRAD_API_KEY=MYKEY \
  -v /your/host/downloads:/downloads \
  sonrad
```

Then open `http://HOST:8910/` for a live queue/history view.

---

## Sonarr / Radarr setup

### 1. Add as an indexer

`Settings → Indexers → Add → Newznab (custom)`

| field   | value                          |
|---------|--------------------------------|
| Name    | sonrad                         |
| URL     | `http://HOST:8910`             |
| API Key | `MYKEY` (same value you used)  |
| Categories | Movies → `2000`, TV → `5000` |

Test should succeed.

### 2. Add as a download client

`Settings → Download Clients → Add → SABnzbd`

| field        | value         |
|--------------|---------------|
| Host         | `HOST`        |
| Port         | `8910`        |
| URL Base     | `/sabnzbd`    |
| API Key      | `MYKEY`       |
| Username     | *(empty)*     |
| Password     | *(empty)*     |
| Category     | `tv` for Sonarr, `movies` for Radarr |
| Use SSL      | off           |

Test should succeed.

### 3. Path mapping (if needed)

Files land at `<download-dir>/<cat>/<sanitized-title>/...` inside sonrad. If
Sonarr/Radarr sees that volume at a different path, add a remote path mapping
in their Download Client settings so they can find the completed files.

---

## How it works

```
                        title query
                           │
   Sonarr/Radarr           ▼                       film2mz.top
        │      ┌──────────────────────┐  POST           │
        │  ◀── │   /api (Newznab)     │ ─────────────▶  │ /quick-search (JSON)
        │      │  scrapes + emits RSS │   parses         post/series page →
        │      └──────────────────────┘                  direct CDN links
        │                                                 │
        │  picks a result, fetches the .nzb               │
        │      ┌──────────────────────┐                   │
        │  ◀── │  /getnzb?token=…     │                   │
        │      │ fake NZB w/ token    │                   │
        │      └──────────────────────┘                   │
        │                                                 │
        │  POST the .nzb to download client               │
        │      ┌──────────────────────┐                   │
        ├────▶ │ /sabnzbd/api         │                   │
        │      │ addfile → queue job  │                   │
        │      └──────────────────────┘                   │
        │                  │                              │
        │                  ▼                              │
        │      ┌──────────────────────┐  GET CDN file(s)  │
        │      │  worker downloads    │ ────────────────▶ │
        │      │  → /downloads/cat/…  │                   │
        │      └──────────────────────┘                   │
        │                  │
        │  polls /sabnzbd/api?mode=history
        │  sees Completed → imports from `storage` path
        ▼
   library
```

Sonarr/Radarr never know they aren't talking to a real Newznab indexer and a
real SABnzbd. The "NZB" is a tiny XML doc that carries a base64-encoded JSON
token describing what to fetch.

---

## Flags

| flag                | env var                  | default                              | meaning |
|---------------------|--------------------------|--------------------------------------|---------|
| `-addr`             | `SONRAD_ADDR`            | `:8910`                              | HTTP listen address |
| `-download-dir`     | `SONRAD_DOWNLOAD_DIR`    | `./downloads`                        | where completed files end up |
| `-api-key`          | `SONRAD_API_KEY`         | *(auto-generated, logged)*           | required by Sonarr/Radarr |
| `-max-concurrent`   | `SONRAD_MAX_CONCURRENT`  | `3`                                  | parallel file downloads |
| `-rate-limit`       | `SONRAD_RATE_LIMIT`      | `0` (unlimited)                      | aggregate bytes/sec cap |
| `-user-agent`       | `SONRAD_USER_AGENT`      | `Mozilla/5.0 … sonrad/1.0`           | UA for upstream requests |
| `-cookies`          | `SONRAD_COOKIES`         | *(empty)*                            | raw `Cookie:` header for upstream |
| `-base-url`         | `SONRAD_BASE_URL`        | `https://www.film2mz.top`            | main site base URL |
| `-cache-ttl`        | `SONRAD_CACHE_TTL`       | `10m`                                | indexer scrape cache TTL |
| `-public-host`      | `SONRAD_PUBLIC_HOST`     | *(derived from request)*             | host:port used in callback links — set this behind a reverse proxy |
| `-debug`            | —                        | off                                  | verbose logging |
| `-test "title"`     | —                        | —                                    | search that title on the site, print results, exit |

Env vars are only honored by the Docker entrypoint; in the binary they map 1:1
to the flags above.

---

## Testing the scraper directly

Before touching Sonarr/Radarr, confirm parsing works on the show you care
about:

```sh
./sonrad -test tt10795658
```

You should see the title, every directory variant, and every file with its
season/episode and size. Example:

```
Title: Alice in Borderland
Directories: 16

  https://…/SoftSub/S01/1080p.x265.10bit.Web-DL/
    season=1 quality=1080p codec=x265 source=Web-DL audio=SoftSub
    - Alice.in.Borderland.S01E01.…x265.HEVC.PSA.SoftSub.…mkv  (S01E01, 748.60 MB)
    …
```

If sizes show `0.00 MB`, that directory's HTML probably uses a different
layout — open an issue or tweak `reSizeCell` / `reFile` regexes.

---

## What gets emitted as indexer results

For a given title query, sonrad emits **one result per file plus one season
pack per (season, quality/codec/audio/source)** (for TV). Each result includes:

- a release-style name: `Show.Name.2020.S01E05.720p.Web-DL.x264.FILM2MZ`
- the right Newznab category (`5040` for HD TV, `2040` for HD movies, etc.)
- an estimated size (film2mz doesn't list per-file sizes; derived from quality/codec)
- the IMDB id (resolved from film2mz's search results)

Sonarr/Radarr's quality profiles then choose which one to grab — no need for
a `-quality` flag.

---

## File layout

```
<download-dir>/
├── movies/
│   └── Some.Movie.2005.1080p.Web-DL.x264.FILM2MZ/
│       └── Some.Movie.original.filename.mkv
└── tv/
    └── Alice.in.Borderland.2020.S01E05.720p.Web-DL.x264.FILM2MZ/
        └── Alice.in.Borderland.S01E05.720p.…mkv
```

This matches SAB's standard `complete_dir/category/jobname/files` layout,
which is what Sonarr/Radarr expect to import from.

---

## Limitations

- **Title search only** — film2mz has no IMDB→page endpoint, so sonrad resolves
  results via its `/quick-search` (which returns the IMDB id per hit). Caps
  advertise `q` only, so Sonarr/Radarr send the title; queries with an IMDB id
  but no title return empty.
- **HTML scraping** — if film2mz changes its `/quick-search` JSON or its page
  markup, the search decoder (`film2Result`) and the link regex (`reMediaURL`)
  are the only things to update.
- **No subtitle sidecar download** — only video files are fetched.

---

## Operations

- **Status page** — `GET /` auto-refreshes every 5 s with per-job progress,
  speed and ETA. Without the API key it is read-only and the key is masked;
  open `/?apikey=YOURKEY` to unlock pause/resume, delete and retry buttons.
- **Pause / resume** — SABnzbd `mode=pause` / `mode=resume` (Sonarr/Radarr's
  pause button) really pause the queue: no new file starts and in-flight
  transfers stop pulling bytes.
- **Retry** — `mode=retry&value=<nzo_id>` (or the Retry button) re-queues a
  failed job from history; already-finished files resume via HTTP `Range`.
- **Disk space** — the SABnzbd API reports real free/total space of the
  download filesystem, so the arrs' free-space checks work.
- **Truncation guard** — a transfer that ends short of the advertised size is
  retried (resuming), never imported as complete.
- **Monitoring** — `GET /healthz` (queue length, speed, paused, last successful
  upstream contact) and `GET /metrics` (Prometheus text format).
- **Logging** — structured `log/slog`; `-debug` enables scrape-level detail.

---

## Endpoints (reference)

| path                  | who calls it           | purpose |
|-----------------------|------------------------|---------|
| `GET /api?t=caps`     | Sonarr/Radarr          | indexer capabilities |
| `GET /api?t=movie&q=…`          | Radarr      | movie search |
| `GET /api?t=tvsearch&q=…&season=…&ep=…` | Sonarr | TV search |
| `GET /getnzb?token=…&apikey=…`  | Sonarr/Radarr | fetch fake NZB |
| `/sabnzbd/api?mode=…` | Sonarr/Radarr          | SABnzbd-compatible client API |
| `GET /`               | you                    | live status page (actions with `?apikey=…`) |
| `POST /ui/action`     | status page buttons    | pause/resume/delete/retry (key required) |
| `GET /healthz`        | Docker/k8s/monitors    | liveness + queue/upstream health |
| `GET /metrics`        | Prometheus             | metrics in text exposition format |

---

## License

Do whatever you want with it.

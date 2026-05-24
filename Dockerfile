# --- build stage (runs on the host arch, cross-compiles for the target arch) ---
FROM --platform=$BUILDPLATFORM golang:1.23-alpine AS build
ARG TARGETOS
ARG TARGETARCH
WORKDIR /src
COPY sonrad.go .
RUN CGO_ENABLED=0 GOOS=$TARGETOS GOARCH=$TARGETARCH \
    go build -trimpath -ldflags="-s -w" -o /out/sonrad sonrad.go

# --- runtime stage ---
FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S sonrad && adduser -S -G sonrad -H -h /app sonrad && \
    mkdir -p /downloads && chown sonrad:sonrad /downloads

COPY --from=build /out/sonrad /usr/local/bin/sonrad

USER sonrad
WORKDIR /app

EXPOSE 8910
VOLUME ["/downloads"]

ENV SONRAD_ADDR=:8910 \
    SONRAD_DOWNLOAD_DIR=/downloads \
    SONRAD_API_KEY="" \
    SONRAD_MAX_CONCURRENT=3 \
    SONRAD_RATE_LIMIT=0 \
    SONRAD_USER_AGENT="Mozilla/5.0 (X11; Linux x86_64) sonrad/1.0" \
    SONRAD_COOKIES="" \
    SONRAD_BASE_URL="https://azfilm.theazizi.ir" \
    SONRAD_CACHE_TTL=10m \
    SONRAD_PUBLIC_HOST=""

ENTRYPOINT ["/bin/sh", "-c", "exec /usr/local/bin/sonrad \
    -addr \"$SONRAD_ADDR\" \
    -download-dir \"$SONRAD_DOWNLOAD_DIR\" \
    -api-key \"$SONRAD_API_KEY\" \
    -max-concurrent \"$SONRAD_MAX_CONCURRENT\" \
    -rate-limit \"$SONRAD_RATE_LIMIT\" \
    -user-agent \"$SONRAD_USER_AGENT\" \
    -cookies \"$SONRAD_COOKIES\" \
    -base-url \"$SONRAD_BASE_URL\" \
    -cache-ttl \"$SONRAD_CACHE_TTL\" \
    -public-host \"$SONRAD_PUBLIC_HOST\" \
    \"$@\"", "--"]

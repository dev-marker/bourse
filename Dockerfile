# syntax=docker/dockerfile:1
# One image with every program of the exchange in it. Compose picks the program each service runs.

# The build stage has Go and the source, and none of it ends up in the image.
FROM golang:1.27.1-alpine AS build
WORKDIR /src

# The modules first, in a layer of their own: it's only built again when go.mod or go.sum change.
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download

COPY . .
ARG VERSION=dev
# Static programs, without this machine's paths or debug symbols, and with the version stamped in.
# What production runs: the five services, and the tools to look after them. No bots, no load.
RUN --mount=type=cache,target=/go/pkg/mod --mount=type=cache,target=/root/.cache/go-build \
    CGO_ENABLED=0 go build -trimpath -ldflags "-s -w -X $(go list -m)/internal/obs.Version=${VERSION}" \
    -o /out/ ./cmd/engine ./cmd/publisher ./cmd/ledger ./cmd/gateway ./cmd/marketdata \
    ./cmd/healthcheck ./cmd/journal
# An empty folder for the engine's data and the backups, owned by the user the image runs as.
RUN mkdir -p /data

# The image: no shell and no package manager, just certificates, time zones, and a user that isn't root.
FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/ /usr/local/bin/
COPY --from=build --chown=nonroot:nonroot /data /var/lib/bourse
COPY --from=build --chown=nonroot:nonroot /data /backup
USER nonroot:nonroot

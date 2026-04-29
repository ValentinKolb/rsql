# syntax=docker/dockerfile:1

# rsql ships as a single static binary. modernc.org/sqlite is pure Go, so
# CGO_ENABLED=0 lets us produce a fully static build that runs in alpine
# (or scratch) without libc.

FROM golang:1.23-alpine AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal

RUN CGO_ENABLED=0 go build \
        -trimpath \
        -ldflags "-s -w" \
        -o /out/rsql \
        ./cmd/rsql

FROM alpine:3.20
RUN apk add --no-cache ca-certificates tzdata \
 && addgroup -S rsql \
 && adduser -S -G rsql -u 1000 rsql \
 && mkdir -p /data \
 && chown rsql:rsql /data

COPY --from=build /out/rsql /usr/local/bin/rsql

USER rsql
WORKDIR /data
VOLUME ["/data"]
EXPOSE 8080

ENV RSQL_LISTEN=0.0.0.0:8080 \
    RSQL_DATA_DIR=/data

ENTRYPOINT ["/usr/local/bin/rsql"]
CMD ["serve"]

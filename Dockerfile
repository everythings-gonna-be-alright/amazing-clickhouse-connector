# Builder runs natively on the build host and cross-compiles, so no QEMU is needed for other targets.
FROM --platform=$BUILDPLATFORM golang:1.27.3@sha256:921f61120011c774a7b37fd80bd3c3d259636090d3cd002b6a344322a0328082 AS builder

ARG TARGETOS
ARG TARGETARCH

WORKDIR /build
COPY go.mod go.sum ./
RUN go mod download && go mod verify
COPY *.go ./
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -trimpath -ldflags="-s -w" -o clickhouse-connector .

FROM scratch
# Root CA bundle so TLS certificate verification works (e.g. ClickHouse Cloud) in the scratch image.
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder /build/clickhouse-connector /sbin/clickhouse-connector

USER 65532:65532

ENTRYPOINT ["/sbin/clickhouse-connector"]

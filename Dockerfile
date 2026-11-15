# Builder runs natively on the build host and cross-compiles, so no QEMU is needed for other targets.
FROM --platform=$BUILDPLATFORM golang:1.27.4@sha256:5e8afd493a7124a8586e134d9b532731161d4b9becf9dee5a5956b107426f93d AS builder

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

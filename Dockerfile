FROM golang:1.22.3  as builder
ARG CGO_ENABLED=0

COPY ./ /build

RUN cd /build && go build

FROM scratch
COPY --from=builder /build/clickhouse-connector /sbin/clickhouse-connector

ENTRYPOINT ["/sbin/clickhouse-connector"]
# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/).

## [Unreleased]

### Added

- Release workflow that builds static binaries for Linux, macOS and Windows (`amd64`, `arm64`) on a version tag
  and attaches them to a GitHub release with checksums and a build provenance attestation.
- Release workflow publishes a multi-architecture image with provenance and SBOM to Docker Hub
  (`hiload/amazing-clickhouse-connector`).

### Changed

- The Helm chart image repository defaults to `hiload/amazing-clickhouse-connector`.

### Security

- Updated `github.com/andybalholm/brotli` to v1.2.7 (GO-2026-6888, excessive memory allocation when
  decompressing crafted streams) and the vulnerable dependencies of the end-to-end test module.

## [0.1.1] - 2026-11-08

### Added

- Required `x-auth-token` authentication for the ingest endpoint. `AUTH_KEY` accepts a comma-separated list
  (minimum 16 characters each) and is compared in constant time. `config.authKeyPrevious` keeps the old key
  valid during a rotation.
- Request limits: `-maxBodyBytes` (default 1 MiB) and `-maxColumns` (default 1000).
- TLS for ClickHouse connections: `-clickhouseTLS` and `-clickhouseTLSSkipVerify`, TLS 1.2 minimum.
- Single ClickHouse instance mode, selected by an empty `-clickhouseClusterName`.
- `-clickhouseLegacyDistPrefix` to keep `dist_<table>` names after migrating a cluster to a single instance.
- `-columnTypesConfig` for manual column type overrides when event types conflict.
- Secrets read from `AUTH_KEY`, `CLICKHOUSE_PASSWORD` and `REDIS_PASSWORD` environment variables.
- Helm chart: Gateway API `HTTPRoute`, NetworkPolicies for the connector and the queue, and a release secret
  that generates the queue password once and reuses it on upgrades.
- Multi-architecture container image (`linux/amd64`, `linux/arm64`) built on a `scratch` base running as a
  non-root user.
- Unit tests for payload validation, SQL generation, column type conversion, HTTP handling and the sample
  fixtures (`go test ./...`).
- Helm chart tests covering rendering, hardening and toggles (`tests/helm_test.sh`).
- End-to-end tests that deploy the chart with ClickHouse into a kind cluster (`e2e/`).
- GitHub Actions workflow running unit, chart, vulnerability, image build and end-to-end checks, and Dependabot
  configuration.

### Changed

- The queue backend is now Valkey 9.1.3, deployed with the official `valkey-io` Helm chart, replacing the Bitnami
  Redis subchart. Authentication uses an ACL user, and destructive and scripting commands are denied.
- All third-party images are pinned by digest. No image uses the `latest` tag.
- The HTTP servers have read, write, idle and header size limits.
- The local `docker-compose.yml` runs a single ClickHouse instance and Valkey with pinned images, passes the
  sample type overrides and binds all ports to `127.0.0.1`.
- `values.yaml` contains neutral defaults. The `HTTPRoute`, ServiceMonitors and CPU limits are opt-in or
  conservative, and credentials are always passed at deploy time.
- Go toolchain, ClickHouse driver and other dependencies updated.
- README documents every flag, the environment variables and the configuration modes.

### Removed

- The nginx `Ingress` template, replaced by `HTTPRoute`.
- The Bitnami Redis subchart and the `bitnamilegacy` image overrides.
- The two-node ClickHouse cluster configuration (`docker/`) and ZooKeeper from the local environment.
- Environment-specific values files and the internal CI definition from the published tree.

### Fixed

- The default `httpRoute` no longer exposes `/api/v1/events/bulk`, which the server does not implement and which
  answered `200` without queueing the events.
- `-columnTypesConfig` conversions no longer panic on `float32`, `int` and `int32` values.
- README listed the monitoring flag as `-monitoring-listen-address` (the flag is `-prometheus-listen-address`)
  and a wrong default for `-clickhouseBatchSize`.
- Startup banner showed the name of an internal product.

### Security

- Event types and field names must match `^[A-Za-z_][A-Za-z0-9_]*$` (200 characters at most) and are quoted in
  every generated statement. Invalid events are dropped instead of reaching ClickHouse.
- Connector-owned columns (`api_key`, `_timestamp`, `eventn_ctx_event_id`) cannot be set by clients.
- Containers drop all capabilities, run as non-root with a read-only root filesystem and no service account
  token, and use the `RuntimeDefault` seccomp profile.
- Sample data in `tests/` contains only placeholder values.

## [0.1.0] - 2024-07-18

### Added

- Initial release: HTTP ingest server, Redis-backed queue, ClickHouse consumer with automatic table creation
  and column alteration, Prometheus metrics, Helm chart, Grafana dashboard and a local docker-compose
  environment.

[Unreleased]: https://github.com/everythings-gonna-be-alright/amazing-clickhouse-connector/compare/v0.1.1...HEAD
[0.1.1]: https://github.com/everythings-gonna-be-alright/amazing-clickhouse-connector/compare/737bd03...v0.1.1

# Planx SQL Server Connector

A Planx 4.0 connector that reads from and writes to Microsoft SQL Server. One
self-describing binary exposing two components (ADR-009):
- **Source** (`source`): executes a finite SELECT query via `database/sql` +
  `go-mssqldb` and streams rows in batches, EOF at end-of-result. Type-tagged
  via the `DBBatch` envelope for full fidelity across
  int64/float64/string/bool/time/[]byte/NULL.
- **Sink** (`sink`): bulk-inserts incoming batches via prepared INSERT inside a
  transaction, batched per `batchRows` (v1 design — no CopyFrom equivalent in
  mssql).

## Build

```bash
go build -o plugin ./cmd/plugin
```

## Run (via Planx Engine)

Place the `plugin` binary at `<pluginsRoot>/sqlserver/plugin` and start the
engine; it Discovers the connector automatically (ADR-008, no manifest).

## Config

### Source component
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `host` | STRING | yes | — | SQL Server host |
| `port` | INTEGER | no | 1433 | SQL Server port |
| `database` | STRING | yes | — | Database name |
| `user` | STRING | yes | — | DB user |
| `password` | SECRET | yes | — | DB password (masked, never logged) |
| `query` | STRING | yes | — | SELECT query (finite result set) |
| `batchRows` | INTEGER | no | 1000 | Rows per batch |
| `encrypt` | ENUM | no | `disable` | Encrypt connection (TLS) — one of `disable` (no TLS), `false` (TLS without cert verify), `true` (TLS with cert verify). Default `disable` for dev. |

### Sink component
| Field | Type | Required | Default | Description |
|-------|------|----------|---------|-------------|
| `host` | STRING | yes | — | SQL Server host |
| `port` | INTEGER | no | 1433 | SQL Server port |
| `database` | STRING | yes | — | Database name |
| `user` | STRING | yes | — | DB user |
| `password` | SECRET | yes | — | DB password (masked, never logged) |
| `table` | STRING | yes | — | Target table (e.g. `dbo.users`) |
| `columns` | STRING | no | — | Comma-separated column list; if empty, uses batch column schema |
| `batchRows` | INTEGER | no | 1000 | Rows per INSERT transaction |
| `encrypt` | ENUM | no | `disable` | Encrypt connection (TLS) — one of `disable` (no TLS), `false` (TLS without cert verify), `true` (TLS with cert verify). Default `disable` for dev. |

The Sink is append-only in v1 (prepared INSERT path; no upsert/MERGE).

## Performance

| Pipeline | Rows | Elapsed | Throughput |
|----------|------|---------|------------|
| MSSQL→MSSQL (INSERT tx) | 10,000 | 2.71s | 3,690 rows/s |

Measured on Docker SQL Server 2025-latest, Apple Silicon (darwin/arm64), local loopback (127.0.0.1), `batchRows=1000`. Uses prepared INSERT per row inside a transaction. NULL preservation verified.

## Cross-Connector

This connector's `DBBatch` payload is gob-compatible with `planx-plugin-postgres` via the shared `gob.RegisterName("planx.io/dbbatch.DBBatch", ...)` wire name. A `sqlserver-source → postgres-sink` pipeline is also possible via the same `DBBatch` envelope; the reverse direction (`postgres-source → sqlserver-sink`) is validated at 3,676 rows/s (10,000 rows, byte-equal to source).

## Specification Authority
The authoritative spec lives in
[planx-spec](https://github.com/planx-lab/planx-spec) — see
[`db-connectors-design.md`](https://github.com/planx-lab/planx-spec/blob/main/db-connectors-design.md).

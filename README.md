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
| `encrypt` | ENUM | no | `false` | Encrypt connection (TLS) — one of `true`, `false` |

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
| `encrypt` | ENUM | no | `false` | Encrypt connection (TLS) — one of `true`, `false` |

The Sink is append-only in v1 (prepared INSERT path; no upsert/MERGE).

## Specification Authority
The authoritative spec lives in
[planx-spec](https://github.com/planx-lab/planx-spec) — see
[`db-connectors-design.md`](https://github.com/planx-lab/planx-spec/blob/main/db-connectors-design.md).
